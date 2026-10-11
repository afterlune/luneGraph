package executor

import (
	"cmp"
	"context"
	"slices"
)

type taskRef struct {
	id   string
	join bool
}

// live owns one invocation's execution position. Candidates contain only the
// records a transition can touch; full snapshots are materialized at boundaries.
type live[S any] struct {
	header                    Checkpoint[S]
	failureOverride           *FailureScope
	beforeInv, afterInv       []Invocation[S]
	beforeGroups, afterGroups []ActivationGroup
	membership                map[string]bool
	inv                       map[string]Invocation[S]
	single                    Invocation[S]
	hasSingle                 bool
	groups                    map[string]ActivationGroup
	remaining                 map[string]int
	ready                     []taskRef
	readyHead                 int
}

func newLive[S any](cp Checkpoint[S]) *live[S] {
	l := &live[S]{header: cp}
	if len(cp.Invocations) == 1 {
		l.single = cp.Invocations[0]
		l.hasSingle = true
	} else if len(cp.Invocations) > 1 {
		l.inv = make(map[string]Invocation[S], len(cp.Invocations))
		for _, v := range cp.Invocations {
			l.inv[v.ID] = v
		}
	}
	if len(cp.Groups) > 0 {
		l.groups = make(map[string]ActivationGroup, len(cp.Groups))
		l.remaining = make(map[string]int, len(cp.Groups))
	}
	l.header.Invocations = nil
	l.header.Groups = nil
	for _, g := range cp.Groups {
		l.groups[g.ID] = g
		for _, id := range g.Children {
			if inv, _ := l.invocation(id); !resolved(inv.Status) {
				l.remaining[g.ID]++
			}
		}
	}
	l.ready = make([]taskRef, 0, len(cp.Invocations))
	for _, inv := range cp.Invocations {
		if inv.Status == InvocationReady {
			l.ready = append(l.ready, taskRef{id: inv.ID})
		}
	}
	slices.SortFunc(l.ready, func(a, b taskRef) int { return cmp.Compare(invocationNumber(a.id), invocationNumber(b.id)) })
	at := 0
	for at < len(l.ready) && invocationNumber(l.ready[at].id) <= cp.ScheduleCursor {
		at++
	}
	// Rotate in place so resumed ready work starts after the persisted cursor.
	reverseTasks(l.ready[:at])
	reverseTasks(l.ready[at:])
	reverseTasks(l.ready)
	for _, g := range cp.Groups {
		if l.remaining[g.ID] == 0 {
			l.enqueue(taskRef{id: g.ID, join: true})
		}
	}
	return l
}

func reverseTasks(tasks []taskRef) {
	for i, j := 0, len(tasks)-1; i < j; i, j = i+1, j-1 {
		tasks[i], tasks[j] = tasks[j], tasks[i]
	}
}

func resolved(status InvocationStatus) bool {
	return status == InvocationJoined || status == InvocationEnded || status == InvocationFailed
}
func (l *live[S]) enqueue(ref taskRef) {
	if l.readyHead == len(l.ready) {
		l.ready = l.ready[:0]
		l.readyHead = 0
	} else if l.readyHead >= 128 && l.readyHead >= len(l.ready)/2 {
		n := copy(l.ready, l.ready[l.readyHead:])
		clear(l.ready[n:])
		l.ready = l.ready[:n]
		l.readyHead = 0
	}
	l.ready = append(l.ready, ref)
}

func (l *live[S]) snapshot() Checkpoint[S] {
	cp := l.header
	if l.inv != nil {
		for _, v := range l.inv {
			cp.Invocations = append(cp.Invocations, v)
		}
	} else if l.hasSingle {
		cp.Invocations = append(cp.Invocations, l.single)
	}
	for _, g := range l.groups {
		cp.Groups = append(cp.Groups, g)
	}
	slices.SortFunc(cp.Invocations, func(a, b Invocation[S]) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(cp.Groups, func(a, b ActivationGroup) int { return cmp.Compare(a.ID, b.ID) })
	return cp
}

func (l *live[S]) invocation(id string) (Invocation[S], bool) {
	if l.inv != nil {
		v, ok := l.inv[id]
		return v, ok
	}
	return l.single, l.hasSingle && l.single.ID == id
}
func (l *live[S]) putInvocation(v Invocation[S]) {
	if l.inv != nil {
		l.inv[v.ID] = v
		return
	}
	if !l.hasSingle {
		l.single = v
		l.hasSingle = true
		return
	}
	if l.single.ID == v.ID {
		l.single = v
		return
	}
	l.inv = make(map[string]Invocation[S], 2)
	l.inv[l.single.ID] = l.single
	l.inv[v.ID] = v
	l.single = Invocation[S]{}
	l.hasSingle = false
}
func (l *live[S]) deleteInvocation(id string) {
	if l.inv != nil {
		delete(l.inv, id)
		return
	}
	if l.hasSingle && l.single.ID == id {
		l.single = Invocation[S]{}
		l.hasSingle = false
	}
}
func (l *live[S]) invocationCount() int {
	if l.inv != nil {
		return len(l.inv)
	}
	if l.hasSingle {
		return 1
	}
	return 0
}

func (l *live[S]) valid(ref taskRef) bool {
	if ref.join {
		_, ok := l.groups[ref.id]
		return ok && l.remaining[ref.id] == 0
	}
	v, ok := l.invocation(ref.id)
	return ok && v.Status == InvocationReady
}

func (l *live[S]) enqueueJoinIfReady(id string) {
	if id == "" {
		return
	}
	if _, ok := l.groups[id]; ok && l.remaining[id] == 0 {
		l.enqueue(taskRef{id: id, join: true})
	}
}

func checkpointPosition[S any](c Change[S]) Checkpoint[S] {
	h := c.Header
	return Checkpoint[S]{FormatVersion: h.FormatVersion, RunID: h.RunID, MachineID: h.MachineID, Revision: h.Revision, Steps: h.Steps, NextID: h.NextID, ScheduleCursor: h.ScheduleCursor, Completed: h.Completed, Final: c.Final, HadLocalFailures: h.HadLocalFailures, Failure: h.Failure}
}

func (l *live[S]) next(nodes bool, running map[taskRef]context.CancelFunc) (taskRef, bool) {
	n := len(l.ready) - l.readyHead
	for range n {
		ref := l.ready[l.readyHead]
		l.ready[l.readyHead] = taskRef{}
		l.readyHead++
		if !l.valid(ref) {
			continue
		}
		if _, busy := running[ref]; busy {
			continue
		}
		if !nodes && !ref.join {
			l.enqueue(ref)
			continue
		}
		return ref, true
	}
	return taskRef{}, false
}

func (l *live[S]) install(c Change[S]) {
	l.header = checkpointPosition(c)
	if c.Header.Completed {
		clear(l.inv)
		l.inv = nil
		l.single = Invocation[S]{}
		l.hasSingle = false
		clear(l.groups)
		clear(l.remaining)
		clear(l.ready)
		l.ready = nil
		l.readyHead = 0
		return
	}
	if len(c.UpsertGroups)+len(c.DeleteGroups) > 0 && l.groups == nil {
		l.groups = make(map[string]ActivationGroup, len(c.UpsertGroups))
		l.remaining = make(map[string]int, len(c.UpsertGroups))
	}
	for _, id := range c.DeleteInvocations {
		old, _ := l.invocation(id)
		if old.GroupID != "" && !resolved(old.Status) {
			l.remaining[old.GroupID]--
		}
		l.deleteInvocation(id)
		l.enqueueJoinIfReady(old.GroupID)
	}
	for _, v := range c.UpsertInvocations {
		old, exists := l.invocation(v.ID)
		oldGroup := ""
		if exists {
			oldGroup = old.GroupID
		}
		if exists && old.GroupID != "" && !resolved(old.Status) {
			l.remaining[old.GroupID]--
		}
		l.putInvocation(v)
		if v.GroupID != "" && !resolved(v.Status) {
			l.remaining[v.GroupID]++
		}
		if v.Status == InvocationReady {
			l.enqueue(taskRef{id: v.ID})
		}
		l.enqueueJoinIfReady(oldGroup)
		if v.GroupID != oldGroup {
			l.enqueueJoinIfReady(v.GroupID)
		}
	}
	for _, id := range c.DeleteGroups {
		delete(l.groups, id)
		delete(l.remaining, id)
	}
	for _, g := range c.UpsertGroups {
		l.groups[g.ID] = g
		l.remaining[g.ID] = 0
		for _, id := range g.Children {
			if inv, _ := l.invocation(id); !resolved(inv.Status) {
				l.remaining[g.ID]++
			}
		}
		l.enqueueJoinIfReady(g.ID)
	}
}

// candidate collects a transition's local topology, never unrelated states.
type candidateMembers struct {
	invocations map[string]bool
	groups      map[string]bool
}

func (l *live[S]) addCandidateInvocation(cp *Checkpoint[S], id string, m *candidateMembers) {
	if m.invocations != nil {
		if m.invocations[id] {
			return
		}
	} else {
		for _, v := range cp.Invocations {
			if v.ID == id {
				return
			}
		}
		if len(cp.Invocations) >= invocationIndexThreshold {
			m.invocations = make(map[string]bool, len(cp.Invocations)+1)
			for _, v := range cp.Invocations {
				m.invocations[v.ID] = true
			}
		}
	}
	v, ok := l.invocation(id)
	if !ok {
		return
	}
	if m.invocations != nil {
		m.invocations[id] = true
	}
	cp.Invocations = append(cp.Invocations, v)
}

func (l *live[S]) addCandidateGroup(cp *Checkpoint[S], id string, children bool, m *candidateMembers) {
	if m.groups != nil {
		if m.groups[id] {
			return
		}
	} else {
		for _, g := range cp.Groups {
			if g.ID == id {
				return
			}
		}
		if len(cp.Groups) >= invocationIndexThreshold {
			m.groups = make(map[string]bool, len(cp.Groups)+1)
			for _, g := range cp.Groups {
				m.groups[g.ID] = true
			}
		}
	}
	g, ok := l.groups[id]
	if !ok {
		return
	}
	if m.groups != nil {
		m.groups[id] = true
	}
	cp.Groups = append(cp.Groups, g)
	if children {
		l.addCandidateInvocation(cp, g.ParentID, m)
		for _, child := range g.Children {
			l.addCandidateInvocation(cp, child, m)
			descendant, exists := l.invocation(child)
			if nested := descendant.ChildGroupID; exists && nested != "" {
				l.addCandidateGroup(cp, nested, true, m)
			}
		}
	}
}

func (l *live[S]) candidate(id string, subtree bool) Checkpoint[S] {
	cp := l.header
	clear(l.beforeInv)
	clear(l.beforeGroups)
	cp.Invocations = l.beforeInv[:0]
	cp.Groups = l.beforeGroups[:0]
	var members candidateMembers
	l.addCandidateInvocation(&cp, id, &members)
	if v, ok := l.invocation(id); ok && v.GroupID != "" {
		l.addCandidateGroup(&cp, v.GroupID, subtree, &members)
	}
	l.beforeInv = cp.Invocations
	l.beforeGroups = cp.Groups
	return cp
}

func (l *live[S]) localChange(before, after Checkpoint[S]) Change[S] {
	c := Change[S]{Header: after.Header(), Final: after.Final, UpsertInvocations: after.Invocations}
	if len(before.Invocations)+len(after.Invocations) < 16 {
		for _, v := range before.Invocations {
			found := false
			for _, next := range after.Invocations {
				if v.ID == next.ID {
					found = true
					break
				}
			}
			if !found {
				c.DeleteInvocations = append(c.DeleteInvocations, v.ID)
			}
		}
	} else {
		if l.membership == nil {
			l.membership = make(map[string]bool, len(before.Invocations))
		}
		clear(l.membership)
		for _, v := range before.Invocations {
			l.membership[v.ID] = true
		}
		for _, v := range after.Invocations {
			delete(l.membership, v.ID)
		}
		for id := range l.membership {
			c.DeleteInvocations = append(c.DeleteInvocations, id)
		}
	}
	if len(before.Groups)+len(after.Groups) < 16 {
		for _, old := range before.Groups {
			found := false
			for _, next := range after.Groups {
				if old.ID == next.ID {
					found = true
					break
				}
			}
			if !found {
				c.DeleteGroups = append(c.DeleteGroups, old.ID)
			}
		}
		for _, next := range after.Groups {
			found := false
			for _, old := range before.Groups {
				if next.ID == old.ID {
					found = true
					break
				}
			}
			if !found {
				c.UpsertGroups = append(c.UpsertGroups, next)
			}
		}
	} else {
		if l.membership == nil {
			l.membership = make(map[string]bool, len(before.Groups))
		}
		clear(l.membership)
		for _, g := range before.Groups {
			l.membership[g.ID] = true
		}
		for _, g := range after.Groups {
			if l.membership[g.ID] {
				delete(l.membership, g.ID)
			} else {
				c.UpsertGroups = append(c.UpsertGroups, g)
			}
		}
		for id := range l.membership {
			c.DeleteGroups = append(c.DeleteGroups, id)
		}
	}
	clear(l.membership)
	return c
}

func (l *live[S]) complete(c *Change[S]) {
	if c.Header.Completed {
		return
	}
	active := l.invocationCount() + len(c.UpsertInvocations) - len(c.DeleteInvocations)
	for _, v := range c.UpsertInvocations {
		if _, ok := l.invocation(v.ID); ok {
			active--
		}
	}
	groups := len(l.groups) + len(c.UpsertGroups) - len(c.DeleteGroups)
	if groups != 0 {
		return
	}
	// Root ended records have no future use. There cannot be parallel root paths
	// without an activation group, so this examines only the affected records.
	if active == 0 {
		c.Header.Completed = true
		return
	}
	if groups == 0 && len(c.UpsertInvocations) == 1 && resolved(c.UpsertInvocations[0].Status) {
		c.Header.Completed = true
		c.UpsertInvocations = nil
		c.DeleteInvocations = nil
	}
}
