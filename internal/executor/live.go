package executor

import (
	"cmp"
	"context"
	"slices"
)

// live owns the accepted execution position and its scheduling counts.
// Queue storage and speculative candidates have separate owners.
type live[S any] struct {
	header             Checkpoint[S]
	failureOverride    *FailureScope
	workspace          candidateWorkspace[S]
	inv                map[string]Invocation[S]
	single             Invocation[S]
	hasSingle          bool
	groups             map[string]ActivationGroup
	remaining          map[string]int
	joins              map[string]JoinSpec[S]
	joinEnqueued       map[string]bool
	ready              readyQueue
	readyNodes         int
	waitingInvocations int
}

func newLive[S any](cp Checkpoint[S], joins map[string]JoinSpec[S]) *live[S] {
	l := &live[S]{header: cp, joins: joins}
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
		for _, branch := range g.Branches {
			id := branch.InvocationID
			if inv, _ := l.invocation(id); !resolved(inv.Status) {
				l.remaining[g.ID]++
			}
		}
	}
	l.ready.items = make([]taskRef, 0, len(cp.Invocations))
	for _, inv := range cp.Invocations {
		if inv.Status == InvocationReady {
			l.ready.push(taskRef{id: inv.ID})
			l.readyNodes++
		} else if inv.Status == InvocationWaiting {
			l.waitingInvocations++
		}
	}
	l.ready.orderAfter(cp.ScheduleCursor)
	for _, g := range cp.Groups {
		l.enqueueJoinIfReady(g.ID)
	}
	return l
}

func resolved(status InvocationStatus) bool {
	return status == InvocationJoined || status == InvocationEnded || status == InvocationFailed
}

func (l *live[S]) enqueue(ref taskRef) { l.ready.push(ref) }

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

func (l *live[S]) hasReadyNode() bool { return l.readyNodes > 0 }

func (l *live[S]) pending() []PendingInvocation {
	pending := make([]PendingInvocation, 0, l.waitingInvocations)
	if l.inv != nil {
		for _, inv := range l.inv {
			if inv.Status == InvocationWaiting {
				pending = append(pending, PendingInvocation{InvocationID: inv.ID, CallID: inv.CallID, Node: inv.Node, Continuation: inv.Continuation})
			}
		}
	} else if l.hasSingle && l.single.Status == InvocationWaiting {
		inv := l.single
		pending = append(pending, PendingInvocation{InvocationID: inv.ID, CallID: inv.CallID, Node: inv.Node, Continuation: inv.Continuation})
	}
	slices.SortFunc(pending, func(a, b PendingInvocation) int { return cmp.Compare(a.InvocationID, b.InvocationID) })
	return pending
}

func (l *live[S]) joinEligible(id string) bool {
	g, ok := l.groups[id]
	if !ok {
		return false
	}
	if l.remaining[id] == 0 {
		return true
	}
	if g.JoinNode == "" || l.joins == nil {
		return false
	}
	spec, ok := l.joins[g.JoinNode]
	if !ok {
		return false
	}
	if spec.CanMerge != nil {
		// CanMerge is evaluated by the scheduler after it has made isolated copies.
		return true
	}
	if spec.Quorum > 0 {
		var joined int
		for _, branch := range g.Branches {
			childID := branch.InvocationID
			if child, exists := l.invocation(childID); exists && child.Status == InvocationJoined {
				joined++
			}
		}
		if joined >= spec.Quorum {
			return true
		}
	}
	return false
}

func (l *live[S]) valid(ref taskRef) bool {
	if ref.join {
		return l.joinEligible(ref.id)
	}
	v, ok := l.invocation(ref.id)
	return ok && v.Status == InvocationReady
}

func (l *live[S]) enqueueJoinIfReady(id string) {
	if id == "" {
		return
	}
	if l.joinEnqueued[id] {
		return
	}
	if l.joinEligible(id) {
		if l.joinEnqueued == nil {
			l.joinEnqueued = make(map[string]bool)
		}
		l.joinEnqueued[id] = true
		l.enqueue(taskRef{id: id, join: true})
	}
}

func checkpointPosition[S any](c Change[S]) Checkpoint[S] {
	h := c.Header
	return Checkpoint[S]{FormatVersion: h.FormatVersion, RunID: h.RunID, MachineID: h.MachineID, Revision: h.Revision, Steps: h.Steps, NextID: h.NextID, ScheduleCursor: h.ScheduleCursor, Completed: h.Completed, Final: c.Final, HadLocalFailures: h.HadLocalFailures, Failure: h.Failure}
}

func (l *live[S]) next(nodes bool, running map[taskRef]context.CancelFunc) (taskRef, bool) {
	n := l.ready.len()
	for range n {
		ref := l.ready.pop()
		if !l.valid(ref) {
			if ref.join {
				delete(l.joinEnqueued, ref.id)
			}
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
		clear(l.joinEnqueued)
		l.ready.clear()
		l.readyNodes = 0
		l.waitingInvocations = 0
		return
	}
	if len(c.UpsertGroups)+len(c.DeleteGroups) > 0 && l.groups == nil {
		l.groups = make(map[string]ActivationGroup, len(c.UpsertGroups))
		l.remaining = make(map[string]int, len(c.UpsertGroups))
	}
	for _, id := range c.DeleteInvocations {
		old, _ := l.invocation(id)
		if old.Status == InvocationReady {
			l.readyNodes--
		} else if old.Status == InvocationWaiting {
			l.waitingInvocations--
		}
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
			if old.Status == InvocationReady {
				l.readyNodes--
			} else if old.Status == InvocationWaiting {
				l.waitingInvocations--
			}
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
			l.readyNodes++
		} else if v.Status == InvocationWaiting {
			l.waitingInvocations++
		}
		l.enqueueJoinIfReady(oldGroup)
		if v.GroupID != oldGroup {
			l.enqueueJoinIfReady(v.GroupID)
		}
	}
	for _, id := range c.DeleteGroups {
		delete(l.groups, id)
		delete(l.remaining, id)
		delete(l.joinEnqueued, id)
	}
	for _, g := range c.UpsertGroups {
		l.groups[g.ID] = g
		l.remaining[g.ID] = 0
		delete(l.joinEnqueued, g.ID)
		for _, branch := range g.Branches {
			id := branch.InvocationID
			if inv, _ := l.invocation(id); !resolved(inv.Status) {
				l.remaining[g.ID]++
			}
		}
		l.enqueueJoinIfReady(g.ID)
	}
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
