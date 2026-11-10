package executor

import (
	"cmp"
	"context"
	"slices"
)

type liveGroup[S any] struct {
	group        ActivationGroup
	remaining    int
	joinEnqueued bool
	join         *CompiledJoin[S]
}

// live owns the accepted execution position and its scheduling counts.
type live[S any] struct {
	header          Checkpoint[S]
	finalVal        S
	failureOverride *FailureScope
	workspace       candidateWorkspace[S]

	// Small-Buffer Optimization (SBO)
	invInline   [4]Invocation[S]
	groupInline [2]liveGroup[S]

	invocations []Invocation[S]
	invIndex    map[string]int

	groups     []liveGroup[S]
	groupIndex map[string]int

	runner *Runner[S]

	nodeQueue readyQueue
	joinQueue readyQueue

	readyNodes         int
	waitingInvocations int
}

func newLive[S any](cp Checkpoint[S], runner *Runner[S]) *live[S] {
	return initLive(&live[S]{}, cp, runner)
}

func initLiveStart[S any](l *live[S], runID string, runner *Runner[S], state S) *live[S] {
	l.header = Checkpoint[S]{
		FormatVersion: CheckpointFormatVersion,
		RunID:         runID,
		MachineID:     runner.id,
		Revision:      1,
		NextID:        3,
	}
	l.runner = runner
	l.invocations = l.invInline[:1]
	l.invocations[0] = Invocation[S]{
		ID:     "i1",
		CallID: "c2",
		Node:   runner.entry,
		State:  state,
		Status: InvocationReady,
	}
	l.groups = l.groupInline[:0]
	nodeIdx := 0
	if runner.entryIndex >= 0 {
		nodeIdx = runner.entryIndex
	}
	l.nodeQueue.push(taskRef{id: "i1", slot: 0, nodeIdx: nodeIdx, join: false})
	l.readyNodes = 1
	return l
}

func initLive[S any](l *live[S], cp Checkpoint[S], runner *Runner[S]) *live[S] {
	l.header = cp
	l.runner = runner
	if len(cp.Invocations) <= len(l.invInline) {
		l.invocations = l.invInline[:0]
	} else {
		l.invocations = make([]Invocation[S], 0, len(cp.Invocations))
	}
	for _, v := range cp.Invocations {
		l.putInvocation(v)
	}

	if len(cp.Groups) <= len(l.groupInline) {
		l.groups = l.groupInline[:0]
	} else {
		l.groups = make([]liveGroup[S], 0, len(cp.Groups))
	}

	for _, g := range cp.Groups {
		rem := 0
		for _, b := range g.Branches {
			if inv, _, ok := l.invocation(b.InvocationID, -1); ok && !inv.Status.Resolved() {
				rem++
			}
		}
		l.putGroup(liveGroup[S]{group: g, remaining: rem})
	}

	l.header.Invocations = nil
	l.header.Groups = nil

	for i, inv := range l.invocations {
		if inv.Status == InvocationReady {
			nodeIdx := 0
			if runner != nil && inv.Node == runner.entry && runner.entryIndex >= 0 {
				nodeIdx = runner.entryIndex
			}
			l.nodeQueue.push(taskRef{id: inv.ID, slot: i, nodeIdx: nodeIdx, join: false})
			l.readyNodes++
		} else if inv.Status == InvocationWaiting {
			l.waitingInvocations++
		}
	}
	l.nodeQueue.orderAfter(cp.ScheduleCursor)

	for i := range l.groups {
		l.enqueueJoinIfReady(l.groups[i].group.ID)
	}
	return l
}

func resolved(status InvocationStatus) bool {
	return status.Resolved()
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (l *live[S]) enqueue(ref taskRef) {
	if ref.join {
		l.joinQueue.push(ref)
	} else {
		l.nodeQueue.push(ref)
	}
}

func (l *live[S]) snapshot() Checkpoint[S] {
	cp := l.header
	if len(l.invocations) > 0 {
		cp.Invocations = make([]Invocation[S], len(l.invocations))
		copy(cp.Invocations, l.invocations)
		slices.SortFunc(cp.Invocations, func(a, b Invocation[S]) int { return cmp.Compare(a.ID, b.ID) })
	}
	if len(l.groups) > 0 {
		cp.Groups = make([]ActivationGroup, len(l.groups))
		for i := range l.groups {
			cp.Groups[i] = l.groups[i].group
		}
		slices.SortFunc(cp.Groups, func(a, b ActivationGroup) int { return cmp.Compare(a.ID, b.ID) })
	}
	return cp
}

func (l *live[S]) invocation(id string, slotHint int) (*Invocation[S], int, bool) {
	if uint(slotHint) < uint(len(l.invocations)) && l.invocations[slotHint].ID == id {
		return &l.invocations[slotHint], slotHint, true
	}
	if l.invIndex != nil {
		if idx, ok := l.invIndex[id]; ok {
			return &l.invocations[idx], idx, true
		}
		return nil, -1, false
	}
	for i := range l.invocations {
		if l.invocations[i].ID == id {
			return &l.invocations[i], i, true
		}
	}
	return nil, -1, false
}

func (l *live[S]) getInvocation(id string) (Invocation[S], bool) {
	inv, _, ok := l.invocation(id, -1)
	if ok {
		return *inv, true
	}
	return Invocation[S]{}, false
}

func (l *live[S]) putInvocation(v Invocation[S]) int {
	if inv, idx, ok := l.invocation(v.ID, -1); ok {
		*inv = v
		return idx
	}
	return l.putNewInvocation(v)
}

func (l *live[S]) putNewInvocation(v Invocation[S]) int {
	idx := len(l.invocations)
	l.invocations = append(l.invocations, v)
	if len(l.invocations) > 8 {
		if l.invIndex == nil {
			l.invIndex = make(map[string]int, len(l.invocations))
			for i := range l.invocations {
				l.invIndex[l.invocations[i].ID] = i
			}
		} else {
			l.invIndex[v.ID] = idx
		}
	}
	return idx
}

func (l *live[S]) deleteInvocation(id string) {
	_, idx, ok := l.invocation(id, -1)
	if !ok {
		return
	}
	last := len(l.invocations) - 1
	if idx != last {
		l.invocations[idx] = l.invocations[last]
		if l.invIndex != nil {
			l.invIndex[l.invocations[idx].ID] = idx
		}
	}
	l.invocations[last] = Invocation[S]{}
	l.invocations = l.invocations[:last]
	if l.invIndex != nil {
		delete(l.invIndex, id)
		if len(l.invocations) <= 8 {
			l.invIndex = nil
		}
	}
}

func (l *live[S]) invocationCount() int {
	return len(l.invocations)
}

func (l *live[S]) hasReadyNode() bool {
	return l.readyNodes > 0
}

func (l *live[S]) pending() []PendingInvocation {
	pending := make([]PendingInvocation, 0, l.waitingInvocations)
	for _, inv := range l.invocations {
		if inv.Status == InvocationWaiting {
			pending = append(pending, PendingInvocation{
				InvocationID: inv.ID,
				CallID:       inv.CallID,
				Node:         inv.Node,
				Continuation: inv.Continuation,
			})
		}
	}
	slices.SortFunc(pending, func(a, b PendingInvocation) int { return cmp.Compare(a.InvocationID, b.InvocationID) })
	return pending
}

func (l *live[S]) group(id string, slotHint int) (*liveGroup[S], int, bool) {
	if uint(slotHint) < uint(len(l.groups)) && l.groups[slotHint].group.ID == id {
		return &l.groups[slotHint], slotHint, true
	}
	if l.groupIndex != nil {
		if idx, ok := l.groupIndex[id]; ok {
			return &l.groups[idx], idx, true
		}
		return nil, -1, false
	}
	for i := range l.groups {
		if l.groups[i].group.ID == id {
			return &l.groups[i], i, true
		}
	}
	return nil, -1, false
}

func (l *live[S]) getGroup(id string) (ActivationGroup, bool) {
	lg, _, ok := l.group(id, -1)
	if ok {
		return lg.group, true
	}
	return ActivationGroup{}, false
}

func (l *live[S]) putGroup(g liveGroup[S]) int {
	if g.join == nil && g.group.JoinNode != "" && l.runner != nil {
		g.join = l.runner.join(g.group.JoinNode)
	}
	if existing, idx, ok := l.group(g.group.ID, -1); ok {
		*existing = g
		return idx
	}
	idx := len(l.groups)
	l.groups = append(l.groups, g)
	if len(l.groups) > 8 {
		if l.groupIndex == nil {
			l.groupIndex = make(map[string]int, len(l.groups))
			for i := range l.groups {
				l.groupIndex[l.groups[i].group.ID] = i
			}
		} else {
			l.groupIndex[g.group.ID] = idx
		}
	}
	return idx
}

func (l *live[S]) deleteGroup(id string) {
	_, idx, ok := l.group(id, -1)
	if !ok {
		return
	}
	last := len(l.groups) - 1
	if idx != last {
		l.groups[idx] = l.groups[last]
		if l.groupIndex != nil {
			l.groupIndex[l.groups[idx].group.ID] = idx
		}
	}
	l.groups[last] = liveGroup[S]{}
	l.groups = l.groups[:last]
	if l.groupIndex != nil {
		delete(l.groupIndex, id)
		if len(l.groups) <= 8 {
			l.groupIndex = nil
		}
	}
}

func (l *live[S]) joinEligible(g *liveGroup[S]) bool {
	if g == nil {
		return false
	}
	if g.remaining == 0 {
		return true
	}
	join := g.join
	if join == nil {
		return false
	}
	if join.Spec.CanMerge != nil {
		return true
	}
	if join.Spec.Quorum > 0 {
		joined := 0
		for _, b := range g.group.Branches {
			if child, _, exists := l.invocation(b.InvocationID, -1); exists && child.Status == InvocationJoined {
				joined++
			}
		}
		if joined >= join.Spec.Quorum {
			return true
		}
	}
	return false
}

func (l *live[S]) valid(ref taskRef) bool {
	if ref.join {
		g, _, ok := l.group(ref.id, ref.slot)
		return ok && l.joinEligible(g)
	}
	v, _, ok := l.invocation(ref.id, ref.slot)
	return ok && v.Status == InvocationReady
}

func (l *live[S]) enqueueJoinIfReady(id string) {
	if id == "" {
		return
	}
	g, idx, ok := l.group(id, -1)
	if !ok || g.joinEnqueued {
		return
	}
	if l.joinEligible(g) {
		g.joinEnqueued = true
		l.joinQueue.push(taskRef{id: id, slot: idx, join: true})
	}
}

func checkpointPosition[S any](c Change[S]) Checkpoint[S] {
	h := c.Header
	return Checkpoint[S]{
		FormatVersion:    h.FormatVersion,
		RunID:            h.RunID,
		MachineID:        h.MachineID,
		Revision:         h.Revision,
		Steps:            h.Steps,
		NextID:           h.NextID,
		ScheduleCursor:   h.ScheduleCursor,
		Completed:        h.Completed,
		Final:            c.Final,
		HadLocalFailures: h.HadLocalFailures,
		Failure:          h.Failure,
	}
}

func isRunning(running map[taskRef]context.CancelFunc, ref taskRef) bool {
	if _, busy := running[ref]; busy {
		return true
	}
	for r := range running {
		if r.id == ref.id && r.join == ref.join {
			return true
		}
	}
	return false
}

func (l *live[S]) next(nodes bool, running map[taskRef]context.CancelFunc) (taskRef, bool) {
	// First drain eligible joins from joinQueue
	nJoins := l.joinQueue.len()
	for range nJoins {
		ref := l.joinQueue.pop()
		g, slot, ok := l.group(ref.id, ref.slot)
		if !ok || !l.joinEligible(g) {
			if ok {
				g.joinEnqueued = false
			}
			continue
		}
		ref.slot = slot
		if isRunning(running, ref) {
			l.joinQueue.push(ref)
			continue
		}
		return ref, true
	}

	if !nodes {
		return taskRef{}, false
	}

	// Then drain ready nodes from nodeQueue
	nNodes := l.nodeQueue.len()
	for range nNodes {
		ref := l.nodeQueue.pop()
		inv, slot, ok := l.invocation(ref.id, ref.slot)
		if !ok || inv.Status != InvocationReady {
			continue
		}
		ref.slot = slot
		if isRunning(running, ref) {
			l.nodeQueue.push(ref)
			continue
		}
		return ref, true
	}

	return taskRef{}, false
}

func (l *live[S]) clearActive() {
	clear(l.invocations)
	l.invocations = l.invInline[:0]
	l.invIndex = nil
	clear(l.groups)
	l.groups = l.groupInline[:0]
	l.groupIndex = nil
	l.nodeQueue.clear()
	l.joinQueue.clear()
	l.readyNodes = 0
	l.waitingInvocations = 0
}

func (l *live[S]) install(c Change[S]) {
	l.header = checkpointPosition(c)
	if c.Header.Completed {
		l.clearActive()
		return
	}

	for _, id := range c.DeleteInvocations {
		old, _, exists := l.invocation(id, -1)
		if exists {
			l.readyNodes -= b2i(old.Status == InvocationReady)
			l.waitingInvocations -= b2i(old.Status == InvocationWaiting)
			if old.GroupID != "" && !old.Status.Resolved() {
				if g, _, gExists := l.group(old.GroupID, -1); gExists {
					g.remaining--
				}
			}
			oldGroup := old.GroupID
			l.deleteInvocation(id)
			l.enqueueJoinIfReady(oldGroup)
		}
	}

	for _, v := range c.UpsertInvocations {
		old, _, exists := l.invocation(v.ID, -1)
		oldGroup := ""
		if exists {
			oldGroup = old.GroupID
			l.readyNodes -= b2i(old.Status == InvocationReady)
			l.waitingInvocations -= b2i(old.Status == InvocationWaiting)
			if old.GroupID != "" && !old.Status.Resolved() {
				if g, _, gExists := l.group(old.GroupID, -1); gExists {
					g.remaining--
				}
			}
		}
		slot := l.putInvocation(v)
		l.readyNodes += b2i(v.Status == InvocationReady)
		l.waitingInvocations += b2i(v.Status == InvocationWaiting)
		if v.GroupID != "" && !v.Status.Resolved() {
			if g, _, gExists := l.group(v.GroupID, -1); gExists {
				g.remaining++
			}
		}
		if v.Status == InvocationReady {
			l.nodeQueue.push(taskRef{id: v.ID, slot: slot, join: false})
		}
		l.enqueueJoinIfReady(oldGroup)
		if v.GroupID != oldGroup {
			l.enqueueJoinIfReady(v.GroupID)
		}
	}

	for _, id := range c.DeleteGroups {
		l.deleteGroup(id)
	}

	for _, g := range c.UpsertGroups {
		rem := 0
		for _, b := range g.Branches {
			if child, _, exists := l.invocation(b.InvocationID, -1); exists && !child.Status.Resolved() {
				rem++
			}
		}
		enqueued := false
		if existing, _, ok := l.group(g.ID, -1); ok {
			enqueued = existing.joinEnqueued
		}
		l.putGroup(liveGroup[S]{group: g, remaining: rem, joinEnqueued: enqueued})
		l.enqueueJoinIfReady(g.ID)
	}
}

func (l *live[S]) complete(c *Change[S]) {
	if c.Header.Completed {
		return
	}
	active := l.invocationCount() + len(c.UpsertInvocations) - len(c.DeleteInvocations)
	for _, v := range c.UpsertInvocations {
		if _, _, ok := l.invocation(v.ID, -1); ok {
			active--
		}
	}
	groups := len(l.groups) + len(c.UpsertGroups) - len(c.DeleteGroups)
	if groups != 0 {
		return
	}
	if active == 0 {
		c.Header.Completed = true
		return
	}
	if groups == 0 && len(c.UpsertInvocations) == 1 && c.UpsertInvocations[0].Status.Resolved() {
		c.Header.Completed = true
		c.UpsertInvocations = nil
		c.DeleteInvocations = nil
	}
}
