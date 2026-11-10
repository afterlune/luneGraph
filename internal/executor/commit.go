package executor

import (
	"context"
	"errors"
	"math"
	"slices"

	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

// commitResult builds and submits one scheduler outcome. Candidate errors never
// reach Store; terminal callback errors are returned only after a successful commit.
func (r *Runner[S]) commitResult(ctx context.Context, l *live[S], out workResult[S], maxInvocations int, cursor uint64, store Store[S], obs *observation.Session, prepared map[string]workerTask[S]) (error, error) {
	if ok, touchedGroupID, commitErr := r.tryCommitFast(ctx, l, out, maxInvocations, cursor, store, obs); ok {
		if commitErr != nil {
			return nil, commitErr
		}
		if touchedGroupID != "" && len(prepared) > 0 {
			delete(prepared, touchedGroupID)
		}
		return nil, nil
	}
	c, terminalErr, err := r.resultChange(l, out, maxInvocations)
	if err != nil {
		return nil, err
	}
	c.Header.ScheduleCursor = cursor
	if err := r.submit(ctx, l, c, store, obs); err != nil {
		return nil, err
	}
	if len(prepared) > 0 {
		for _, v := range c.UpsertInvocations {
			if v.GroupID != "" {
				delete(prepared, v.GroupID)
			}
		}
	}
	return terminalErr, nil
}

func cloneGroupWithTimeoutCleared[S any](lg *liveGroup[S], branchIndex int) (ActivationGroup, bool) {
	if lg != nil && branchIndex >= 0 && branchIndex < len(lg.group.Branches) && lg.group.Branches[branchIndex].EntryTimeout != 0 {
		g := lg.group
		g.Branches = slices.Clone(lg.group.Branches)
		g.Branches[branchIndex].EntryTimeout = 0
		return g, true
	}
	return ActivationGroup{}, false
}

func clearEntryTimeoutInLive[S any](lg *liveGroup[S], branchIndex int) {
	if lg != nil && branchIndex >= 0 && branchIndex < len(lg.group.Branches) {
		lg.group.Branches[branchIndex].EntryTimeout = 0
	}
}

func (r *Runner[S]) tryCommitFast(ctx context.Context, l *live[S], out workResult[S], maxInvocations int, cursor uint64, store Store[S], obs *observation.Session) (bool, string, error) {
	if out.err != nil || ctx.Err() != nil || l.header.Revision == math.MaxUint64 {
		return false, "", nil
	}
	if out.ref.join {
		if store != nil {
			return false, "", nil
		}
		ok, groupID := r.tryCommitFastJoin(ctx, l, out, cursor, obs)
		return ok, groupID, nil
	}
	inv, slot, ok := l.invocation(out.ref.id, out.ref.slot)
	if !ok {
		return false, "", nil
	}
	var lg *liveGroup[S]
	if inv.GroupID != "" {
		var gOk bool
		lg, _, gOk = l.group(inv.GroupID, -1)
		if !gOk || inv.BranchIndex < 0 || inv.BranchIndex >= len(lg.group.Branches) || lg.group.Branches[inv.BranchIndex].InvocationID != inv.ID {
			return false, "", nil
		}
	}
	source := inv.Node
	n := out.node
	if n == nil || n.Spec.Name != source {
		n = r.node(source)
	}
	tr := model.ReadTransition(out.transition)
	switch tr.Action {
	case ActionContinue:
		if len(tr.Targets) > 0 {
			return false, "", nil
		}
		edge, err := r.resolveEdge(n, source, tr.Target)
		if err != nil {
			return false, "", nil
		}
		return r.commitFastEdge(ctx, l, inv, slot, lg, tr.State, edge, cursor, store, obs)
	case ActionEndExecution:
		if store != nil {
			h := l.header.Header()
			h.Revision = l.header.Revision + 1
			h.Steps = l.header.Steps + 1
			h.ScheduleCursor = cursor
			h.Completed = true
			c := Change[S]{
				Header: h,
				Final:  &tr.State,
			}
			if err := store.Commit(ctx, l.header.Revision, c); err != nil {
				outcome := model.OutcomeUnknown
				if errors.Is(err, ErrConflict) {
					outcome = model.OutcomeDiscarded
				}
				obs.ResolveSelected(ctx, outcome, c.Header.Revision, err)
				return true, "", err
			}
		}
		l.finalVal = tr.State
		l.header.Revision++
		l.header.Steps++
		l.header.ScheduleCursor = cursor
		l.header.Final = &l.finalVal
		l.header.Completed = true
		l.clearActive()
		obs.ResolveSelected(ctx, model.OutcomeCommitted, l.header.Revision, nil)
		return true, "", nil
	case ActionCompleteBranch:
		if lg == nil || lg.group.JoinNode == "" {
			return false, "", nil
		}
		updatedGroup, hasTimeoutUpdate := cloneGroupWithTimeoutCleared(lg, inv.BranchIndex)
		updatedInv := *inv
		updatedInv.State = tr.State
		updatedInv.Status = InvocationJoined
		updatedInv.CallID = ""
		updatedInv.Node = lg.group.JoinNode
		updatedInv.Next = nil
		updatedInv.Continuation = ""

		if store != nil {
			h := l.header.Header()
			h.Revision = l.header.Revision + 1
			h.Steps = l.header.Steps + 1
			h.ScheduleCursor = cursor
			c := Change[S]{
				Header:            h,
				UpsertInvocations: []Invocation[S]{updatedInv},
			}
			if hasTimeoutUpdate {
				c.UpsertGroups = []ActivationGroup{updatedGroup}
			}
			if err := store.Commit(ctx, l.header.Revision, c); err != nil {
				outcome := model.OutcomeUnknown
				if errors.Is(err, ErrConflict) {
					outcome = model.OutcomeDiscarded
				}
				obs.ResolveSelected(ctx, outcome, c.Header.Revision, err)
				return true, "", err
			}
		}

		if hasTimeoutUpdate {
			lg.group = updatedGroup
		}
		oldStatus := inv.Status
		*inv = updatedInv
		l.readyNodes -= b2i(oldStatus == InvocationReady)
		l.waitingInvocations -= b2i(oldStatus == InvocationWaiting)
		if !oldStatus.Resolved() {
			lg.remaining--
		}
		groupID := inv.GroupID
		l.enqueueJoinIfReady(groupID)
		l.header.Revision++
		l.header.Steps++
		l.header.ScheduleCursor = cursor
		obs.ResolveSelected(ctx, model.OutcomeCommitted, l.header.Revision, nil)
		return true, groupID, nil
	case ActionEndBranch:
		return r.commitFastEndBranch(ctx, l, inv, lg, tr.State, false, cursor, store, obs)
	case ActionReturn:
		if n == nil || !n.Mounted {
			return false, "", nil
		}
		if n.ReturnTarget == "" {
			return r.commitFastEndBranch(ctx, l, inv, lg, tr.State, true, cursor, store, obs)
		}
		edge := CompiledEdge{Target: n.ReturnTarget, NodeIndex: n.ReturnNodeIndex, JoinIndex: n.ReturnJoinIndex}
		return r.commitFastEdge(ctx, l, inv, slot, lg, tr.State, edge, cursor, store, obs)
	case ActionWait:
		if len(tr.Targets) > 0 || tr.Continuation == "" || r.continuation(tr.Continuation) == nil || l.header.NextID == math.MaxUint64 {
			return false, "", nil
		}
		edge, err := r.resolveEdge(n, source, tr.Target)
		if err != nil {
			return false, "", nil
		}
		if edge.JoinIndex >= 0 && (lg == nil || lg.group.JoinNode != edge.Target) {
			return false, "", nil
		}

		updatedGroup, hasTimeoutUpdate := cloneGroupWithTimeoutCleared(lg, inv.BranchIndex)
		nextID := l.header.NextID
		callID := model.AllocID(&nextID, "c")

		updatedInv := *inv
		updatedInv.State = tr.State
		updatedInv.Status = InvocationWaiting
		updatedInv.CallID = callID
		updatedInv.Continuation = tr.Continuation
		updatedInv.Next = []string{tr.Target}

		if store != nil {
			h := l.header.Header()
			h.Revision = l.header.Revision + 1
			h.Steps = l.header.Steps + 1
			h.NextID = nextID
			h.ScheduleCursor = cursor
			c := Change[S]{
				Header:            h,
				UpsertInvocations: []Invocation[S]{updatedInv},
			}
			if hasTimeoutUpdate {
				c.UpsertGroups = []ActivationGroup{updatedGroup}
			}
			if err := store.Commit(ctx, l.header.Revision, c); err != nil {
				outcome := model.OutcomeUnknown
				if errors.Is(err, ErrConflict) {
					outcome = model.OutcomeDiscarded
				}
				obs.ResolveSelected(ctx, outcome, c.Header.Revision, err)
				return true, "", err
			}
		}

		if hasTimeoutUpdate {
			lg.group = updatedGroup
		}
		oldStatus := inv.Status
		*inv = updatedInv
		l.header.NextID = nextID
		l.readyNodes -= b2i(oldStatus == InvocationReady)
		l.waitingInvocations += b2i(oldStatus != InvocationWaiting)
		l.header.Revision++
		l.header.Steps++
		l.header.ScheduleCursor = cursor
		obs.ResolveSelected(ctx, model.OutcomeCommitted, l.header.Revision, nil)
		return true, inv.GroupID, nil
	case ActionFanOut:
		if store != nil {
			return false, "", nil
		}
		ok, gID := r.tryCommitFastFanOut(ctx, l, inv, slot, lg, n, source, tr.State, tr.Branches, maxInvocations, cursor, obs)
		return ok, gID, nil
	default:
		return false, "", nil
	}
}

func (r *Runner[S]) commitFastEdge(ctx context.Context, l *live[S], inv *Invocation[S], slot int, lg *liveGroup[S], state S, edge CompiledEdge, cursor uint64, store Store[S], obs *observation.Session) (bool, string, error) {
	updatedGroup, hasTimeoutUpdate := cloneGroupWithTimeoutCleared(lg, inv.BranchIndex)
	if edge.JoinIndex >= 0 {
		if lg == nil || lg.group.JoinNode != edge.Target {
			return false, "", nil
		}

		updatedInv := *inv
		updatedInv.State = state
		updatedInv.Continuation = ""
		updatedInv.Next = nil
		updatedInv.Status = InvocationJoined
		updatedInv.CallID = ""
		updatedInv.Node = edge.Target

		if store != nil {
			h := l.header.Header()
			h.Revision = l.header.Revision + 1
			h.Steps = l.header.Steps + 1
			h.ScheduleCursor = cursor
			c := Change[S]{
				Header:            h,
				UpsertInvocations: []Invocation[S]{updatedInv},
			}
			if hasTimeoutUpdate {
				c.UpsertGroups = []ActivationGroup{updatedGroup}
			}
			if err := store.Commit(ctx, l.header.Revision, c); err != nil {
				outcome := model.OutcomeUnknown
				if errors.Is(err, ErrConflict) {
					outcome = model.OutcomeDiscarded
				}
				obs.ResolveSelected(ctx, outcome, c.Header.Revision, err)
				return true, "", err
			}
		}

		if hasTimeoutUpdate {
			lg.group = updatedGroup
		}
		oldStatus := inv.Status
		*inv = updatedInv
		l.readyNodes -= b2i(oldStatus == InvocationReady)
		l.waitingInvocations -= b2i(oldStatus == InvocationWaiting)
		if !oldStatus.Resolved() {
			lg.remaining--
		}
		groupID := inv.GroupID
		l.enqueueJoinIfReady(groupID)
		l.header.Revision++
		l.header.Steps++
		l.header.ScheduleCursor = cursor
		obs.ResolveSelected(ctx, model.OutcomeCommitted, l.header.Revision, nil)
		return true, groupID, nil
	}
	if edge.NodeIndex < 0 || l.header.NextID == math.MaxUint64 {
		return false, "", nil
	}

	nextID := l.header.NextID
	callID := model.AllocID(&nextID, "c")

	updatedInv := *inv
	updatedInv.State = state
	updatedInv.Continuation = ""
	updatedInv.Next = nil
	updatedInv.Status = InvocationReady
	updatedInv.CallID = callID
	updatedInv.Node = edge.Target

	if store != nil {
		h := l.header.Header()
		h.Revision = l.header.Revision + 1
		h.Steps = l.header.Steps + 1
		h.NextID = nextID
		h.ScheduleCursor = cursor
		c := Change[S]{
			Header:            h,
			UpsertInvocations: []Invocation[S]{updatedInv},
		}
		if hasTimeoutUpdate {
			c.UpsertGroups = []ActivationGroup{updatedGroup}
		}
		if err := store.Commit(ctx, l.header.Revision, c); err != nil {
			outcome := model.OutcomeUnknown
			if errors.Is(err, ErrConflict) {
				outcome = model.OutcomeDiscarded
			}
			obs.ResolveSelected(ctx, outcome, c.Header.Revision, err)
			return true, "", err
		}
	}

	if hasTimeoutUpdate {
		lg.group = updatedGroup
	}
	oldStatus := inv.Status
	*inv = updatedInv
	l.header.NextID = nextID
	l.readyNodes += b2i(oldStatus != InvocationReady)
	l.waitingInvocations -= b2i(oldStatus == InvocationWaiting)
	l.nodeQueue.push(taskRef{id: inv.ID, slot: slot, nodeIdx: edge.NodeIndex, join: false})
	l.header.Revision++
	l.header.Steps++
	l.header.ScheduleCursor = cursor
	obs.ResolveSelected(ctx, model.OutcomeCommitted, l.header.Revision, nil)
	return true, inv.GroupID, nil
}

func (r *Runner[S]) commitFastEndBranch(ctx context.Context, l *live[S], inv *Invocation[S], lg *liveGroup[S], state S, updateState bool, cursor uint64, store Store[S], obs *observation.Session) (bool, string, error) {
	isCompleted := len(l.groups) == 0

	updatedGroup, hasTimeoutUpdate := cloneGroupWithTimeoutCleared(lg, inv.BranchIndex)
	updatedInv := *inv
	if updateState {
		updatedInv.State = state
	}
	updatedInv.Status = InvocationEnded
	updatedInv.CallID = ""
	updatedInv.Next = nil
	updatedInv.Continuation = ""

	if store != nil {
		h := l.header.Header()
		h.Revision = l.header.Revision + 1
		h.Steps = l.header.Steps + 1
		h.ScheduleCursor = cursor
		if isCompleted {
			h.Completed = true
		}
		c := Change[S]{
			Header: h,
		}
		if !isCompleted {
			c.UpsertInvocations = []Invocation[S]{updatedInv}
			if hasTimeoutUpdate {
				c.UpsertGroups = []ActivationGroup{updatedGroup}
			}
		}
		if err := store.Commit(ctx, l.header.Revision, c); err != nil {
			outcome := model.OutcomeUnknown
			if errors.Is(err, ErrConflict) {
				outcome = model.OutcomeDiscarded
			}
			obs.ResolveSelected(ctx, outcome, c.Header.Revision, err)
			return true, "", err
		}
	}

	if hasTimeoutUpdate {
		lg.group = updatedGroup
	}
	oldStatus := inv.Status
	*inv = updatedInv
	l.readyNodes -= b2i(oldStatus == InvocationReady)
	l.waitingInvocations -= b2i(oldStatus == InvocationWaiting)
	groupID := inv.GroupID
	if lg != nil {
		if !oldStatus.Resolved() {
			lg.remaining--
		}
		l.enqueueJoinIfReady(groupID)
	}
	l.header.Revision++
	l.header.Steps++
	l.header.ScheduleCursor = cursor
	if isCompleted {
		l.header.Completed = true
		l.clearActive()
	}
	obs.ResolveSelected(ctx, model.OutcomeCommitted, l.header.Revision, nil)
	return true, groupID, nil
}

func (r *Runner[S]) tryCommitFastFanOut(ctx context.Context, l *live[S], parent *Invocation[S], parentSlot int, parentGroup *liveGroup[S], sourceNode *CompiledNode[S], source string, parentState S, branches []Branch[S], maxInvocations int, cursor uint64, obs *observation.Session) (bool, string) {
	if sourceNode == nil || sourceNode.JoinIndex < 0 {
		return false, ""
	}
	if maxInvocations > 0 && l.invocationCount()+len(branches) > maxInvocations {
		return false, ""
	}
	if uint64(len(branches)) > (math.MaxUint64-2)/2 {
		return false, ""
	}
	needed := uint64(2) + 2*uint64(len(branches))
	if l.header.NextID > math.MaxUint64-needed {
		return false, ""
	}
	edges := r.edges[sourceNode.EdgeStart:sourceNode.EdgeEnd]
	for _, branch := range branches {
		if branch.EntryTimeout < 0 {
			return false, ""
		}
		foundNodeIdx := -1
		for i := range edges {
			if edges[i].Target == branch.Target {
				foundNodeIdx = edges[i].NodeIndex
				break
			}
		}
		if foundNodeIdx < 0 {
			return false, ""
		}
	}
	clonedStates := make([]S, len(branches))
	for i, branch := range branches {
		state, err := r.cloneState(branch.State, parent.ID)
		if err != nil {
			return false, ""
		}
		clonedStates[i] = state
	}
	clearEntryTimeoutInLive(parentGroup, parent.BranchIndex)
	compiledJoin := &r.joins[sourceNode.JoinIndex]
	joinNode := compiledJoin.Spec.Name
	groupID := model.AllocID(&l.header.NextID, "g")
	groupCallID := model.AllocID(&l.header.NextID, "c")
	actBranches := make([]ActivationBranch, len(branches))

	oldStatus := parent.Status
	parentID := parent.ID
	touchedGroupID := parent.GroupID
	parent.State = parentState
	parent.Status = InvocationGroup
	parent.CallID = ""
	parent.ChildGroupID = groupID
	parent.Continuation = ""
	parent.Next = nil
	l.readyNodes -= b2i(oldStatus == InvocationReady)
	l.waitingInvocations -= b2i(oldStatus == InvocationWaiting)

	for branchIndex, branch := range branches {
		childID := model.AllocID(&l.header.NextID, "i")
		callID := model.AllocID(&l.header.NextID, "c")
		actBranches[branchIndex] = ActivationBranch{
			InvocationID: childID,
			Target:       branch.Target,
			EntryTimeout: branch.EntryTimeout,
		}
		nodeIdx := 0
		for i := range edges {
			if edges[i].Target == branch.Target {
				nodeIdx = edges[i].NodeIndex
				break
			}
		}
		childSlot := l.putNewInvocation(Invocation[S]{
			ID:          childID,
			CallID:      callID,
			Node:        branch.Target,
			State:       clonedStates[branchIndex],
			Status:      InvocationReady,
			GroupID:     groupID,
			BranchIndex: branchIndex,
		})
		l.readyNodes++
		l.nodeQueue.push(taskRef{id: childID, slot: childSlot, nodeIdx: nodeIdx, join: false})
	}
	_ = parentSlot
	gSlot := l.putGroup(liveGroup[S]{
		group: ActivationGroup{
			ID:       groupID,
			CallID:   groupCallID,
			Source:   source,
			ParentID: parentID,
			JoinNode: joinNode,
			Branches: actBranches,
		},
		remaining: len(branches),
		join:      compiledJoin,
	})
	if l.joinEligible(&l.groups[gSlot]) {
		l.groups[gSlot].joinEnqueued = true
		l.joinQueue.push(taskRef{id: groupID, slot: gSlot, join: true})
	}
	l.header.Revision++
	l.header.Steps++
	l.header.ScheduleCursor = cursor
	obs.ResolveSelected(ctx, model.OutcomeCommitted, l.header.Revision, nil)
	return true, touchedGroupID
}

func (r *Runner[S]) tryCommitFastJoin(ctx context.Context, l *live[S], out workResult[S], cursor uint64, obs *observation.Session) (bool, string) {
	lg, _, ok := l.group(out.ref.id, out.ref.slot)
	if !ok {
		return false, ""
	}
	for _, branch := range lg.group.Branches {
		child, _, exists := l.invocation(branch.InvocationID, -1)
		if !exists || child.ChildGroupID != "" {
			return false, ""
		}
	}
	parent, _, pOk := l.invocation(lg.group.ParentID, -1)
	if !pOk {
		return false, ""
	}
	nextTarget := ""
	nextNodeIdx := -1
	nextJoinIdx := -1
	if lg.group.JoinNode != "" {
		j := lg.join
		if j == nil {
			j = r.join(lg.group.JoinNode)
		}
		if j == nil {
			return false, ""
		}
		nextTarget = j.NextTarget
		nextNodeIdx = j.NextNodeIndex
		nextJoinIdx = j.NextJoinIndex
		if nextTarget != "" {
			if nextJoinIdx >= 0 {
				if parent.GroupID == "" {
					return false, ""
				}
				pg, _, pgOk := l.group(parent.GroupID, -1)
				if !pgOk || pg.group.JoinNode != nextTarget {
					return false, ""
				}
			} else if nextNodeIdx < 0 || l.header.NextID == math.MaxUint64 {
				return false, ""
			}
		}
	}
	parentID := lg.group.ParentID
	groupID := lg.group.ID
	joinNode := lg.group.JoinNode
	for _, branch := range lg.group.Branches {
		if child, _, exists := l.invocation(branch.InvocationID, -1); exists {
			l.readyNodes -= b2i(child.Status == InvocationReady)
			l.waitingInvocations -= b2i(child.Status == InvocationWaiting)
			l.deleteInvocation(branch.InvocationID)
		}
	}
	l.deleteGroup(groupID)
	parent, parentSlot, _ := l.invocation(parentID, -1)
	parent.ChildGroupID = ""
	touchedGroupID := parent.GroupID
	if joinNode == "" {
		parent.Status = InvocationEnded
		parent.CallID = ""
	} else {
		parent.State = out.state
		if nextTarget == "" {
			parent.Status = InvocationEnded
			parent.CallID = ""
		} else if nextJoinIdx >= 0 {
			parent.Status = InvocationJoined
			parent.CallID = ""
			parent.Node = nextTarget
		} else {
			parent.Status = InvocationReady
			parent.CallID = model.AllocID(&l.header.NextID, "c")
			parent.Node = nextTarget
			l.readyNodes++
			l.nodeQueue.push(taskRef{id: parent.ID, slot: parentSlot, nodeIdx: nextNodeIdx, join: false})
		}
	}
	if parent.Status.Resolved() && parent.GroupID != "" {
		if pg, _, pgOk := l.group(parent.GroupID, -1); pgOk {
			pg.remaining--
		}
		l.enqueueJoinIfReady(parent.GroupID)
	}
	l.header.Revision++
	l.header.ScheduleCursor = cursor
	if len(l.groups) == 0 && (len(l.invocations) == 0 || (len(l.invocations) == 1 && l.invocations[0].Status.Resolved())) {
		l.header.Completed = true
		l.clearActive()
	}
	obs.ResolveSelected(ctx, model.OutcomeCommitted, l.header.Revision, nil)
	return true, touchedGroupID
}

func (r *Runner[S]) submit(ctx context.Context, l *live[S], c Change[S], store Store[S], obs *observation.Session) error {
	if err := ctx.Err(); err != nil {
		obs.ResolveSelected(ctx, model.OutcomeDiscarded, 0, err)
		return err
	}
	if l.header.Revision == math.MaxUint64 {
		obs.ResolveSelected(ctx, model.OutcomeDiscarded, 0, ErrExecutionLimit)
		return ErrExecutionLimit
	}
	c.Header.Revision = l.header.Revision + 1
	l.complete(&c)
	if store != nil {
		if err := store.Commit(ctx, l.header.Revision, c); err != nil {
			outcome := model.OutcomeUnknown
			if errors.Is(err, ErrConflict) {
				outcome = model.OutcomeDiscarded
			}
			obs.ResolveSelected(ctx, outcome, c.Header.Revision, err)
			return err
		}
	}
	l.install(c)
	obs.ResolveSelected(ctx, model.OutcomeCommitted, c.Header.Revision, nil)
	return nil
}
