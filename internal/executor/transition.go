package executor

import (
	"errors"
	"fmt"
	"math"

	"github.com/afterlune/luneGraph/internal/model"
)

func (r *Runner[S]) applyTransition(pos *candidatePosition[S], index *invocationIndex, id string, transition Transition[S], budget invocationBudget) error {
	_, inv := indexedInvocation(index, pos, id)
	if inv == nil {
		return &TransitionError{InvocationID: id, Cause: fmt.Errorf("invocation disappeared")}
	}
	tr := model.ReadTransition(transition)
	source := inv.Node
	switch tr.Action {
	case ActionContinue:
		if len(tr.Targets) == 0 {
			target := tr.Target
			edge, err := r.resolveEdge(nil, source, target)
			if err != nil {
				return &TransitionError{InvocationID: id, Node: source, Cause: err}
			}
			return r.applyEdgeTransition(pos, inv, tr.State, edge)
		}
		if err := r.checkTargets(source, tr.Targets); err != nil {
			return &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		if err := r.checkJoinTargets(pos.Groups, inv, tr.Targets); err != nil {
			return &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		if err := r.route(pos, index, id, source, tr.State, tr.Targets, budget); err != nil {
			if _, ok := errors.AsType[*stateCopyError](err); ok || errors.Is(err, ErrExecutionLimit) || errors.Is(err, errInvocationBudget) {
				return err
			}
			return &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		return nil
	case ActionWait:
		if err := r.validateWait(tr.Continuation, pos.NextID); err != nil {
			if errors.Is(err, ErrExecutionLimit) {
				return err
			}
			return &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		var next []string
		if len(tr.Targets) == 0 {
			if err := r.checkWaitTarget(nil, inv, tr.Target, invocationJoin(pos, inv)); err != nil {
				return &TransitionError{InvocationID: id, Node: source, Cause: err}
			}
			next = []string{tr.Target}
		} else {
			if err := r.checkTargets(source, tr.Targets); err != nil {
				return &TransitionError{InvocationID: id, Node: source, Cause: err}
			}
			if err := r.checkJoinTargets(pos.Groups, inv, tr.Targets); err != nil {
				return &TransitionError{InvocationID: id, Node: source, Cause: err}
			}
			next = append([]string(nil), tr.Targets...)
		}
		setWaiting(inv, tr.State, model.AllocID(&pos.NextID, "c"), tr.Continuation, next)
		return nil
	case ActionFanOut:
		if err := r.fanOut(pos, index, inv, source, tr.State, tr.Branches, budget); err != nil {
			if _, ok := errors.AsType[*stateCopyError](err); ok || errors.Is(err, ErrExecutionLimit) || errors.Is(err, errInvocationBudget) {
				return err
			}
			return &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		return nil
	case ActionCompleteBranch:
		if inv.GroupID == "" {
			return &TransitionError{InvocationID: id, Node: source, Cause: errors.New("CompleteBranch used outside an activation group")}
		}
		activation := findActivationGroup(pos.Groups, inv.GroupID)
		if activation == nil || activation.JoinNode == "" {
			return &TransitionError{InvocationID: id, Node: source, Cause: errors.New("CompleteBranch requires a group with a join")}
		}
		inv.State = tr.State
		inv.Status = InvocationJoined
		inv.CallID = ""
		inv.Node = activation.JoinNode
		inv.Next = nil
		inv.Continuation = ""
		return nil
	case ActionEndBranch:
		inv.Status = InvocationEnded
		inv.CallID = ""
		inv.Next = nil
		inv.Continuation = ""
		return nil
	case ActionEndExecution:
		state := tr.State
		pos.Final = &state
		pos.Completed = true
		pos.Invocations = nil
		pos.Groups = nil
		clearInvocationIndex(index)
		return nil
	case ActionReturn:
		n := r.node(source)
		if n == nil || !n.Mounted {
			return &TransitionError{InvocationID: id, Node: source, Cause: errors.New("Return used outside a subgraph")}
		}
		target := n.ReturnEdge.Target
		if target == "" {
			inv.State = tr.State
			inv.Status = InvocationEnded
			inv.CallID = ""
			inv.Next = nil
			inv.Continuation = ""
			return nil
		}
		return r.applyEdgeTransition(pos, inv, tr.State, n.ReturnEdge)
	default:
		return &TransitionError{InvocationID: id, Node: source, Cause: fmt.Errorf("invalid action %d", tr.Action)}
	}
}

func (r *Runner[S]) applyEdgeTransition(pos *candidatePosition[S], inv *Invocation[S], state S, edge CompiledEdge) error {
	source := inv.Node
	if edge.TargetJoinIndex >= 0 {
		if err := checkJoinDestination(inv.ID, edge, invocationJoin(pos, inv)); err != nil {
			return &TransitionError{InvocationID: inv.ID, Node: source, Cause: err}
		}
	}
	inv.State = state
	inv.Continuation = ""
	inv.Next = nil
	if err := r.setTargetEdge(pos, inv, edge); err != nil {
		if _, ok := errors.AsType[*stateCopyError](err); ok || errors.Is(err, ErrExecutionLimit) || errors.Is(err, errInvocationBudget) {
			return err
		}
		return &TransitionError{InvocationID: inv.ID, Node: source, Cause: err}
	}
	return nil
}

func (r *Runner[S]) validateWait(continuation string, nextID uint64) error {
	if continuation == "" {
		return errors.New("Wait requires non-empty continuation key")
	}
	if r.continuation(continuation) == nil {
		return fmt.Errorf("unknown continuation %q", continuation)
	}
	if nextID == math.MaxUint64 {
		return fmt.Errorf("allocate continuation call ID: %w", ErrExecutionLimit)
	}
	return nil
}

func (r *Runner[S]) checkWaitTarget(node *CompiledNode[S], inv *Invocation[S], target, expectedJoin string) error {
	edge, err := r.resolveEdge(node, inv.Node, target)
	if err != nil {
		return err
	}
	return checkJoinDestination(inv.ID, edge, expectedJoin)
}

func setWaiting[S any](inv *Invocation[S], state S, callID, continuation string, next []string) {
	inv.State = state
	inv.Status = InvocationWaiting
	inv.CallID = callID
	inv.Continuation = continuation
	inv.Next = next
}

func invocationJoin[S any](pos *candidatePosition[S], inv *Invocation[S]) string {
	if inv.GroupID != "" {
		if activation := findActivationGroup(pos.Groups, inv.GroupID); activation != nil {
			return activation.JoinNode
		}
	}
	return ""
}

func checkJoinDestination(invocationID string, edge CompiledEdge, expected string) error {
	if edge.TargetJoinIndex >= 0 && edge.Target != expected {
		return fmt.Errorf("invocation %q reached join %q outside its activation group", invocationID, edge.Target)
	}
	return nil
}

func (r *Runner[S]) checkJoinTargets(groups []ActivationGroup, inv *Invocation[S], targets []string) error {
	if len(r.joins) == 0 {
		return nil
	}
	for _, target := range targets {
		if r.join(target) == nil {
			continue
		}
		var expected string
		if len(targets) > 1 {
			if n := r.node(inv.Node); n != nil && n.FanOutJoinIndex >= 0 {
				expected = r.joins[n.FanOutJoinIndex].Spec.Name
			}
		} else if inv.GroupID != "" {
			group := findActivationGroup(groups, inv.GroupID)
			if group != nil {
				expected = group.JoinNode
			}
		}
		if target != expected {
			return fmt.Errorf("invocation %q reached join %q outside its activation group", inv.ID, target)
		}
	}
	return nil
}

type stateCopyError struct{ cause error }

func (e *stateCopyError) Error() string { return e.cause.Error() }
func (e *stateCopyError) Unwrap() error { return e.cause }

func (r *Runner[S]) route(pos *candidatePosition[S], index *invocationIndex, id, source string, state S, targets []string, budget invocationBudget) error {
	_, inv := indexedInvocation(index, pos, id)
	if inv == nil {
		return fmt.Errorf("invocation %q disappeared", id)
	}
	if len(targets) == 1 {
		inv.State = state
		inv.Continuation = ""
		inv.Next = nil

		return r.setTarget(pos, inv, targets[0])
	}
	if err := budget.check(len(targets)); err != nil {
		return err
	}
	needed := uint64(1) // activation group
	addIDs := func(count uint64) bool {
		if needed > math.MaxUint64-count {
			return false
		}
		needed += count
		return true
	}
	if !addIDs(uint64(len(targets))) { // child invocations
		return fmt.Errorf("reserve fan-out IDs: %w", ErrExecutionLimit)
	}
	joinNode := ""
	if n := r.node(source); n != nil && n.FanOutJoinIndex >= 0 {
		joinNode = r.joins[n.FanOutJoinIndex].Spec.Name
	}
	if joinNode != "" {
		if !addIDs(1) { // join callback call ID
			return fmt.Errorf("reserve fan-out IDs: %w", ErrExecutionLimit)
		}
	}
	for _, target := range targets {
		if r.join(target) == nil {
			if !addIDs(1) { // child node callback call ID
				return fmt.Errorf("reserve fan-out IDs: %w", ErrExecutionLimit)
			}
		}
	}
	if pos.NextID > math.MaxUint64-needed {
		return fmt.Errorf("reserve fan-out IDs: %w", ErrExecutionLimit)
	}
	reserveInvocationIndex(index, pos, len(targets))
	groupID := model.AllocID(&pos.NextID, "g")
	group := ActivationGroup{
		ID:       groupID,
		Source:   source,
		ParentID: id,
		JoinNode: joinNode,
		Branches: make([]ActivationBranch, len(targets)),
	}
	if group.JoinNode != "" {
		group.CallID = model.AllocID(&pos.NextID, "c")
	}
	inv.State = state
	inv.Status = InvocationGroup
	inv.CallID = ""
	inv.ChildGroupID = groupID
	inv.Continuation = ""
	inv.Next = nil
	pos.Groups = append(pos.Groups, group)
	for branchIndex, target := range targets {
		childState, err := r.cloneState(state, id)
		if err != nil {
			return &stateCopyError{cause: fmt.Errorf("clone fan-out state: %w", err)}
		}
		child := Invocation[S]{ID: model.AllocID(&pos.NextID, "i"), State: childState, GroupID: groupID, BranchIndex: branchIndex}
		group.Branches[branchIndex] = ActivationBranch{InvocationID: child.ID, Target: target}
		appended := appendInvocation(index, pos, child)
		if err := r.setTarget(pos, appended, target); err != nil {
			return err
		}
	}
	return nil
}

// validateFanOut checks the whole activation before either commit path clones state.
func (r *Runner[S]) validateFanOut(sourceNode *CompiledNode[S], source string, branches []Branch[S], budget invocationBudget, nextID uint64) (*CompiledJoin[S], error) {
	if sourceNode == nil {
		return nil, fmt.Errorf("node %q is unknown", source)
	}
	var join *CompiledJoin[S]
	if sourceNode.FanOutJoinIndex >= 0 {
		join = &r.joins[sourceNode.FanOutJoinIndex]
	}
	edges := r.edges[sourceNode.EdgeStart:sourceNode.EdgeEnd]
	for branchIndex, branch := range branches {
		if branch.EntryTimeout < 0 {
			return nil, fmt.Errorf("branch %d timeout must not be negative", branchIndex)
		}
		i := findEdge(edges, branch.Target)
		if i < 0 {
			return nil, fmt.Errorf("node %q selected undeclared branch target %q", source, branch.Target)
		}
		if edges[i].TargetNodeIndex < 0 {
			return nil, fmt.Errorf("branch target %q is not an executable node", branch.Target)
		}
	}
	if err := budget.check(len(branches)); err != nil {
		return nil, err
	}
	if join == nil && len(branches) == 0 {
		return nil, nil
	}
	children := uint64(len(branches))
	// Reserve IDs for the group (plus join callback when joined) and the invocation and callback of each child.
	reserved := uint64(1)
	if join != nil {
		reserved = 2
	}
	if children > (math.MaxUint64-reserved)/2 || nextID > math.MaxUint64-(reserved+2*children) {
		return nil, fmt.Errorf("reserve FanOut IDs: %w", ErrExecutionLimit)
	}
	return join, nil
}

// fanOut creates a dynamic activation from declared, independently cloned tasks.
func (r *Runner[S]) fanOut(pos *candidatePosition[S], index *invocationIndex, parent *Invocation[S], source string, parentState S, branches []Branch[S], budget invocationBudget) error {
	join, err := r.validateFanOut(r.node(source), source, branches, budget, pos.NextID)
	if err != nil {
		return err
	}
	if join == nil && len(branches) == 0 {
		parent.State = parentState
		parent.Status = InvocationEnded
		parent.CallID = ""
		parent.ChildGroupID = ""
		parent.Continuation = ""
		parent.Next = nil
		return nil
	}
	reserveInvocationIndex(index, pos, len(branches))
	groupID := model.AllocID(&pos.NextID, "g")
	var joinCallID, joinNode string
	if join != nil {
		joinCallID = model.AllocID(&pos.NextID, "c")
		joinNode = join.Spec.Name
	}
	activation := ActivationGroup{
		ID:       groupID,
		CallID:   joinCallID,
		Source:   source,
		ParentID: parent.ID,
		JoinNode: joinNode,
		Branches: make([]ActivationBranch, len(branches)),
	}
	parent.State = parentState
	parent.Status = InvocationGroup
	parent.CallID = ""
	parent.ChildGroupID = groupID
	parent.Continuation = ""
	parent.Next = nil
	for branchIndex, branch := range branches {
		state, err := r.cloneState(branch.State, parent.ID)
		if err != nil {
			return &stateCopyError{cause: fmt.Errorf("clone FanOut branch %d: %w", branchIndex, err)}
		}
		child := Invocation[S]{
			ID:          model.AllocID(&pos.NextID, "i"),
			State:       state,
			Status:      InvocationReady,
			GroupID:     groupID,
			BranchIndex: branchIndex,
		}
		activation.Branches[branchIndex] = ActivationBranch{InvocationID: child.ID, Target: branch.Target, EntryTimeout: branch.EntryTimeout}
		childPosition := appendInvocation(index, pos, child)
		if err := r.setTarget(pos, childPosition, branch.Target); err != nil {
			return err
		}
	}
	pos.Groups = append(pos.Groups, activation)
	return nil
}

func (r *Runner[S]) setTargetEdge(pos *candidatePosition[S], inv *Invocation[S], edge CompiledEdge) error {
	if edge.TargetJoinIndex >= 0 {
		if inv.GroupID == "" {
			return fmt.Errorf("invocation %q reached join %q outside a group", inv.ID, edge.Target)
		}
		group := findActivationGroup(pos.Groups, inv.GroupID)
		if group == nil {
			return fmt.Errorf("invocation %q has missing group %q", inv.ID, inv.GroupID)
		}
		if group.JoinNode != edge.Target {
			return fmt.Errorf("invocation %q reached join %q for group %q", inv.ID, edge.Target, group.ID)
		}
		inv.Status = InvocationJoined
		inv.CallID = ""
	} else {
		if edge.TargetNodeIndex < 0 {
			return fmt.Errorf("unknown node %q", edge.Target)
		}
		if pos.NextID == math.MaxUint64 {
			return fmt.Errorf("allocate node call ID: %w", ErrExecutionLimit)
		}
		inv.Status = InvocationReady
		inv.CallID = model.AllocID(&pos.NextID, "c")
	}
	inv.Node = edge.Target
	return nil
}

func (r *Runner[S]) setTarget(pos *candidatePosition[S], inv *Invocation[S], target string) error {
	nIdx := -1
	if idx, ok := r.nodeIndex[target]; ok {
		nIdx = idx
	}
	jIdx := -1
	if idx, ok := r.joinIndex[target]; ok {
		jIdx = idx
	}
	return r.setTargetEdge(pos, inv, CompiledEdge{Target: target, TargetNodeIndex: nIdx, TargetJoinIndex: jIdx})
}
