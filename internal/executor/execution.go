package executor

import (
	"errors"
	"fmt"
	"math"

	"github.com/afterlune/luneGraph/internal/model"
)

func (r *Runner[S]) applyTransition(s *Checkpoint[S], index *invocationIndex, id string, transition Transition[S], budget invocationBudget) (bool, error) {
	_, inv := indexedInvocation(index, s, id)
	if inv == nil {
		return false, &TransitionError{InvocationID: id, Cause: fmt.Errorf("invocation disappeared")}
	}
	tr := model.ReadTransition(transition)
	source := inv.Node
	switch tr.Action {
	case ActionContinue:
		if len(tr.Targets) == 0 {
			target := tr.Target
			edge, err := r.resolveEdge(nil, source, target)
			if err != nil {
				return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
			}
			if err := r.checkJoinEdge(s, inv, edge); err != nil {
				return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
			}
			inv.State = tr.State
			inv.Continuation = ""
			inv.Next = nil
			if err := r.setTargetEdge(s, inv, edge); err != nil {
				if _, ok := errors.AsType[*stateCopyError](err); ok || errors.Is(err, ErrExecutionLimit) || errors.Is(err, errInvocationBudget) {
					return false, err
				}
				return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
			}
			return false, nil
		}
		if err := r.checkTargets(source, tr.Targets); err != nil {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		if err := r.checkJoinTargets(s, inv, tr.Targets); err != nil {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		if err := r.route(s, index, id, source, tr.State, tr.Targets, budget); err != nil {
			if _, ok := errors.AsType[*stateCopyError](err); ok || errors.Is(err, ErrExecutionLimit) || errors.Is(err, errInvocationBudget) {
				return false, err
			}
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		return false, nil
	case ActionWait:
		if tr.Continuation == "" {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: errors.New("Wait requires non-empty continuation key")}
		}
		if r.continuation(tr.Continuation) == nil {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: fmt.Errorf("unknown continuation %q", tr.Continuation)}
		}
		if s.NextID == math.MaxUint64 {
			return false, fmt.Errorf("allocate continuation call ID: %w", ErrExecutionLimit)
		}
		if len(tr.Targets) == 0 {
			target := tr.Target
			edge, err := r.resolveEdge(nil, source, target)
			if err != nil {
				return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
			}
			if err := r.checkJoinEdge(s, inv, edge); err != nil {
				return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
			}
			inv.State = tr.State
			inv.Status = InvocationWaiting
			inv.CallID = newID(s, "c")
			inv.Continuation = tr.Continuation
			inv.Next = []string{target}
			return false, nil
		}
		if err := r.checkTargets(source, tr.Targets); err != nil {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		if err := r.checkJoinTargets(s, inv, tr.Targets); err != nil {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		inv.State = tr.State
		inv.Status = InvocationWaiting
		inv.CallID = newID(s, "c")
		inv.Continuation = tr.Continuation
		inv.Next = append([]string(nil), tr.Targets...)
		return false, nil
	case ActionFanOut:
		if err := r.fanOut(s, index, inv, source, tr.State, tr.Branches, budget); err != nil {
			if _, ok := errors.AsType[*stateCopyError](err); ok || errors.Is(err, ErrExecutionLimit) || errors.Is(err, errInvocationBudget) {
				return false, err
			}
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		return false, nil
	case ActionCompleteBranch:
		if inv.GroupID == "" {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: errors.New("CompleteBranch used outside an activation group")}
		}
		_, activation := group(s, inv.GroupID)
		if activation == nil || activation.JoinNode == "" {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: errors.New("CompleteBranch requires a group with a join")}
		}
		inv.State = tr.State
		inv.Status = InvocationJoined
		inv.CallID = ""
		inv.Node = activation.JoinNode
		inv.Next = nil
		inv.Continuation = ""
		return false, nil
	case ActionEndBranch:
		inv.Status = InvocationEnded
		inv.CallID = ""
		inv.Next = nil
		inv.Continuation = ""
		return false, nil
	case ActionEndExecution:
		state := tr.State
		s.Final = &state
		s.Completed = true
		s.Invocations = nil
		s.Groups = nil
		clearInvocationIndex(index)
		return true, nil
	case ActionReturn:
		n := r.node(source)
		if n == nil || !n.Mounted {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: errors.New("Return used outside a subgraph")}
		}
		target := n.ReturnTarget
		if target == "" {
			inv.State = tr.State
			inv.Status = InvocationEnded
			inv.CallID = ""
			inv.Next = nil
			inv.Continuation = ""
			return false, nil
		}
		edge := CompiledEdge{Target: target, NodeIndex: n.ReturnNodeIndex, JoinIndex: n.ReturnJoinIndex}
		if err := r.checkJoinEdge(s, inv, edge); err != nil {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		inv.State = tr.State
		inv.Continuation = ""
		inv.Next = nil
		if err := r.setTargetEdge(s, inv, edge); err != nil {
			if _, ok := errors.AsType[*stateCopyError](err); ok || errors.Is(err, ErrExecutionLimit) || errors.Is(err, errInvocationBudget) {
				return false, err
			}
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		return false, nil
	default:
		return false, &TransitionError{InvocationID: id, Node: source, Cause: fmt.Errorf("invalid action %d", tr.Action)}
	}
}

func (r *Runner[S]) checkJoinEdge(s *Checkpoint[S], inv *Invocation[S], edge CompiledEdge) error {
	if edge.JoinIndex < 0 {
		return nil
	}
	var expected string
	if inv.GroupID != "" {
		_, group := group(s, inv.GroupID)
		if group != nil {
			expected = group.JoinNode
		}
	}
	if edge.Target != expected {
		return fmt.Errorf("invocation %q reached join %q outside its activation group", inv.ID, edge.Target)
	}
	return nil
}

func (r *Runner[S]) checkJoinTargets(s *Checkpoint[S], inv *Invocation[S], targets []string) error {
	if len(r.joins) == 0 {
		return nil
	}
	for _, target := range targets {
		if r.join(target) == nil {
			continue
		}
		var expected string
		if len(targets) > 1 {
			if n := r.node(inv.Node); n != nil && n.JoinIndex >= 0 {
				expected = r.joins[n.JoinIndex].Spec.Name
			}
		} else if inv.GroupID != "" {
			_, group := group(s, inv.GroupID)
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

func (r *Runner[S]) route(s *Checkpoint[S], index *invocationIndex, id, source string, state S, targets []string, budget invocationBudget) error {
	_, inv := indexedInvocation(index, s, id)
	if inv == nil {
		return fmt.Errorf("invocation %q disappeared", id)
	}
	if len(targets) == 1 {
		inv.State = state
		inv.Continuation = ""
		inv.Next = nil

		return r.setTarget(s, inv, targets[0])
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
	if n := r.node(source); n != nil && n.JoinIndex >= 0 {
		joinNode = r.joins[n.JoinIndex].Spec.Name
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
	if s.NextID > math.MaxUint64-needed {
		return fmt.Errorf("reserve fan-out IDs: %w", ErrExecutionLimit)
	}
	reserveInvocationIndex(index, s, len(targets))
	groupID := newID(s, "g")
	group := ActivationGroup{
		ID:       groupID,
		Source:   source,
		ParentID: id,
		JoinNode: joinNode,
		Branches: make([]ActivationBranch, len(targets)),
	}
	if group.JoinNode != "" {
		group.CallID = newID(s, "c")
	}
	inv.State = state
	inv.Status = InvocationGroup
	inv.CallID = ""
	inv.ChildGroupID = groupID
	inv.Continuation = ""
	inv.Next = nil
	for branchIndex, target := range targets {
		childState, err := r.cloneState(state, id)
		if err != nil {
			return &stateCopyError{cause: fmt.Errorf("clone fan-out state: %w", err)}
		}
		child := Invocation[S]{ID: newID(s, "i"), State: childState, GroupID: groupID, BranchIndex: branchIndex}
		group.Branches[branchIndex] = ActivationBranch{InvocationID: child.ID, Target: target}
		appended := appendInvocation(index, s, child)
		if err := r.setTarget(s, appended, target); err != nil {
			return err
		}
	}
	s.Groups = append(s.Groups, group)
	return nil
}

// fanOut creates a dynamic activation from declared, independently cloned tasks.
func (r *Runner[S]) fanOut(s *Checkpoint[S], index *invocationIndex, parent *Invocation[S], source string, parentState S, branches []Branch[S], budget invocationBudget) error {
	joinNode := ""
	sourceNode := r.node(source)
	if sourceNode != nil && sourceNode.JoinIndex >= 0 {
		joinNode = r.joins[sourceNode.JoinIndex].Spec.Name
	}
	if joinNode == "" {
		return errors.New("FanOut requires a JoinSpec for its source node")
	}
	for branchIndex, branch := range branches {
		if branch.EntryTimeout < 0 {
			return fmt.Errorf("branch %d timeout must not be negative", branchIndex)
		}
		foundEdge := false
		if sourceNode != nil {
			for _, edgeTarget := range r.edgeTargets[sourceNode.EdgeStart:sourceNode.EdgeEnd] {
				if edgeTarget == branch.Target {
					foundEdge = true
					break
				}
			}
		}
		if !foundEdge {
			return fmt.Errorf("node %q selected undeclared branch target %q", source, branch.Target)
		}
		if r.node(branch.Target) == nil {
			return fmt.Errorf("branch target %q is not an executable node", branch.Target)
		}
	}
	if err := budget.check(len(branches)); err != nil {
		return err
	}
	if uint64(len(branches)) > (math.MaxUint64-2)/2 {
		return fmt.Errorf("reserve FanOut IDs: %w", ErrExecutionLimit)
	}
	needed := uint64(2) + 2*uint64(len(branches)) // group/join, invocation/callback per child
	if s.NextID > math.MaxUint64-needed {
		return fmt.Errorf("reserve FanOut IDs: %w", ErrExecutionLimit)
	}
	reserveInvocationIndex(index, s, len(branches))
	groupID := newID(s, "g")
	activation := ActivationGroup{
		ID:       groupID,
		CallID:   newID(s, "c"),
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
			ID:          newID(s, "i"),
			State:       state,
			Status:      InvocationReady,
			GroupID:     groupID,
			BranchIndex: branchIndex,
		}
		activation.Branches[branchIndex] = ActivationBranch{InvocationID: child.ID, Target: branch.Target, EntryTimeout: branch.EntryTimeout}
		childPosition := appendInvocation(index, s, child)
		if err := r.setTarget(s, childPosition, branch.Target); err != nil {
			return err
		}
	}
	s.Groups = append(s.Groups, activation)
	return nil
}

func (r *Runner[S]) setTargetEdge(s *Checkpoint[S], inv *Invocation[S], edge CompiledEdge) error {
	if edge.JoinIndex >= 0 {
		if inv.GroupID == "" {
			return fmt.Errorf("invocation %q reached join %q outside a group", inv.ID, edge.Target)
		}
		_, group := group(s, inv.GroupID)
		if group == nil {
			return fmt.Errorf("invocation %q has missing group %q", inv.ID, inv.GroupID)
		}
		if group.JoinNode != edge.Target {
			return fmt.Errorf("invocation %q reached join %q for group %q", inv.ID, edge.Target, group.ID)
		}
		inv.Status = InvocationJoined
		inv.CallID = ""
	} else {
		if edge.NodeIndex < 0 {
			return fmt.Errorf("unknown node %q", edge.Target)
		}
		if s.NextID == math.MaxUint64 {
			return fmt.Errorf("allocate node call ID: %w", ErrExecutionLimit)
		}
		inv.Status = InvocationReady
		inv.CallID = newID(s, "c")
	}
	inv.Node = edge.Target
	return nil
}

func (r *Runner[S]) setTarget(s *Checkpoint[S], inv *Invocation[S], target string) error {
	nIdx := -1
	if idx, ok := r.nodeIndex[target]; ok {
		nIdx = idx
	}
	jIdx := -1
	if idx, ok := r.joinIndex[target]; ok {
		jIdx = idx
	}
	return r.setTargetEdge(s, inv, CompiledEdge{Target: target, NodeIndex: nIdx, JoinIndex: jIdx})
}
