package executor

import (
	"context"
	"errors"
	"fmt"
	"math"
)

func (r *Runner[S]) applyTransition(s *Checkpoint[S], id string, tr Transition[S]) (bool, error) {
	_, inv := invocation(s, id)
	if inv == nil {
		return false, &TransitionError{InvocationID: id, Cause: fmt.Errorf("invocation disappeared")}
	}
	source := inv.Node
	switch tr.Action {
	case ActionContinue, ActionWait:
		if err := r.checkTargets(source, tr.Targets); err != nil {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		if err := r.checkJoinTargets(s, inv, tr.Targets); err != nil {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		if tr.Action == ActionContinue && tr.Continuation != "" {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: errors.New("continuation supplied with Continue")}
		}
		if tr.Action == ActionWait {
			if _, ok := r.continuations[tr.Continuation]; !ok {
				return false, &TransitionError{InvocationID: id, Node: source, Cause: fmt.Errorf("unknown continuation %q", tr.Continuation)}
			}
			if s.NextID == math.MaxUint64 {
				return false, fmt.Errorf("allocate continuation call ID: %w", ErrExecutionLimit)
			}
			inv.State = tr.State
			inv.Status = InvocationWaiting
			inv.CallID = newID(s, "c")
			inv.Continuation = tr.Continuation
			inv.Next = append([]string(nil), tr.Targets...)
			return false, nil
		}
		if err := r.route(s, id, source, tr.State, tr.Targets); err != nil {
			var cloneErr *stateCopyError
			if errors.As(err, &cloneErr) || errors.Is(err, ErrExecutionLimit) {
				return false, err
			}
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		return false, nil
	case ActionEndBranch:
		if len(tr.Targets) != 0 || tr.Continuation != "" {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: errors.New("routing supplied with EndBranch")}
		}
		inv.State = tr.State
		inv.Status = InvocationEnded
		inv.CallID = ""
		inv.Next = nil
		inv.Continuation = ""
		s.Terminals = append(s.Terminals, Terminal[S]{InvocationID: id, State: tr.State})
		return false, nil
	case ActionEndExecution:
		if len(tr.Targets) != 0 || tr.Continuation != "" {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: errors.New("routing supplied with EndExecution")}
		}
		state := tr.State
		s.Final = &state
		s.Completed = true
		s.Invocations = nil
		s.Groups = nil
		return true, nil
	case ActionReturn:
		if len(tr.Targets) != 0 || tr.Continuation != "" {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: errors.New("routing supplied with Return")}
		}
		target, mounted := r.returnTargets[source]
		if !mounted {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: errors.New("Return used outside a subgraph")}
		}
		if target == "" {
			inv.State = tr.State
			inv.Status = InvocationEnded
			inv.CallID = ""
			inv.Next = nil
			inv.Continuation = ""
			s.Terminals = append(s.Terminals, Terminal[S]{InvocationID: id, State: tr.State})
			return false, nil
		}
		targets := []string{target}
		if err := r.checkJoinTargets(s, inv, targets); err != nil {
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		if err := r.route(s, id, source, tr.State, targets); err != nil {
			var cloneErr *stateCopyError
			if errors.As(err, &cloneErr) || errors.Is(err, ErrExecutionLimit) {
				return false, err
			}
			return false, &TransitionError{InvocationID: id, Node: source, Cause: err}
		}
		return false, nil
	default:
		return false, &TransitionError{InvocationID: id, Node: source, Cause: fmt.Errorf("invalid action %d", tr.Action)}
	}
}

func (r *Runner[S]) checkJoinTargets(s *Checkpoint[S], inv *Invocation[S], targets []string) error {
	for _, target := range targets {
		if _, isJoin := r.joins[target]; !isJoin {
			continue
		}
		var expected string
		if len(targets) > 1 {
			expected = r.joinBySource[inv.Node]
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

func (r *Runner[S]) route(s *Checkpoint[S], id, source string, state S, targets []string) error {
	_, inv := invocation(s, id)
	if inv == nil {
		return fmt.Errorf("invocation %q disappeared", id)
	}
	if len(targets) == 1 {
		inv.State = state
		inv.Continuation = ""
		inv.Next = nil
		return r.setTarget(s, inv, targets[0])
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
	if r.joinBySource[source] != "" {
		if !addIDs(1) { // join callback call ID
			return fmt.Errorf("reserve fan-out IDs: %w", ErrExecutionLimit)
		}
	}
	for _, target := range targets {
		if _, join := r.joins[target]; !join {
			if !addIDs(1) { // child node callback call ID
				return fmt.Errorf("reserve fan-out IDs: %w", ErrExecutionLimit)
			}
		}
	}
	if s.NextID > math.MaxUint64-needed {
		return fmt.Errorf("reserve fan-out IDs: %w", ErrExecutionLimit)
	}
	groupID := newID(s, "g")
	group := ActivationGroup{ID: groupID, Source: source, ParentID: id, JoinNode: r.joinBySource[source]}
	if group.JoinNode != "" {
		group.CallID = newID(s, "c")
	}
	inv.State = state
	inv.Status = InvocationGroup
	inv.CallID = ""
	inv.ChildGroupID = groupID
	inv.Continuation = ""
	inv.Next = nil
	s.Groups = append(s.Groups, group)
	for index, target := range targets {
		childState, err := r.cloneState(state, id)
		if err != nil {
			return &stateCopyError{cause: fmt.Errorf("clone fan-out state: %w", err)}
		}
		child := Invocation[S]{ID: newID(s, "i"), State: childState, GroupID: groupID, BranchIndex: index}
		s.Groups[len(s.Groups)-1].Children = append(s.Groups[len(s.Groups)-1].Children, child.ID)
		s.Invocations = append(s.Invocations, child)
		_, appended := invocation(s, child.ID)
		if err := r.setTarget(s, appended, target); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner[S]) setTarget(s *Checkpoint[S], inv *Invocation[S], target string) error {
	if _, join := r.joins[target]; join {
		if inv.GroupID == "" {
			return fmt.Errorf("invocation %q reached join %q outside a group", inv.ID, target)
		}
		_, group := group(s, inv.GroupID)
		if group == nil {
			return fmt.Errorf("invocation %q has missing group %q", inv.ID, inv.GroupID)
		}
		if group.JoinNode != target {
			return fmt.Errorf("invocation %q reached join %q for group %q", inv.ID, target, group.ID)
		}
		inv.Status = InvocationJoined
		inv.CallID = ""
	} else {
		if _, ok := r.nodes[target]; !ok {
			return fmt.Errorf("unknown node %q", target)
		}
		if s.NextID == math.MaxUint64 {
			return fmt.Errorf("allocate node call ID: %w", ErrExecutionLimit)
		}
		inv.Status = InvocationReady
		inv.CallID = newID(s, "c")
	}
	inv.Node = target
	return nil
}

func (r *Runner[S]) settleGroups(ctx context.Context, s *Checkpoint[S], override *FailureScope) error {
	for {
		index := -1
		for i, group := range s.Groups {
			complete := true
			for _, childID := range group.Children {
				_, child := invocation(s, childID)
				if child == nil || (child.Status != InvocationJoined && child.Status != InvocationEnded && child.Status != InvocationFailed) {
					complete = false
					break
				}
			}
			if complete {
				index = i
				break
			}
		}
		if index < 0 {
			return nil
		}
		group := s.Groups[index]
		_, parent := invocation(s, group.ParentID)
		if parent == nil {
			return fmt.Errorf("group %q has missing parent", group.ID)
		}
		var merged S
		var mergeErr error
		if group.JoinNode != "" {
			values := make([]S, 0, len(group.Children))
			for _, childID := range group.Children {
				_, child := invocation(s, childID)
				if child.Status == InvocationJoined {
					value, err := r.cloneState(child.State, childID)
					if err != nil {
						return fmt.Errorf("clone join input: %w", err)
					}
					values = append(values, value)
				}
			}
			call := CallInfo{RunID: s.RunID, InvocationID: parent.ID, CallID: group.CallID}
			merged, mergeErr = r.mergeStates(ctx, call, r.joins[group.JoinNode], values)
		}
		children := make(map[string]bool, len(group.Children))
		for _, childID := range group.Children {
			children[childID] = true
		}
		remaining := s.Invocations[:0]
		for _, inv := range s.Invocations {
			if !children[inv.ID] {
				remaining = append(remaining, inv)
			}
		}
		s.Invocations = remaining
		s.Groups = append(s.Groups[:index], s.Groups[index+1:]...)
		_, parent = invocation(s, group.ParentID)
		parent.ChildGroupID = ""
		if mergeErr != nil {
			scope := r.joins[group.JoinNode].OnError
			if override != nil {
				scope = *override
			}
			if err := r.recordFailure(s, parent.ID, group.JoinNode, scope, mergeErr); err != nil {
				return err
			}
			continue
		}
		if group.JoinNode == "" {
			parent.Status = InvocationEnded
			continue
		}
		parent.State = merged
		var next string
		for target := range r.edges[group.JoinNode] {
			next = target
		}
		if next == "" {
			parent.Status = InvocationEnded
			s.Terminals = append(s.Terminals, Terminal[S]{InvocationID: parent.ID, State: merged})
		} else if err := r.setTarget(s, parent, next); err != nil {
			return &TransitionError{InvocationID: parent.ID, Node: group.JoinNode, Cause: err}
		}
	}
}
