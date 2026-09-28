package executor

import (
	"context"
	"fmt"
)

// recordFailure changes a candidate checkpoint. An execution-level error
// leaves a terminal candidate for the caller to commit before returning.
func (r *Runner[S]) recordFailure(s *Checkpoint[S], id, node string, scope FailureScope, cause error) error {
	_, inv := invocation(s, id)
	if inv == nil {
		return fmt.Errorf("missing failed invocation %q", id)
	}
	if scope == FailGroup && inv.GroupID == "" {
		scope = FailExecution
	}
	s.Failures = append(s.Failures, Failure{InvocationID: id, Node: node, Scope: scope, Message: cause.Error(), PanicStack: panicStack(cause)})
	if scope == FailExecution {
		s.Completed = true
		s.Final = nil
		s.Invocations = nil
		s.Groups = nil
		return fmt.Errorf("%s: %w", node, cause)
	}
	if scope == FailGroup && inv.GroupID != "" {
		return r.failGroup(s, inv.GroupID)
	}
	inv.Status = InvocationFailed
	inv.CallID = ""
	inv.Next = nil
	inv.Continuation = ""
	return nil
}

func (r *Runner[S]) failGroup(s *Checkpoint[S], id string) error {
	_, activation := group(s, id)
	if activation == nil {
		return fmt.Errorf("missing failed group %q", id)
	}
	parentID := activation.ParentID
	removeInv := make(map[string]bool)
	removeGroups := make(map[string]bool)
	var walk func(string)
	walk = func(groupID string) {
		_, current := group(s, groupID)
		if current == nil || removeGroups[groupID] {
			return
		}
		removeGroups[groupID] = true
		for _, childID := range current.Children {
			removeInv[childID] = true
			_, child := invocation(s, childID)
			if child != nil && child.ChildGroupID != "" {
				walk(child.ChildGroupID)
			}
		}
	}
	walk(id)
	invocations := s.Invocations[:0]
	for _, inv := range s.Invocations {
		if !removeInv[inv.ID] {
			invocations = append(invocations, inv)
		}
	}
	s.Invocations = invocations
	groups := s.Groups[:0]
	for _, group := range s.Groups {
		if !removeGroups[group.ID] {
			groups = append(groups, group)
		}
	}
	s.Groups = groups
	_, parent := invocation(s, parentID)
	if parent == nil {
		return fmt.Errorf("failed group %q lost parent", id)
	}
	parent.ChildGroupID = ""
	parent.Status = InvocationFailed
	parent.CallID = ""
	return nil
}

func terminalFailureRecord[S any](s Checkpoint[S]) *Failure {
	for i := range s.Failures {
		if s.Failures[i].Scope == FailExecution {
			return &s.Failures[i]
		}
	}
	return nil
}

func (r *Runner[S]) commitTerminalFailure(ctx context.Context, before, candidate Checkpoint[S], cause error, store Store[S]) (Result[S], error) {
	committed, err := r.commit(ctx, before, candidate, store)
	if err != nil {
		return resultWith(committed, errorStatus(err)), err
	}
	return resultWith(committed, StatusFailed), cause
}

func recoveredFailure[S any](s Checkpoint[S]) error {
	failure := terminalFailureRecord(s)
	if failure == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %s", ErrRunFailed, failure.Node, failure.Message)
}
