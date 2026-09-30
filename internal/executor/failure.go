package executor

import (
	"context"
	"fmt"
)

// recordFailure changes a candidate checkpoint. An execution-level error
// leaves a terminal candidate for the caller to commit before returning.
func (r *Runner[S]) recordFailure(s *Checkpoint[S], index *invocationIndex, id, node string, scope FailureScope, cause error) error {
	_, inv := indexedInvocation(index, s, id)
	if inv == nil {
		return fmt.Errorf("missing failed invocation %q", id)
	}
	if scope == FailGroup && inv.GroupID == "" {
		scope = FailExecution
	}
	if scope == FailExecution {
		s.Failure = &Failure{InvocationID: id, Node: node, Message: cause.Error(), PanicStack: panicStack(cause)}
		s.Completed = true
		s.Final = nil
		s.Invocations = nil
		s.Groups = nil
		clearInvocationIndex(index)
		return fmt.Errorf("%s: %w", node, cause)
	}
	s.HadLocalFailures = true
	if scope == FailGroup && inv.GroupID != "" {
		return r.failGroup(s, index, inv.GroupID)
	}
	inv.Status = InvocationFailed
	inv.CallID = ""
	inv.Next = nil
	inv.Continuation = ""
	return nil
}

func (r *Runner[S]) failGroup(s *Checkpoint[S], index *invocationIndex, id string) error {
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
			_, child := indexedInvocation(index, s, childID)
			if child != nil && child.ChildGroupID != "" {
				walk(child.ChildGroupID)
			}
		}
	}
	walk(id)
	removeInvocations(index, s, removeInv)
	groups := s.Groups[:0]
	for _, group := range s.Groups {
		if !removeGroups[group.ID] {
			groups = append(groups, group)
		}
	}
	clear(s.Groups[len(groups):])
	s.Groups = groups
	_, parent := indexedInvocation(index, s, parentID)
	if parent == nil {
		return fmt.Errorf("failed group %q lost parent", id)
	}
	parent.ChildGroupID = ""
	parent.Status = InvocationFailed
	parent.CallID = ""
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
	failure := s.Failure
	if failure == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %s", ErrRunFailed, failure.Node, failure.Message)
}
