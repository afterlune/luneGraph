package executor

import (
	"fmt"
	"slices"
)

// recordFailure changes a candidate position. An execution-level error
// leaves a terminal candidate for the caller to commit before returning.
func (r *Runner[S]) recordFailure(pos *candidatePosition[S], index *invocationIndex, id, node string, scope FailureScope, cause error) error {
	_, inv := indexedInvocation(index, pos, id)
	if inv == nil {
		return fmt.Errorf("missing failed invocation %q", id)
	}
	if scope == FailGroup && inv.GroupID == "" {
		scope = FailExecution
	}
	if scope == FailExecution {
		pos.Failure = &Failure{InvocationID: id, Node: node, Message: cause.Error(), PanicStack: panicStack(cause)}
		pos.Completed = true
		pos.Final = nil
		pos.Invocations = nil
		pos.Groups = nil
		clearInvocationIndex(index)
		return fmt.Errorf("%s: %w", node, cause)
	}
	pos.HadLocalFailures = true
	if scope == FailGroup && inv.GroupID != "" {
		return r.failGroup(pos, index, inv.GroupID, cause.Error())
	}
	return markInvocationFailed(pos, inv, cause.Error())
}

func markInvocationFailed[S any](pos *candidatePosition[S], inv *Invocation[S], message string) error {
	branch, activation, err := activationBranch(pos, inv)
	if err != nil {
		return err
	}
	if branch != nil && branch.Error != message {
		activation.Branches = slices.Clone(activation.Branches)
		activation.Branches[inv.BranchIndex].Error = message
	}
	inv.Status = InvocationFailed
	inv.CallID = ""
	inv.Next = nil
	inv.Continuation = ""
	return nil
}

func (r *Runner[S]) failGroup(pos *candidatePosition[S], index *invocationIndex, id, message string) error {
	activation := findActivationGroup(pos.Groups, id)
	if activation == nil {
		return fmt.Errorf("missing failed group %q", id)
	}
	parentID := activation.ParentID
	removeInv := make(map[string]bool)
	removeGroups := make(map[string]bool)
	var walk func(string)
	walk = func(groupID string) {
		if removeGroups[groupID] {
			return
		}
		current := findActivationGroup(pos.Groups, groupID)
		if current == nil {
			return
		}
		removeGroups[groupID] = true
		for _, branch := range current.Branches {
			childID := branch.InvocationID
			removeInv[childID] = true
			_, child := indexedInvocation(index, pos, childID)
			if child != nil && child.ChildGroupID != "" {
				walk(child.ChildGroupID)
			}
		}
	}
	walk(id)
	removeInvocations(index, pos, removeInv)
	groups := pos.Groups[:0]
	for _, group := range pos.Groups {
		if !removeGroups[group.ID] {
			groups = append(groups, group)
		}
	}
	clear(pos.Groups[len(groups):])
	pos.Groups = groups
	_, parent := indexedInvocation(index, pos, parentID)
	if parent == nil {
		return fmt.Errorf("failed group %q lost parent", id)
	}
	parent.ChildGroupID = ""
	return markInvocationFailed(pos, parent, message)
}

func recoveredFailure[S any](cp Checkpoint[S]) error {
	failure := cp.Failure
	if failure == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %s", ErrRunFailed, failure.Node, failure.Message)
}
