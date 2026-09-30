package executor

import (
	"context"
	"fmt"

	"github.com/afterlune/luneGraph/internal/observation"
)

func (r *Runner[S]) settleGroups(ctx context.Context, s *Checkpoint[S], invIndex *invocationIndex, progress *groupProgress, override *FailureScope, obs *observation.Session) error {
	for {
		index := readyGroup(s, invIndex, progress)
		if index < 0 {
			return nil
		}
		group := s.Groups[index]
		_, parent := indexedInvocation(invIndex, s, group.ParentID)
		if parent == nil {
			return fmt.Errorf("group %q has missing parent", group.ID)
		}
		var merged S
		var mergeErr error
		if group.JoinNode != "" {
			values := make([]S, 0, len(group.Children))
			for _, childID := range group.Children {
				_, child := indexedInvocation(invIndex, s, childID)
				if child.Status == InvocationJoined {
					value, err := r.cloneState(child.State, childID)
					if err != nil {
						return fmt.Errorf("clone join input: %w", err)
					}
					values = append(values, value)
				}
			}
			call := CallInfo{RunID: s.RunID, InvocationID: parent.ID, CallID: group.CallID}
			merged, mergeErr = r.observedMerge(ctx, obs, s.Revision, call, r.joins[group.JoinNode], values)
		}
		children := make(map[string]bool, len(group.Children))
		for _, childID := range group.Children {
			children[childID] = true
		}
		removeInvocations(invIndex, s, children)
		s.Groups = append(s.Groups[:index], s.Groups[index+1:]...)
		_, parent = indexedInvocation(invIndex, s, group.ParentID)
		parent.ChildGroupID = ""
		if mergeErr != nil {
			scope := r.joins[group.JoinNode].OnError
			if override != nil {
				scope = *override
			}
			if err := r.recordFailure(s, invIndex, parent.ID, group.JoinNode, scope, mergeErr); err != nil {
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
