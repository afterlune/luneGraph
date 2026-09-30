package executor

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func invalidCheckpoint(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidCheckpoint, fmt.Sprintf(format, args...))
}

func checkpointID(id, prefix string) (uint64, bool) {
	if !strings.HasPrefix(id, prefix) {
		return 0, false
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(id, prefix), 10, 64)
	return n, err == nil && n > 0 && fmt.Sprintf("%s%d", prefix, n) == id
}

func (r *Runner[S]) validateCheckpoint(s Checkpoint[S]) error {
	if r == nil {
		return errors.New("runner is nil")
	}
	if s.FormatVersion != CheckpointFormatVersion {
		return invalidCheckpoint("unsupported format version %d", s.FormatVersion)
	}
	if !validName(s.RunID) || s.MachineID != r.id || s.Revision == 0 || (!s.Completed && s.Final != nil) || s.NextID < 3 || s.ScheduleCursor >= s.NextID {
		return invalidCheckpoint("invalid execution header")
	}
	if s.Failure != nil {
		n, valid := checkpointID(s.Failure.InvocationID, "i")
		_, node := r.nodes[s.Failure.Node]
		_, join := r.joins[s.Failure.Node]
		if !s.Completed || s.Final != nil || !valid || n >= s.NextID || (!node && !join) {
			return invalidCheckpoint("invalid execution failure")
		}
	}
	invocations := make(map[string]Invocation[S], len(s.Invocations))
	groups := make(map[string]ActivationGroup, len(s.Groups))
	var maxID uint64
	active := false
	callIDs := make(map[string]struct{})
	for _, inv := range s.Invocations {
		n, valid := checkpointID(inv.ID, "i")
		if !valid || n >= s.NextID {
			return invalidCheckpoint("invalid invocation ID %q", inv.ID)
		}
		if n > maxID {
			maxID = n
		}
		if inv.CallID != "" {
			callID, valid := checkpointID(inv.CallID, "c")
			if !valid || callID >= s.NextID {
				return invalidCheckpoint("invalid callback ID %q for invocation %q", inv.CallID, inv.ID)
			}
			if _, exists := callIDs[inv.CallID]; exists {
				return invalidCheckpoint("duplicate callback ID %q", inv.CallID)
			}
			callIDs[inv.CallID] = struct{}{}
			if callID > maxID {
				maxID = callID
			}
		}
		if _, exists := invocations[inv.ID]; exists {
			return invalidCheckpoint("duplicate invocation ID %q", inv.ID)
		}
		invocations[inv.ID] = inv
		if inv.BranchIndex < 0 || (inv.GroupID == "" && inv.BranchIndex != 0) {
			return invalidCheckpoint("invocation %q has invalid branch index", inv.ID)
		}
		if inv.Status != InvocationWaiting && (inv.Continuation != "" || len(inv.Next) != 0) {
			return invalidCheckpoint("invocation %q has stray continuation", inv.ID)
		}
		if inv.Status != InvocationGroup && inv.ChildGroupID != "" {
			return invalidCheckpoint("invocation %q has stray child group", inv.ID)
		}
		switch inv.Status {
		case InvocationReady, InvocationWaiting:
			active = true
			if inv.CallID == "" {
				return invalidCheckpoint("active invocation %q has no callback ID", inv.ID)
			}
			if _, ok := r.nodes[inv.Node]; !ok {
				return invalidCheckpoint("invocation %q has unknown node %q", inv.ID, inv.Node)
			}
			if inv.Status == InvocationWaiting {
				if _, ok := r.continuations[inv.Continuation]; !ok {
					return invalidCheckpoint("invocation %q has unknown continuation %q", inv.ID, inv.Continuation)
				}
				if err := r.checkTargets(inv.Node, inv.Next); err != nil {
					return invalidCheckpoint("invocation %q: %v", inv.ID, err)
				}
				if err := r.checkJoinTargets(&s, &inv, inv.Next); err != nil {
					return invalidCheckpoint("invocation %q: %v", inv.ID, err)
				}
			}
		case InvocationGroup:
			if inv.CallID != "" {
				return invalidCheckpoint("group parent %q has a callback ID", inv.ID)
			}
			if _, ok := r.nodes[inv.Node]; !ok {
				return invalidCheckpoint("group parent %q has unknown node", inv.ID)
			}
		case InvocationJoined, InvocationEnded, InvocationFailed:
			if inv.CallID != "" {
				return invalidCheckpoint("inactive invocation %q has a callback ID", inv.ID)
			}
		default:
			return invalidCheckpoint("invocation %q has invalid status", inv.ID)
		}
	}
	for _, group := range s.Groups {
		n, valid := checkpointID(group.ID, "g")
		if !valid || n >= s.NextID {
			return invalidCheckpoint("invalid group ID %q", group.ID)
		}
		if n > maxID {
			maxID = n
		}
		if group.JoinNode != "" {
			callID, valid := checkpointID(group.CallID, "c")
			if !valid || callID >= s.NextID {
				return invalidCheckpoint("invalid callback ID %q for group %q", group.CallID, group.ID)
			}
			if _, exists := callIDs[group.CallID]; exists {
				return invalidCheckpoint("duplicate callback ID %q", group.CallID)
			}
			callIDs[group.CallID] = struct{}{}
			if callID > maxID {
				maxID = callID
			}
		} else if group.CallID != "" {
			return invalidCheckpoint("group %q has an unexpected callback ID", group.ID)
		}
		if _, exists := groups[group.ID]; exists {
			return invalidCheckpoint("duplicate group ID %q", group.ID)
		}
		groups[group.ID] = group
		parent, exists := invocations[group.ParentID]
		if !exists || parent.Status != InvocationGroup || parent.ChildGroupID != group.ID || parent.Node != group.Source {
			return invalidCheckpoint("group %q has invalid parent", group.ID)
		}
		if group.JoinNode != r.joinBySource[group.Source] {
			return invalidCheckpoint("group %q has incompatible join", group.ID)
		}
		if len(group.Children) < 2 || len(group.Children) > len(r.edges[group.Source]) {
			return invalidCheckpoint("group %q has invalid child count", group.ID)
		}
		for index, childID := range group.Children {
			child, exists := invocations[childID]
			if !exists || child.GroupID != group.ID || child.BranchIndex != index || childID == group.ParentID {
				return invalidCheckpoint("group %q has invalid child %q", group.ID, childID)
			}
			if child.Status == InvocationJoined && (group.JoinNode == "" || child.Node != group.JoinNode) {
				return invalidCheckpoint("group %q has child at wrong join", group.ID)
			}
		}
		resolved := true
		for _, childID := range group.Children {
			status := invocations[childID].Status
			if status != InvocationJoined && status != InvocationEnded && status != InvocationFailed {
				resolved = false
				break
			}
		}
		if resolved {
			return invalidCheckpoint("group %q should already be settled", group.ID)
		}
	}
	if maxID >= s.NextID {
		return invalidCheckpoint("next ID has already been used")
	}
	for _, inv := range s.Invocations {
		if inv.GroupID != "" {
			group, exists := groups[inv.GroupID]
			if !exists || inv.BranchIndex >= len(group.Children) || group.Children[inv.BranchIndex] != inv.ID {
				return invalidCheckpoint("invocation %q has invalid group membership", inv.ID)
			}
		}
		if inv.Status == InvocationGroup {
			if _, exists := groups[inv.ChildGroupID]; !exists {
				return invalidCheckpoint("invocation %q has missing child group", inv.ID)
			}
		}
		if inv.Status == InvocationJoined && inv.GroupID == "" {
			return invalidCheckpoint("joined invocation %q has no group", inv.ID)
		}
	}
	for _, group := range s.Groups {
		seen := map[string]bool{}
		for current := group.ID; current != ""; {
			if seen[current] {
				return invalidCheckpoint("group %q has cyclic ancestry", group.ID)
			}
			seen[current] = true
			parent := invocations[groups[current].ParentID]
			current = parent.GroupID
			if current != "" {
				if _, exists := groups[current]; !exists {
					return invalidCheckpoint("group %q has missing ancestor", group.ID)
				}
			}
		}
	}
	if s.Completed {
		if s.Steps == 0 || len(s.Groups) != 0 || len(s.Invocations) != 0 {
			return invalidCheckpoint("invalid completed execution")
		}
		return nil
	}
	if !active {
		return invalidCheckpoint("execution has no runnable or waiting invocation")
	}
	return nil
}
