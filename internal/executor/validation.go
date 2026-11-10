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
	numStr := id[len(prefix):]
	if len(numStr) == 0 || numStr[0] < '1' || numStr[0] > '9' {
		return 0, false
	}
	if len(numStr) >= 20 {
		n, err := strconv.ParseUint(numStr, 10, 64)
		return n, err == nil
	}
	var n uint64
	for i := 0; i < len(numStr); i++ {
		c := numStr[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint64(c-'0')
	}
	return n, true
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
		node := r.node(s.Failure.Node) != nil
		join := r.join(s.Failure.Node) != nil
		if !s.Completed || s.Final != nil || !valid || n >= s.NextID || (!node && !join) {
			return invalidCheckpoint("invalid execution failure")
		}
	}
	invIndices := make(map[string]int, len(s.Invocations))
	groupIndices := make(map[string]int, len(s.Groups))
	var maxID uint64
	active := false
	callIDs := make(map[string]struct{})
	for i, inv := range s.Invocations {
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
		if _, exists := invIndices[inv.ID]; exists {
			return invalidCheckpoint("duplicate invocation ID %q", inv.ID)
		}
		invIndices[inv.ID] = i
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
			if r.node(inv.Node) == nil {
				return invalidCheckpoint("invocation %q has unknown node %q", inv.ID, inv.Node)
			}
			if inv.Status == InvocationWaiting {
				if r.continuation(inv.Continuation) == nil {
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
			if r.node(inv.Node) == nil {
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
	for i, group := range s.Groups {
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
		if _, exists := groupIndices[group.ID]; exists {
			return invalidCheckpoint("duplicate group ID %q", group.ID)
		}
		groupIndices[group.ID] = i
		parentIdx, exists := invIndices[group.ParentID]
		if !exists {
			return invalidCheckpoint("group %q has invalid parent", group.ID)
		}
		parent := s.Invocations[parentIdx]
		if parent.Status != InvocationGroup || parent.ChildGroupID != group.ID || parent.Node != group.Source {
			return invalidCheckpoint("group %q has invalid parent", group.ID)
		}
		sourceNode := r.node(group.Source)
		var expectedJoin string
		if sourceNode != nil && sourceNode.JoinIndex >= 0 {
			expectedJoin = r.joins[sourceNode.JoinIndex].Spec.Name
		}
		if group.JoinNode != expectedJoin {
			return invalidCheckpoint("group %q has incompatible join", group.ID)
		}
		numEdges := 0
		if sourceNode != nil {
			numEdges = sourceNode.EdgeEnd - sourceNode.EdgeStart
		}
		if group.JoinNode == "" && (len(group.Branches) < 2 || len(group.Branches) > numEdges) {
			return invalidCheckpoint("group %q has invalid unjoined child count", group.ID)
		}
		for index, branch := range group.Branches {
			childID := branch.InvocationID
			childIdx, exists := invIndices[childID]
			if !exists {
				return invalidCheckpoint("group %q has invalid child %q", group.ID, childID)
			}
			child := s.Invocations[childIdx]
			if child.GroupID != group.ID || child.BranchIndex != index || childID == group.ParentID {
				return invalidCheckpoint("group %q has invalid child %q", group.ID, childID)
			}
			hasEdge := false
			if sourceNode != nil {
				for _, edgeTarget := range r.edgeTargets[sourceNode.EdgeStart:sourceNode.EdgeEnd] {
					if edgeTarget == branch.Target {
						hasEdge = true
						break
					}
				}
			}
			if !hasEdge || branch.EntryTimeout < 0 {
				return invalidCheckpoint("group %q has invalid branch metadata at index %d", group.ID, index)
			}
			if branch.EntryTimeout > 0 && child.Status != InvocationFailed && (child.Status != InvocationReady || child.Node != branch.Target) {
				return invalidCheckpoint("group %q retains an entry timeout after branch %d advanced", group.ID, index)
			}
			if branch.Error != "" && child.Status != InvocationFailed {
				return invalidCheckpoint("group %q has an error for non-failed branch %d", group.ID, index)
			}
			if child.Status == InvocationJoined && (group.JoinNode == "" || child.Node != group.JoinNode) {
				return invalidCheckpoint("group %q has child at wrong join", group.ID)
			}
		}
		resolved := true
		for _, branch := range group.Branches {
			childID := branch.InvocationID
			status := s.Invocations[invIndices[childID]].Status
			if status != InvocationJoined && status != InvocationEnded && status != InvocationFailed {
				resolved = false
				break
			}
		}
		if resolved {
			active = true
		}
	}
	if maxID >= s.NextID {
		return invalidCheckpoint("next ID has already been used")
	}
	for _, inv := range s.Invocations {
		if inv.GroupID != "" {
			gIdx, exists := groupIndices[inv.GroupID]
			if !exists {
				return invalidCheckpoint("invocation %q has invalid group membership", inv.ID)
			}
			group := s.Groups[gIdx]
			if inv.BranchIndex >= len(group.Branches) || group.Branches[inv.BranchIndex].InvocationID != inv.ID {
				return invalidCheckpoint("invocation %q has invalid group membership", inv.ID)
			}
		}
		if inv.Status == InvocationGroup {
			if _, exists := groupIndices[inv.ChildGroupID]; !exists {
				return invalidCheckpoint("invocation %q has missing child group", inv.ID)
			}
		}
		if inv.Status == InvocationJoined && inv.GroupID == "" {
			return invalidCheckpoint("joined invocation %q has no group", inv.ID)
		}
	}
	groupVisit := make([]uint8, len(s.Groups))
	for gIdx := range s.Groups {
		if groupVisit[gIdx] == 2 {
			continue
		}
		curr := gIdx
		for curr >= 0 {
			if groupVisit[curr] == 1 {
				return invalidCheckpoint("group %q has cyclic ancestry", s.Groups[curr].ID)
			}
			if groupVisit[curr] == 2 {
				break
			}
			groupVisit[curr] = 1
			pIdx, pExists := invIndices[s.Groups[curr].ParentID]
			if !pExists {
				return invalidCheckpoint("group %q has missing ancestor", s.Groups[curr].ID)
			}
			parentGroupID := s.Invocations[pIdx].GroupID
			if parentGroupID == "" {
				break
			}
			nextGIdx, gExists := groupIndices[parentGroupID]
			if !gExists {
				return invalidCheckpoint("group %q has missing ancestor", s.Groups[curr].ID)
			}
			curr = nextGIdx
		}
		curr = gIdx
		for curr >= 0 {
			if groupVisit[curr] == 2 {
				break
			}
			groupVisit[curr] = 2
			pIdx := invIndices[s.Groups[curr].ParentID]
			parentGroupID := s.Invocations[pIdx].GroupID
			if parentGroupID == "" {
				break
			}
			curr = groupIndices[parentGroupID]
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
