package executor

import (
	"fmt"
	"slices"
)

// activationBranch borrows the current branch record. Mutations must first
// copy the activation's slice to preserve the accepted position's ownership.
func activationBranch[S any](s *Checkpoint[S], inv *Invocation[S]) (*ActivationBranch, *ActivationGroup, error) {
	if inv == nil {
		return nil, nil, fmt.Errorf("invocation disappeared")
	}
	if inv.GroupID == "" {
		return nil, nil, nil
	}
	_, activation := group(s, inv.GroupID)
	if activation == nil || inv.BranchIndex < 0 || inv.BranchIndex >= len(activation.Branches) || activation.Branches[inv.BranchIndex].InvocationID != inv.ID {
		return nil, nil, fmt.Errorf("invocation %q has invalid group membership", inv.ID)
	}
	return &activation.Branches[inv.BranchIndex], activation, nil
}

// clearEntryTimeout is applied only to a successful node outcome's candidate.
// If transition construction or submission fails, the accepted override survives.
func clearEntryTimeout[S any](s *Checkpoint[S], inv *Invocation[S]) error {
	branch, activation, err := activationBranch(s, inv)
	if err != nil {
		return err
	}
	if branch != nil && branch.EntryTimeout != 0 {
		activation.Branches = slices.Clone(activation.Branches)
		activation.Branches[inv.BranchIndex].EntryTimeout = 0
	}
	return nil
}
