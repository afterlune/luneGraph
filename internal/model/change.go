package model

import (
	"math"
	"slices"
)

// Header is the complete metadata of a committed execution position.
type Header struct {
	FormatVersion                           uint32
	RunID                                   string
	DefinitionID                            string `json:"MachineID"`
	Revision, Steps, NextID, ScheduleCursor uint64
	Completed, HadLocalFailures             bool
	Failure                                 *Failure
}

// Clone returns a copy of h with an independent Failure record.
func (h Header) Clone() Header {
	h.Failure = copyFailure(h.Failure)
	return h
}

// Change atomically updates only listed records. Final and Header are
// authoritative; a completed Header removes all active records.
type Change[S any] struct {
	Header            Header
	Final             *S
	UpsertInvocations []Invocation[S]
	DeleteInvocations []string
	UpsertGroups      []ActivationGroup
	DeleteGroups      []string
}

func (cp Checkpoint[S]) Header() Header {
	return Header{cp.FormatVersion, cp.RunID, cp.DefinitionID, cp.Revision, cp.Steps, cp.NextID, cp.ScheduleCursor, cp.Completed, cp.HadLocalFailures, copyFailure(cp.Failure)}
}

func copyFailure(f *Failure) *Failure {
	if f == nil {
		return nil
	}
	return new(*f)
}

// Change returns upserts for all records in cp. Omitted records are not deleted,
// except when cp is terminal. Executors submit only changed records.
func (cp Checkpoint[S]) Change() Change[S] {
	inv := slices.Clone(cp.Invocations)
	for i := range inv {
		inv[i].Next = slices.Clone(cp.Invocations[i].Next)
	}
	groups := slices.Clone(cp.Groups)
	for i := range groups {
		groups[i].Branches = slices.Clone(cp.Groups[i].Branches)
	}
	var final *S
	if cp.Final != nil {
		final = new(*cp.Final)
	}
	return Change[S]{Header: cp.Header(), Final: final, UpsertInvocations: inv, UpsertGroups: groups}
}

// Checkpoint materializes the records in this change, not the full execution.
func (c Change[S]) Checkpoint() Checkpoint[S] {
	h := c.Header
	var final *S
	if c.Final != nil {
		final = new(*c.Final)
	}
	return Copy(Checkpoint[S]{FormatVersion: h.FormatVersion, RunID: h.RunID, DefinitionID: h.DefinitionID, Revision: h.Revision, Steps: h.Steps, NextID: h.NextID, ScheduleCursor: h.ScheduleCursor, Completed: h.Completed, HadLocalFailures: h.HadLocalFailures, Failure: copyFailure(h.Failure), Final: final, Invocations: c.UpsertInvocations, Groups: c.UpsertGroups})
}

func (c Change[S]) Clone(clone Clone[S]) (Change[S], error) {
	cp, err := c.Checkpoint().Clone(clone)
	if err != nil {
		return Change[S]{}, err
	}
	out := cp.Change()
	out.DeleteInvocations = slices.Clone(c.DeleteInvocations)
	out.DeleteGroups = slices.Clone(c.DeleteGroups)
	return out, nil
}

// ValidateChange checks a submission without scanning unchanged records.
func ValidateChange[S any](expected uint64, c Change[S]) error {
	if expected == math.MaxUint64 {
		return ErrExecutionLimit
	}
	if err := ValidateCheckpointHeader(c.Checkpoint()); err != nil {
		return err
	}
	if c.Header.Revision != expected+1 {
		return ErrConflict
	}
	if (!c.Header.Completed && (c.Final != nil || c.Header.Failure != nil)) || (c.Final != nil && c.Header.Failure != nil) {
		return ErrInvalidCheckpoint
	}
	if c.Header.Completed && (len(c.UpsertInvocations) > 0 || len(c.UpsertGroups) > 0) {
		return ErrInvalidCheckpoint
	}
	inv := make(map[string]bool, len(c.UpsertInvocations)+len(c.DeleteInvocations))
	for _, v := range c.UpsertInvocations {
		if !ValidName(v.ID) || inv[v.ID] {
			return ErrInvalidCheckpoint
		}
		inv[v.ID] = true
	}
	for _, id := range c.DeleteInvocations {
		if !ValidName(id) || inv[id] {
			return ErrInvalidCheckpoint
		}
		inv[id] = true
	}
	groups := make(map[string]bool, len(c.UpsertGroups)+len(c.DeleteGroups))
	for _, v := range c.UpsertGroups {
		if !ValidName(v.ID) || groups[v.ID] {
			return ErrInvalidCheckpoint
		}
		groups[v.ID] = true
	}
	for _, id := range c.DeleteGroups {
		if !ValidName(id) || groups[id] {
			return ErrInvalidCheckpoint
		}
		groups[id] = true
	}
	return nil
}

func ValidateAdvance(before Header, after Header) error {
	if before.RunID != after.RunID || before.DefinitionID != after.DefinitionID || before.Revision+1 != after.Revision {
		return ErrConflict
	}
	if before.Completed {
		return ErrRunCompleted
	}
	if after.Steps < before.Steps || after.NextID < before.NextID || (before.HadLocalFailures && !after.HadLocalFailures) {
		return ErrInvalidCheckpoint
	}
	return nil
}
