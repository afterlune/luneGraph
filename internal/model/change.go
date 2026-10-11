package model

import (
	"math"
	"slices"
)

// Header is the complete metadata of a committed execution position.
type Header struct {
	FormatVersion                           uint32
	RunID, MachineID                        string
	Revision, Steps, NextID, ScheduleCursor uint64
	Completed, HadLocalFailures             bool
	Failure                                 *Failure
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

func (s Checkpoint[S]) Header() Header {
	return Header{s.FormatVersion, s.RunID, s.MachineID, s.Revision, s.Steps, s.NextID, s.ScheduleCursor, s.Completed, s.HadLocalFailures, copyFailure(s.Failure)}
}

func copyFailure(f *Failure) *Failure {
	if f == nil {
		return nil
	}
	c := *f
	return &c
}

// Change returns upserts for all records in s. Omitted records are not deleted,
// except when s is terminal. Executors submit only changed records.
func (s Checkpoint[S]) Change() Change[S] {
	inv := make([]Invocation[S], len(s.Invocations))
	copy(inv, s.Invocations)
	for i := range inv {
		inv[i].Next = slices.Clone(s.Invocations[i].Next)
	}
	groups := make([]ActivationGroup, len(s.Groups))
	copy(groups, s.Groups)
	for i := range groups {
		groups[i].Children = slices.Clone(s.Groups[i].Children)
	}
	var final *S
	if s.Final != nil {
		v := *s.Final
		final = &v
	}
	return Change[S]{Header: s.Header(), Final: final, UpsertInvocations: inv, UpsertGroups: groups}
}

// Checkpoint materializes the records in this change, not the full execution.
func (c Change[S]) Checkpoint() Checkpoint[S] {
	h := c.Header
	var final *S
	if c.Final != nil {
		v := *c.Final
		final = &v
	}
	return Copy(Checkpoint[S]{FormatVersion: h.FormatVersion, RunID: h.RunID, MachineID: h.MachineID, Revision: h.Revision, Steps: h.Steps, NextID: h.NextID, ScheduleCursor: h.ScheduleCursor, Completed: h.Completed, HadLocalFailures: h.HadLocalFailures, Failure: copyFailure(h.Failure), Final: final, Invocations: c.UpsertInvocations, Groups: c.UpsertGroups})
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
	if before.RunID != after.RunID || before.MachineID != after.MachineID || before.Revision+1 != after.Revision {
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
