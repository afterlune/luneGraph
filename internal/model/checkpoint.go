package model

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
)

// CheckpointFormatVersion is the format emitted and accepted by this runner.
const CheckpointFormatVersion uint32 = 3

// InvocationStatus describes one path's scheduling state.
type InvocationStatus string

const (
	InvocationReady   InvocationStatus = "ready"
	InvocationWaiting InvocationStatus = "waiting"
	InvocationGroup   InvocationStatus = "group"
	InvocationJoined  InvocationStatus = "joined"
	InvocationEnded   InvocationStatus = "ended"
	InvocationFailed  InvocationStatus = "failed"
)

// Invocation is a resumable path. Node is the next node when ready and the
// suspending node when waiting. CallID identifies its pending node or
// continuation callback.
type Invocation[S any] struct {
	ID           string
	CallID       string
	Node         string
	State        S
	Status       InvocationStatus
	GroupID      string
	ChildGroupID string
	BranchIndex  int
	Continuation string
	Next         []string
}

// ActivationGroup correlates branches started by one fan-out occurrence.
// CallID identifies the pending join callback when JoinNode is set.
type ActivationGroup struct {
	ID       string
	CallID   string
	Source   string
	ParentID string
	JoinNode string
	Children []string
}

// Failure records the execution-level failure of a completed run.
type Failure struct {
	InvocationID string
	Node         string
	Message      string
	PanicStack   string
}

// Checkpoint contains the complete execution position. Treat a returned
// checkpoint as immutable. Completed includes execution-level failures, marked
// by a non-nil Failure. Store implementations must own a deep
// copy of values passed to them and return an independent copy from Load.
type Checkpoint[S any] struct {
	FormatVersion  uint32
	RunID          string
	MachineID      string
	Revision       uint64
	Steps          uint64
	NextID         uint64
	ScheduleCursor uint64
	Completed      bool
	Final          *S
	Invocations    []Invocation[S]
	Groups         []ActivationGroup
	// HadLocalFailures remains true after a committed local failure.
	// Local error details and ended branch states are not retained.
	HadLocalFailures bool
	// Failure is non-nil only for a completed execution-level failure.
	Failure *Failure
}

// Result is the outcome of one call to Start, Resume, or Recover.
type Result[S any] struct {
	Status     Status
	Checkpoint Checkpoint[S]
}

// ResumeInput addresses exactly one waiting invocation. A continuation
// decodes Payload before applying it to the invocation's state.
type ResumeInput struct {
	InvocationID string
	Payload      []byte
}

// Store provides optional checkpoint persistence. Create accepts revision one
// and rejects an existing RunID. CompareAndSwap accepts the next revision for
// the same run and machine, and rejects stale revisions or machine IDs. Both
// writes reject malformed checkpoint headers with ErrInvalidCheckpoint.
// Load returns an independent copy and reports a missing run with
// checkpoint.ErrNotFound. ErrConflict guarantees no write. Other storage errors
// may leave commit outcome unknown; callers should Load before retrying.
type Store[S any] interface {
	Create(context.Context, Checkpoint[S]) error
	Load(context.Context, string) (Checkpoint[S], error)
	CompareAndSwap(context.Context, uint64, Checkpoint[S]) error
}

// Options controls one call's budget, concurrency, storage, and failure scope.
type Options[S any] struct {
	// Observer optionally receives transient execution events. Nil disables
	// observation without allocating a session or reading clocks.
	Observer        Observer
	MaxSteps        int
	MaxConcurrency  int
	Store           Store[S]
	FailureOverride *FailureScope
}

// Clone returns an independent checkpoint using copyState for every stored S.
// Store implementations can use it to satisfy their ownership contract.
func (s Checkpoint[S]) Clone(copyState Clone[S]) (Checkpoint[S], error) {
	if copyState == nil {
		return Checkpoint[S]{}, errors.New("checkpoint clone function is nil")
	}
	out := Copy(s)
	for i := range out.Invocations {
		state, err := callClone(copyState, s.Invocations[i].State, s.Invocations[i].ID)
		if err != nil {
			return Checkpoint[S]{}, fmt.Errorf("clone invocation %q: %w", s.Invocations[i].ID, err)
		}
		out.Invocations[i].State = state
	}
	if s.Final != nil {
		state, err := callClone(copyState, *s.Final, "")
		if err != nil {
			return Checkpoint[S]{}, fmt.Errorf("clone final state: %w", err)
		}
		out.Final = &state
	}
	return out, nil
}

func callClone[S any](clone Clone[S], state S, id string) (out S, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = &PanicError{Callback: "clone", InvocationID: id, Value: value, Stack: debug.Stack()}
		}
	}()
	return clone(state)
}

func FindInvocation[S any](s *Checkpoint[S], id string) (int, *Invocation[S]) {
	for i := range s.Invocations {
		if s.Invocations[i].ID == id {
			return i, &s.Invocations[i]
		}
	}
	return -1, nil
}

func FindGroup[S any](s *Checkpoint[S], id string) (int, *ActivationGroup) {
	for i := range s.Groups {
		if s.Groups[i].ID == id {
			return i, &s.Groups[i]
		}
	}
	return -1, nil
}

func NewID[S any](s *Checkpoint[S], prefix string) string {
	id := fmt.Sprintf("%s%d", prefix, s.NextID)
	s.NextID++
	return id
}
