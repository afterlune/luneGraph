package model

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"

	"github.com/afterlune/luneGraph/internal/limit"
)

// CheckpointFormatVersion is the format emitted and accepted by this runner.
const CheckpointFormatVersion uint32 = 6

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
	Error        string
	Target       string
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

// Result describes one public execution call without materializing its full
// checkpoint. Use Execution.Checkpoint when a full independent snapshot is needed.
type Result[S any] struct {
	Status Status
	Header Header
	Final  *S
}

// PendingInvocation identifies one waiting continuation without exposing its state.
type PendingInvocation struct {
	InvocationID string
	CallID       string
	Node         string
	Continuation string
}

// ResumeInput addresses one specific wait of an invocation. CallID must match
// the original waiting CallID; never rebind an old payload to a new wait.
// A continuation decodes Payload before applying it to the invocation's state.
type ResumeInput struct {
	InvocationID string
	CallID       string
	Payload      []byte
}

// Store persists execution checkpoints. Create accepts revision one and rejects
// an existing RunID. Load returns an independent full checkpoint with invocations
// and groups sorted by ID; group children preserve fan-out target order.
// LoadHeader returns copied metadata without loading invocation states. Commit
// atomically applies a Change at expected+1. ErrConflict guarantees no write;
// other Store errors can leave the outcome uncertain, so callers must reload
// before retrying. Writes own independent copies.
type Store[S any] interface {
	Create(context.Context, Checkpoint[S]) error
	Load(context.Context, string) (Checkpoint[S], error)

	LoadHeader(context.Context, string) (Header, error)
	Commit(context.Context, uint64, Change[S]) error
	Delete(context.Context, string) error
}

// Options configures one execution call. Persistence is passed separately to
// Runner methods so it is explicit at the point where a run is opened or created.
type Options struct {
	// Observer optionally receives transient execution events. Nil disables
	// observation without allocating a session or reading clocks.
	Observer Observer
	// Limiter optionally shares a callback budget across executions and runners.
	// Node, join and continuation Apply callbacks acquire one permit each.
	Limiter         *limit.Limiter
	MaxSteps        int
	MaxConcurrency  int
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
	var buf [32]byte
	n := copy(buf[:], prefix)
	b := strconv.AppendUint(buf[:n], s.NextID, 10)
	s.NextID++
	return string(b)
}
