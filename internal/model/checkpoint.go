package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/afterlune/luneGraph/internal/limit"
)

// CheckpointFormatVersion is the format emitted and accepted by this runner.
const CheckpointFormatVersion uint32 = 8

// InvocationStatus describes one path's scheduling state.
type InvocationStatus uint8

const (
	InvocationReady   InvocationStatus = 1
	InvocationWaiting InvocationStatus = 2
	InvocationGroup   InvocationStatus = 3
	InvocationJoined  InvocationStatus = 4
	InvocationEnded   InvocationStatus = 5
	InvocationFailed  InvocationStatus = 6
)

func (s InvocationStatus) String() string {
	switch s {
	case 0:
		return ""
	case InvocationReady:
		return "ready"
	case InvocationWaiting:
		return "waiting"
	case InvocationGroup:
		return "group"
	case InvocationJoined:
		return "joined"
	case InvocationEnded:
		return "ended"
	case InvocationFailed:
		return "failed"
	default:
		return fmt.Sprintf("InvocationStatus(%d)", s)
	}
}

func (s InvocationStatus) Resolved() bool {
	return s >= InvocationJoined && s <= InvocationFailed
}

func (s InvocationStatus) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s *InvocationStatus) UnmarshalText(text []byte) error {
	switch string(text) {
	case "", "0":
		*s = 0
	case "ready", "1":
		*s = InvocationReady
	case "waiting", "2":
		*s = InvocationWaiting
	case "group", "3":
		*s = InvocationGroup
	case "joined", "4":
		*s = InvocationJoined
	case "ended", "5":
		*s = InvocationEnded
	case "failed", "6":
		*s = InvocationFailed
	default:
		return fmt.Errorf("invalid invocation status %q", string(text))
	}
	return nil
}

func (s InvocationStatus) MarshalJSON() ([]byte, error) {
	switch s {
	case 0:
		return []byte(`""`), nil
	case InvocationReady:
		return []byte(`"ready"`), nil
	case InvocationWaiting:
		return []byte(`"waiting"`), nil
	case InvocationGroup:
		return []byte(`"group"`), nil
	case InvocationJoined:
		return []byte(`"joined"`), nil
	case InvocationEnded:
		return []byte(`"ended"`), nil
	case InvocationFailed:
		return []byte(`"failed"`), nil
	default:
		return json.Marshal(s.String())
	}
}

func (s *InvocationStatus) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		*s = 0
		return nil
	}
	if data[0] == '"' {
		var str string
		if err := json.Unmarshal(data, &str); err != nil {
			return err
		}
		return s.UnmarshalText([]byte(str))
	}
	var num uint8
	if err := json.Unmarshal(data, &num); err != nil {
		return err
	}
	*s = InvocationStatus(num)
	return nil
}

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

// ActivationBranch stores the stable, ordered identity and outcome metadata
// for one invocation in an activation group.
type ActivationBranch struct {
	// InvocationID identifies this branch's invocation.
	InvocationID string
	// Target is the branch's declared target node.
	Target string
	// Error is the local failure message when the invocation failed.
	Error string
	// EntryTimeout is the pending entry callback override. Successful node
	// outcomes clear it atomically; failed branches may retain it for inspection.
	EntryTimeout time.Duration
}

// ActivationGroup correlates branches started by one fan-out occurrence.
// CallID identifies the pending join callback when JoinNode is set.
type ActivationGroup struct {
	ID       string
	CallID   string
	Source   string
	ParentID string
	JoinNode string
	Branches []ActivationBranch
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
	// Failed activation branches retain their error message until their group
	// settles; ended branch states are not exposed as join results.
	HadLocalFailures bool
	// Failure is non-nil only for a completed execution-level failure.
	Failure *Failure
}

// Result describes one public execution call without materializing its full
// checkpoint. Use Execution.Checkpoint when a full independent snapshot is needed.
type Result[S any] struct {
	// Status is the outcome of this public call.
	Status Status
	// BudgetReason identifies the limit when Status is StatusBudget; otherwise it is empty.
	BudgetReason BudgetReason
	Header       Header
	Final        *S
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
// and groups sorted by ID; group branches preserve fan-out target order.
// LoadHeader returns copied metadata without loading invocation states. Commit
// atomically applies a Change at expected+1. ErrConflict guarantees no write;
// other Store errors can leave the outcome uncertain, so callers must reload
// before retrying. Writes own independent copies. All methods must support
// concurrent calls from multiple executions and runners.
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
	Limiter        *limit.Limiter
	MaxSteps       int
	MaxConcurrency int
	// MaxInvocations limits the number of invocation records retained by one
	// execution. Zero leaves invocation creation unbounded; negative values are
	// invalid.
	MaxInvocations  int
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

const (
	smallIDLimit      = 1024
	smallGroupIDLimit = 256
)

var (
	smallCallIDs  [smallIDLimit]string
	smallInvIDs   [smallIDLimit]string
	smallGroupIDs [smallGroupIDLimit]string
)

func init() {
	for i := range smallIDLimit {
		s := strconv.Itoa(i)
		smallCallIDs[i] = "c" + s
		smallInvIDs[i] = "i" + s
		if i < smallGroupIDLimit {
			smallGroupIDs[i] = "g" + s
		}
	}
}

func AllocID(nextID *uint64, prefix string) string {
	id := *nextID
	*nextID = id + 1
	if id < smallIDLimit {
		switch prefix {
		case "c":
			return smallCallIDs[id]
		case "i":
			return smallInvIDs[id]
		case "g":
			if id < smallGroupIDLimit {
				return smallGroupIDs[id]
			}
		}
	}
	var buf [32]byte
	n := copy(buf[:], prefix)
	b := strconv.AppendUint(buf[:n], id, 10)
	return string(b)
}

func NewID[S any](s *Checkpoint[S], prefix string) string {
	return AllocID(&s.NextID, prefix)
}
