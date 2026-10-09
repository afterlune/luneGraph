package model

import (
	"context"
	"time"
)

// EventOperation identifies the operation being observed.
type EventOperation string

const (
	OperationStart          EventOperation = "start"
	OperationResume         EventOperation = "resume"
	OperationRecover        EventOperation = "recover"
	OperationNode           EventOperation = "node"
	OperationJoin           EventOperation = "join"
	OperationDecode         EventOperation = "decode"
	OperationApply          EventOperation = "apply"
	OperationCreate         EventOperation = "create"
	OperationLoad           EventOperation = "load"
	OperationCompareAndSwap EventOperation = "compare_and_swap"
	OperationDelete         EventOperation = "delete"
)

// EventPhase identifies the beginning or end of an operation.
type EventPhase string

const (
	PhaseStarted  EventPhase = "started"
	PhaseFinished EventPhase = "finished"
	// PhaseResolved reports acceptance or rejection of an execution callback.
	PhaseResolved EventPhase = "resolved"
)

// CallbackOutcome describes what happened to one callback's result, not its
// external side effects. With no Store, committed means accepted in memory.
type CallbackOutcome string

const (
	OutcomeCommitted CallbackOutcome = "committed"
	OutcomeDiscarded CallbackOutcome = "discarded"
	OutcomeUnknown   CallbackOutcome = "unknown"
)

// Event contains execution metadata, never application state or resume bytes.
// Events are best effort and are not persisted. A callback finishing does not
// mean its outcome committed. Err may contain application-provided messages.
// Treat errors and any objects they reference as read-only.
type Event struct {
	Operation EventOperation
	Phase     EventPhase
	// Outcome is populated only on resolved node, join and Apply events.
	Outcome CallbackOutcome
	// OperationID correlates one public Runner call within this process. It is
	// not durable; use RunID and CallID to correlate logical callback replays.
	OperationID  uint64
	RunID        string
	MachineID    string
	InvocationID string
	CallID       string
	Node         string
	Continuation string
	// Revision is the callback's base revision, the attempted write revision,
	// or the returned revision for a successful Load or public call. On a
	// resolved callback it is the target revision if commit was entered,
	// otherwise the callback's base revision.
	Revision uint64
	Time     time.Time
	// Duration is set on finished events and excludes their own delivery and
	// the delivery of the corresponding started event. Public-call durations
	// include delivery of nested events.
	Duration time.Duration
	// Status is populated only for a finished public Runner call.
	Status Status
	// Action is the node's returned decision, before transition validation.
	Action Action
	Err    error
}

// Observer receives events synchronously, potentially from concurrent calls
// and workers. Observe must be concurrency safe and return promptly. Panics
// are recovered independently for each delivery and do not fail an execution.
type Observer interface {
	Observe(context.Context, Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Event)

func (f ObserverFunc) Observe(ctx context.Context, event Event) { f(ctx, event) }
