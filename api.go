// Package graph builds and executes typed stateful graphs.
package graph

import (
	"context"
	"encoding/json"

	"github.com/afterlune/luneGraph/internal/definition"
	"github.com/afterlune/luneGraph/internal/executor"
	"github.com/afterlune/luneGraph/internal/limit"
	"github.com/afterlune/luneGraph/internal/model"
)

type (
	Header           = model.Header
	Change[S any]    = model.Change[S]
	Limiter          = limit.Limiter
	Event            = model.Event
	EventOperation   = model.EventOperation
	EventPhase       = model.EventPhase
	CallbackOutcome  = model.CallbackOutcome
	Observer         = model.Observer
	ObserverFunc     = model.ObserverFunc
	Action           = model.Action
	ActivationGroup  = model.ActivationGroup
	ActivationBranch = model.ActivationBranch
	// Branch declares an independently cloned task. EntryTimeout overrides only
	// each attempt of its initial node callback; zero inherits the node timeout.
	Branch[S any]       = model.Branch[S]
	BranchResult[S any] = model.BranchResult[S]
	// BudgetReason identifies the per-call limit that prevented further progress.
	BudgetReason      = model.BudgetReason
	CallInfo          = model.CallInfo
	Checkpoint[S any] = model.Checkpoint[S]
	Clone[S any]      = model.Clone[S]
	Config[S any]     = model.Config[S]
	Failure           = model.Failure
	FailureScope      = model.FailureScope
	Graph[S any]      = definition.Graph[S]
	Invocation[S any] = model.Invocation[S]
	InvocationStatus  = model.InvocationStatus
	// JoinSpec defines one fan-out join. Quorum and CanMerge are exclusive.
	JoinSpec[S any]   = model.JoinSpec[S]
	Merge[S any]      = model.Merge[S]
	Node[S any]       = model.Node[S]
	NodeSpec[S any]   = model.NodeSpec[S]
	Options           = model.Options
	PanicError        = model.PanicError
	PendingInvocation = model.PendingInvocation
	// Result describes one public call; BudgetReason is set only for StatusBudget.
	Result[S any]    = model.Result[S]
	ResumeInput      = model.ResumeInput
	Execution[S any] = executor.Execution[S]
	Runner[S any]    = executor.Runner[S]
	// Status describes the outcome of one Start, Resume, Recover, or Advance call.
	Status = model.Status
	// Store persists independent execution positions and supports concurrent calls.
	Store[S any] = model.Store[S]
	// Transition is constructed with the control-flow helpers; its fields are private.
	// A zero value returned with nil error is an invalid transition.
	Transition[S any] = model.Transition[S]
	TransitionError   = model.TransitionError
)

const (
	OperationStart  = model.OperationStart
	OperationResume = model.OperationResume
	// OperationAdvance identifies a call that continues an existing live handle.
	OperationAdvance = model.OperationAdvance
	OperationRecover = model.OperationRecover
	OperationNode    = model.OperationNode
	OperationJoin    = model.OperationJoin
	OperationDecode  = model.OperationDecode
	OperationApply   = model.OperationApply
	OperationCreate  = model.OperationCreate
	OperationLoad    = model.OperationLoad
	OperationCommit  = model.OperationCommit
	OperationDelete  = model.OperationDelete
	PhaseStarted     = model.PhaseStarted
	PhaseFinished    = model.PhaseFinished
	PhaseResolved    = model.PhaseResolved
	OutcomeCommitted = model.OutcomeCommitted
	OutcomeDiscarded = model.OutcomeDiscarded
	OutcomeUnknown   = model.OutcomeUnknown

	ActionContinue       = model.ActionContinue
	ActionWait           = model.ActionWait
	ActionFanOut         = model.ActionFanOut
	ActionCompleteBranch = model.ActionCompleteBranch
	ActionEndBranch      = model.ActionEndBranch
	ActionEndExecution   = model.ActionEndExecution
	ActionReturn         = model.ActionReturn

	FailInvocation = model.FailInvocation
	FailGroup      = model.FailGroup
	FailExecution  = model.FailExecution

	InvocationReady   = model.InvocationReady
	InvocationWaiting = model.InvocationWaiting
	InvocationGroup   = model.InvocationGroup
	InvocationJoined  = model.InvocationJoined
	InvocationEnded   = model.InvocationEnded
	InvocationFailed  = model.InvocationFailed

	StatusCompleted             = model.StatusCompleted
	StatusCompletedWithFailures = model.StatusCompletedWithFailures
	StatusWaiting               = model.StatusWaiting
	StatusBudget                = model.StatusBudget
	StatusFailed                = model.StatusFailed
	StatusCancelled             = model.StatusCancelled
	StatusInterrupted           = model.StatusInterrupted

	// BudgetMaxSteps reports that node-start admission exhausted its per-call limit.
	BudgetMaxSteps = model.BudgetMaxSteps
	// BudgetMaxInvocations reports that a transition exceeded the retained invocation limit.
	BudgetMaxInvocations = model.BudgetMaxInvocations

	CheckpointFormatVersion = model.CheckpointFormatVersion
)

var (
	// ErrConflict means the stored execution revision changed before commit.
	ErrConflict = model.ErrConflict
	// ErrExecutionLimit means an execution counter cannot advance safely.
	ErrExecutionLimit = model.ErrExecutionLimit
	// ErrInvalidCheckpoint means a checkpoint violates the execution schema.
	ErrInvalidCheckpoint = model.ErrInvalidCheckpoint
	// ErrInvalidResumeInput means a continuation input does not match a pending wait.
	ErrInvalidResumeInput = model.ErrInvalidResumeInput
	// ErrRunCompleted means an operation requiring active work was given a terminal run.
	ErrRunCompleted = model.ErrRunCompleted
	// ErrRunFailed reports a persisted execution-level failure after recovery.
	ErrRunFailed = model.ErrRunFailed
	// ErrInterrupted is the default cause returned by Interrupt(nil).
	ErrInterrupted = model.ErrInterrupted
)

// New creates a graph builder with the given entry vertex name.
func New[S any](entry string) *Graph[S] { return definition.New[S](entry) }

// NewLimiter creates a fixed, process-local budget shared by node, join and
// continuation Apply callbacks. Share its pointer across Options and runners.
// Capacity must be positive. Waiting responds to context cancellation.
func NewLimiter(capacity int) (*Limiter, error) { return limit.New(capacity) }

// RegisterContinuation adapts a typed continuation handler to persisted bytes.
// Apply receives a CallInfo whose CallID remains stable across recovery replays.
func RegisterContinuation[S, P any](g *Graph[S], key string, decode func([]byte) (P, error), apply func(context.Context, CallInfo, S, P) (S, error)) error {
	return definition.RegisterContinuation(g, key, decode, apply)
}

// RegisterJSONContinuation registers a typed continuation that unmarshals its payload as JSON into P.
func RegisterJSONContinuation[S, P any](g *Graph[S], key string, apply func(context.Context, CallInfo, S, P) (S, error)) error {
	return definition.RegisterJSONContinuation(g, key, apply)
}

// ValueClone returns a Clone function that performs a direct Go value copy.
// Suitable for scalar types, immutable types, or structs containing no reference fields (pointers, slices, maps).
func ValueClone[S any](state S) (S, error) {
	return state, nil
}

// JSONClone returns a Clone function that deep-copies state via JSON encoding and decoding.
// State must survive a JSON round trip. Unexported and json:"-" fields are lost;
// interface values may change concrete types. Prefer a manual Clone for hot paths.
func JSONClone[S any](state S) (S, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return state, err
	}
	var cloned S
	if err := json.Unmarshal(data, &cloned); err != nil {
		return state, err
	}
	return cloned, nil
}

// To continues to the required target or fans out to additional declared targets.
func To[S any](state S, target string, additionalTargets ...string) Transition[S] {
	return model.To(state, target, additionalTargets...)
}

// ToTargets continues to a runtime-generated list of declared targets. The
// list must not be empty; an empty list is rejected when the transition runs.
func ToTargets[S any](state S, targets []string) Transition[S] {
	return model.ToTargets(state, targets)
}

// Wait commits state and suspends this invocation for a registered continuation.
// After Apply updates state, execution follows the required target or fans out
// to additional declared targets.
func Wait[S any](state S, continuation, target string, additionalTargets ...string) Transition[S] {
	return model.Wait(state, continuation, target, additionalTargets...)
}

// FanOut creates one independent child invocation for each branch.
func FanOut[S any](parent S, branches ...Branch[S]) Transition[S] {
	return model.FanOut(parent, branches...)
}

// CompleteBranch returns this invocation's state to its activation group's join.
func CompleteBranch[S any](state S) Transition[S] {
	return model.CompleteBranch(state)
}

// BranchValues extracts successful states from branch results in order.
func BranchValues[S any](branches []BranchResult[S]) []S {
	out := make([]S, 0, len(branches))
	for _, b := range branches {
		if b.OK() {
			out = append(out, b.State)
		}
	}
	return out
}

// EndBranch finishes the current invocation without producing a result.
func EndBranch[S any]() Transition[S] { return model.EndBranch[S]() }

// EndExecution finishes the whole run with state as its final result.
func EndExecution[S any](state S) Transition[S] { return model.EndExecution(state) }

// Return leaves a mounted subgraph and follows the edge leaving its mount point.
func Return[S any](state S) Transition[S] { return model.Return(state) }

// Interrupt asks a node, join, or continuation handler to end the current call
// without committing its result or applying its failure policy. The returned
// Result reports StatusInterrupted and its last committed Header. Recover or
// Resume may replay pending callbacks with their original CallID. The cause is
// preserved for errors.Is and errors.As; a nil cause returns ErrInterrupted.
func Interrupt(cause error) error { return model.Interrupt(cause) }
