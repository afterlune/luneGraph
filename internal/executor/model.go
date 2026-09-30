package executor

import "github.com/afterlune/luneGraph/internal/model"

type (
	Action              = model.Action
	ActivationGroup     = model.ActivationGroup
	CallInfo            = model.CallInfo
	Checkpoint[S any]   = model.Checkpoint[S]
	Clone[S any]        = model.Clone[S]
	Continuation[S any] = model.Continuation[S]
	Failure             = model.Failure
	FailureScope        = model.FailureScope
	Invocation[S any]   = model.Invocation[S]
	InvocationStatus    = model.InvocationStatus
	JoinSpec[S any]     = model.JoinSpec[S]
	Machine[S any]      = model.Machine[S]
	NodeSpec[S any]     = model.NodeSpec[S]
	Options[S any]      = model.Options[S]
	PanicError          = model.PanicError
	Result[S any]       = model.Result[S]
	ResumeInput         = model.ResumeInput
	Status              = model.Status
	Store[S any]        = model.Store[S]
	Terminal[S any]     = model.Terminal[S]
	Transition[S any]   = model.Transition[S]
	TransitionError     = model.TransitionError
)

const (
	ActionContinue     = model.ActionContinue
	ActionWait         = model.ActionWait
	ActionEndBranch    = model.ActionEndBranch
	ActionEndExecution = model.ActionEndExecution
	ActionReturn       = model.ActionReturn

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

	CheckpointFormatVersion = model.CheckpointFormatVersion
)

var (
	ErrConflict          = model.ErrConflict
	ErrExecutionLimit    = model.ErrExecutionLimit
	ErrInvalidCheckpoint = model.ErrInvalidCheckpoint
	ErrRunCompleted      = model.ErrRunCompleted
	ErrRunFailed         = model.ErrRunFailed
)

func validName(name string) bool         { return model.ValidName(name) }
func validScope(scope FailureScope) bool { return model.ValidScope(scope) }
func copyCheckpoint[S any](value Checkpoint[S]) Checkpoint[S] {
	return model.Copy(value)
}
func copyCheckpointInto[S any](dst *Checkpoint[S], value Checkpoint[S]) Checkpoint[S] {
	model.CopyInto(dst, value)
	return *dst
}
func clearCheckpointStateValues[S any](value *Checkpoint[S]) {
	var zero S
	for i := range value.Invocations {
		value.Invocations[i].State = zero
	}
	for i := range value.Terminals {
		value.Terminals[i].State = zero
	}
	if value.Final != nil {
		*value.Final = zero
		value.Final = nil
	}
}
func invocation[S any](value *Checkpoint[S], id string) (int, *Invocation[S]) {
	return model.FindInvocation(value, id)
}
func group[S any](value *Checkpoint[S], id string) (int, *ActivationGroup) {
	return model.FindGroup(value, id)
}
func newID[S any](value *Checkpoint[S], prefix string) string {
	return model.NewID(value, prefix)
}
