package executor

import "github.com/afterlune/luneGraph/internal/model"

type (
	Change[S any]       = model.Change[S]
	Action              = model.Action
	ActivationBranch    = model.ActivationBranch
	ActivationGroup     = model.ActivationGroup
	Branch[S any]       = model.Branch[S]
	BranchResult[S any] = model.BranchResult[S]
	BudgetReason        = model.BudgetReason
	CallInfo            = model.CallInfo
	Checkpoint[S any]   = model.Checkpoint[S]
	Clone[S any]        = model.Clone[S]
	Continuation[S any] = model.Continuation[S]
	Failure             = model.Failure
	FailureScope        = model.FailureScope
	Header              = model.Header
	Invocation[S any]   = model.Invocation[S]
	InvocationStatus    = model.InvocationStatus
	JoinSpec[S any]     = model.JoinSpec[S]
	Machine[S any]      = model.Machine[S]
	NodeSpec[S any]     = model.NodeSpec[S]
	Options             = model.Options
	PanicError          = model.PanicError
	PendingInvocation   = model.PendingInvocation
	Result[S any]       = model.Result[S]
	ResumeInput         = model.ResumeInput
	Status              = model.Status
	Store[S any]        = model.Store[S]
	Transition[S any]   = model.Transition[S]
	TransitionError     = model.TransitionError
)

const (
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

	BudgetMaxSteps       = model.BudgetMaxSteps
	BudgetMaxInvocations = model.BudgetMaxInvocations

	CheckpointFormatVersion = model.CheckpointFormatVersion
)

var (
	ErrConflict           = model.ErrConflict
	ErrExecutionLimit     = model.ErrExecutionLimit
	ErrInvalidCheckpoint  = model.ErrInvalidCheckpoint
	ErrInvalidResumeInput = model.ErrInvalidResumeInput
	ErrRunCompleted       = model.ErrRunCompleted
	ErrRunFailed          = model.ErrRunFailed
	ErrInterrupted        = model.ErrInterrupted
)

func validName(name string) bool         { return model.ValidName(name) }
func validScope(scope FailureScope) bool { return model.ValidScope(scope) }
func copyCheckpoint[S any](value Checkpoint[S]) Checkpoint[S] {
	return model.Copy(value)
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
