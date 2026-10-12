package model

import (
	"errors"
	"fmt"
)

var ErrConflict = errors.New("checkpoint revision conflict")
var ErrInvalidCheckpoint = errors.New("invalid checkpoint")

// ErrInvalidResumeInput reports a duplicate or non-matching waiting identity.
var ErrInvalidResumeInput = errors.New("invalid resume input")
var ErrExecutionLimit = errors.New("execution counter exhausted")

// ErrRunCompleted reports that Resume, Recover, Advance, or Fork was given a
// terminal execution where active work is required.
var ErrRunCompleted = errors.New("run already completed")

// ErrRunFailed reports a persisted execution-level failure after recovery.
var ErrRunFailed = errors.New("run failed")

// Status describes why a call to Start, Resume, Recover, or Advance returned.
type Status string

const (
	StatusCompleted             Status = "completed"
	StatusCompletedWithFailures Status = "completed_with_failures"
	StatusWaiting               Status = "waiting"
	StatusBudget                Status = "budget_exhausted"
	StatusFailed                Status = "failed"
	StatusCancelled             Status = "cancelled"
	StatusInterrupted           Status = "interrupted"
)

// BudgetReason identifies the per-call limit that prevented further progress.
// It is empty for results whose Status is not StatusBudget.
type BudgetReason string

const (
	// BudgetMaxSteps means the per-call node-start limit blocked progress.
	BudgetMaxSteps BudgetReason = "max_steps"
	// BudgetMaxInvocations means a transition would exceed the retained invocation limit.
	BudgetMaxInvocations BudgetReason = "max_invocations"
)

// PanicError reports a panic raised by an execution callback.
type PanicError struct {
	Callback     string
	InvocationID string
	Value        any
	Stack        []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("%s callback panicked for invocation %q: %v", e.Callback, e.InvocationID, e.Value)
}

func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// TransitionError reports invalid control flow returned by a node.
type TransitionError struct {
	InvocationID string
	Node         string
	Cause        error
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("invalid transition from %q (%s): %v", e.Node, e.InvocationID, e.Cause)
}

func (e *TransitionError) Unwrap() error { return e.Cause }
