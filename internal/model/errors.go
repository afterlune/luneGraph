package model

import (
	"errors"
	"fmt"
)

var ErrConflict = errors.New("checkpoint revision conflict")
var ErrInvalidCheckpoint = errors.New("invalid checkpoint")
var ErrExecutionLimit = errors.New("execution counter exhausted")

// Status describes why a call to Start or Resume returned.
type Status string

const (
	StatusCompleted             Status = "completed"
	StatusCompletedWithFailures Status = "completed_with_failures"
	StatusWaiting               Status = "waiting"
	StatusBudget                Status = "budget_exhausted"
	StatusFailed                Status = "failed"
	StatusCancelled             Status = "cancelled"
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
