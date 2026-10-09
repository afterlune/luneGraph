package model

import (
	"errors"
	"fmt"
)

// ErrInterrupted identifies an explicit, recoverable callback interruption.
var ErrInterrupted = errors.New("execution interrupted")

type interruptionError struct{ cause error }

func (e *interruptionError) Error() string        { return fmt.Sprintf("%s: %v", ErrInterrupted, e.cause) }
func (e *interruptionError) Is(target error) bool { return target == ErrInterrupted }
func (e *interruptionError) Unwrap() error        { return e.cause }

// Interrupt asks the runner to return without committing this callback's result.
// The last committed checkpoint remains recoverable. The cause is preserved for
// errors.Is and errors.As; a nil cause returns ErrInterrupted.
func Interrupt(cause error) error {
	if cause == nil {
		return ErrInterrupted
	}
	return &interruptionError{cause: cause}
}
