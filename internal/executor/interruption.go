package executor

import (
	"context"
	"errors"
)

// Only errors returned by effect-capable callbacks are control signals.
// PanicError unwraps its panic value, so exclude panics before checking the marker.
func isCallbackInterruption(err error) bool {
	if err == nil || !errors.Is(err, ErrInterrupted) {
		return false
	}
	var panicErr *PanicError
	return !errors.As(err, &panicErr)
}

func interruptedResult[S any](ctx context.Context, checkpoint Checkpoint[S], err error) (Result[S], error) {
	if cancelled := ctx.Err(); cancelled != nil {
		return Result[S]{Status: StatusCancelled, Header: checkpoint.Header(), Final: checkpoint.Final}, cancelled
	}
	return Result[S]{Status: StatusInterrupted, Header: checkpoint.Header(), Final: checkpoint.Final}, err
}
