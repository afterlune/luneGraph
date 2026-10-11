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
		return resultWith(checkpoint, StatusCancelled), cancelled
	}
	return resultWith(checkpoint, StatusInterrupted), err
}
