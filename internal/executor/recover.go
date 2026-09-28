package executor

import (
	"context"
	"errors"
)

// Recover loads the latest checkpoint for runID from opts.Store and advances it.
// A completed run is returned without executing callbacks or writing another
// revision; supplying inputs to one returns ErrRunCompleted.
func (r *Runner[S]) Recover(ctx context.Context, runID string, inputs []ResumeInput, opts Options[S]) (Result[S], error) {
	var empty Checkpoint[S]
	if ctx == nil {
		return resultWith(empty, StatusFailed), errors.New("context must not be nil")
	}
	if r == nil {
		return resultWith(empty, StatusFailed), errors.New("runner is nil")
	}
	if !validName(runID) {
		return resultWith(empty, StatusFailed), errors.New("run ID must be non-empty and have no surrounding whitespace")
	}
	var err error
	if opts, err = normalizeOptions(opts); err != nil {
		return resultWith(empty, StatusFailed), err
	}
	if opts.Store == nil {
		return resultWith(empty, StatusFailed), errors.New("store is required for recovery")
	}
	if err = ctx.Err(); err != nil {
		return resultWith(empty, StatusCancelled), err
	}
	checkpoint, err := opts.Store.Load(ctx, runID)
	if err != nil {
		return resultWith(empty, errorStatus(err)), err
	}
	if err = ctx.Err(); err != nil {
		return resultWith(checkpoint, StatusCancelled), err
	}
	if checkpoint.RunID != runID {
		return resultWith(checkpoint, StatusFailed), invalidCheckpoint("loaded run ID does not match requested run")
	}
	if err = r.validateCheckpoint(checkpoint); err != nil {
		return resultWith(checkpoint, StatusFailed), err
	}
	if checkpoint.Completed {
		if len(inputs) != 0 {
			return resultWith(checkpoint, StatusFailed), ErrRunCompleted
		}
		return resultWith(checkpoint, statusOf(checkpoint, false)), nil
	}
	return r.resumeValidated(ctx, checkpoint, inputs, opts)
}
