package executor

import (
	"context"
	"errors"

	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

// Recover loads the latest checkpoint for runID from opts.Store and advances it.
// A terminal run is returned without executing callbacks or writing another
// revision. A failed terminal returns ErrRunFailed; inputs return ErrRunCompleted.
func (r *Runner[S]) Recover(ctx context.Context, runID string, inputs []ResumeInput, opts Options[S]) (result Result[S], retErr error) {
	var empty Checkpoint[S]
	if ctx == nil {
		return resultWith(empty, StatusFailed), errors.New("context must not be nil")
	}
	if r == nil {
		return resultWith(empty, StatusFailed), errors.New("runner is nil")
	}
	obs := observation.New(opts.Observer, r.id, runID)
	if obs != nil {
		span := obs.Begin(ctx, model.Event{Operation: model.OperationRecover})
		defer func() {
			if result.Status != "" {
				obs.DiscardPending(ctx, retErr)
				span.End(ctx, model.Event{Revision: result.Checkpoint.Revision, Status: result.Status, Err: retErr})
			}
		}()
		opts.Store = observation.WrapStore(opts.Store, obs)
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
		if failure := recoveredFailure(checkpoint); failure != nil {
			return resultWith(checkpoint, StatusFailed), failure
		}
		return resultWith(checkpoint, statusOf(checkpoint, false)), nil
	}
	return r.resumeValidated(ctx, checkpoint, inputs, opts, obs)
}
