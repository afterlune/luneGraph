package executor

import (
	"context"
	"errors"

	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

// Recover loads the latest checkpoint for runID and advances it. It returns a
// live handle with this call's Result. A terminal run is returned without
// callbacks or another revision. Failed terminals return ErrRunFailed; inputs
// return ErrRunCompleted.
func (r *Runner[S]) Recover(ctx context.Context, runID string, inputs []ResumeInput, store Store[S], opts Options) (execution *Execution[S], result Result[S], retErr error) {
	if ctx == nil {
		return nil, Result[S]{Status: StatusFailed}, errors.New("context must not be nil")
	}
	if r == nil {
		return nil, Result[S]{Status: StatusFailed}, errors.New("runner is nil")
	}
	obs := observation.New(opts.Observer, r.id, runID)
	if obs != nil {
		span := obs.Begin(ctx, model.Event{Operation: model.OperationRecover})
		defer func() {
			if result.Status != "" {
				obs.DiscardPending(ctx, retErr)
				span.End(ctx, model.Event{Revision: result.Header.Revision, Status: result.Status, Err: retErr})
			}
		}()
	}
	if !validName(runID) {
		return nil, Result[S]{Status: StatusFailed}, errors.New("run ID must be non-empty and have no surrounding whitespace")
	}
	var err error
	if opts, err = normalizeOptions(opts); err != nil {
		return nil, Result[S]{Status: StatusFailed}, err
	}
	if store == nil {
		return nil, Result[S]{Status: StatusFailed}, errors.New("store is required for recovery")
	}
	if err = ctx.Err(); err != nil {
		return nil, Result[S]{Status: StatusCancelled}, err
	}
	activeStore := observation.WrapStore(store, obs)
	checkpoint, err := activeStore.Load(ctx, runID)
	if err != nil {
		return nil, Result[S]{Status: errorStatus(err)}, err
	}
	if err = ctx.Err(); err != nil {
		l := newLive(checkpoint, r.joins)
		execution = &Execution[S]{runner: r, live: l, store: store}
		result, retErr = r.resultFromLiveOrEmpty(l, StatusCancelled, err)
		return execution, result, retErr
	}
	if checkpoint.RunID != runID {
		result, resultErr := r.resultFromCheckpoint(checkpoint, StatusFailed)
		return nil, result, errors.Join(invalidCheckpoint("loaded run ID does not match requested run"), resultErr)
	}
	if err = r.validateCheckpoint(checkpoint); err != nil {
		result, resultErr := r.resultFromCheckpoint(checkpoint, StatusFailed)
		return nil, result, errors.Join(err, resultErr)
	}
	l := newLive(checkpoint, r.joins)
	execution = &Execution[S]{runner: r, live: l, store: store}
	if checkpoint.Completed {
		if len(inputs) != 0 {
			result, retErr = r.resultFromLiveOrEmpty(l, StatusFailed, ErrRunCompleted)
			return execution, result, retErr
		}
		if failure := recoveredFailure(checkpoint); failure != nil {
			result, retErr = r.resultFromLiveOrEmpty(l, StatusFailed, failure)
			return execution, result, retErr
		}
		status := statusOf(checkpoint.Header(), 0, false)
		result, retErr = r.resultFromLiveOrEmpty(l, status, nil)
		return execution, result, retErr
	}
	result, retErr = r.advanceLive(ctx, l, inputs, opts, activeStore, obs)
	return execution, result, retErr
}
