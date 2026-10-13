package executor

import (
	"context"
	"errors"

	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

// Execution owns the live scheduler state for one run. Its methods must be
// called by only one goroutine at a time.
type Execution[S any] struct {
	runner *Runner[S]
	live   *live[S]
	store  Store[S]
}

// Pending returns sorted identities for continuations currently waiting on input.
func (e *Execution[S]) Pending() []PendingInvocation {
	if e == nil || e.live == nil {
		return nil
	}
	return e.live.pending()
}

// Checkpoint returns an independent full snapshot, cloning every live state.
func (e *Execution[S]) Checkpoint() (Checkpoint[S], error) {
	if e == nil || e.runner == nil || e.live == nil {
		return Checkpoint[S]{}, errors.New("execution is nil")
	}
	return e.live.snapshot().Clone(e.runner.clone)
}

// Advance applies continuation inputs and continues this live execution. When
// backed by a Store, it checks the persisted header revision before callbacks.
// Its observer operation is OperationAdvance; Runner.Resume reports OperationResume.
// All terminal handles return ErrRunCompleted, with or without a Store or inputs.
func (e *Execution[S]) Advance(ctx context.Context, inputs []ResumeInput, opts Options) (result Result[S], retErr error) {
	if ctx == nil {
		return Result[S]{Status: StatusFailed}, errors.New("context must not be nil")
	}
	if e == nil || e.runner == nil || e.live == nil {
		return Result[S]{Status: StatusFailed}, errors.New("execution is nil")
	}
	r := e.runner
	obs := observation.New(opts.Observer, r.id, e.live.header.RunID)
	if obs != nil {
		span := obs.Begin(ctx, model.Event{Operation: model.OperationAdvance, Revision: e.live.header.Revision})
		defer func() {
			if result.Status != "" {
				obs.DiscardPending(ctx, retErr)
				span.End(ctx, model.Event{Revision: result.Header.Revision, Status: result.Status, Err: retErr})
			}
		}()
	}
	activeStore := observation.WrapStore(e.store, obs)
	var err error
	if opts, err = normalizeOptions(opts); err != nil {
		return r.resultFromLiveOrEmpty(e.live, StatusFailed, err)
	}
	if err = ctx.Err(); err != nil {
		return r.resultFromLiveOrEmpty(e.live, StatusCancelled, err)
	}
	if activeStore != nil {
		header, loadErr := activeStore.LoadHeader(ctx, e.live.header.RunID)
		if loadErr != nil {
			return r.resultFromLiveOrEmpty(e.live, errorStatus(loadErr), loadErr)
		}
		if header.RunID != e.live.header.RunID || header.MachineID != r.id || header.Revision != e.live.header.Revision || header.FormatVersion != CheckpointFormatVersion {
			return r.resultFromLiveOrEmpty(e.live, StatusFailed, ErrConflict)
		}
		if header.Completed {
			return r.resultFromLiveOrEmpty(e.live, StatusFailed, ErrRunCompleted)
		}
	}
	if e.live.header.Completed {
		return r.resultFromLiveOrEmpty(e.live, StatusFailed, ErrRunCompleted)
	}
	return r.advanceLive(ctx, e.live, inputs, opts, activeStore, obs)
}

func (r *Runner[S]) resultFromLiveOrEmpty(l *live[S], status Status, primary error) (Result[S], error) {
	result, copyErr := r.resultFromLive(l, status)
	if primary != nil {
		return result, primary
	}
	return result, copyErr
}
