package executor

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/afterlune/luneGraph/internal/limit"
	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

const defaultMaxSteps = 10_000

func normalizeOptions(opts Options) (Options, error) {
	if err := limit.Validate(opts.Limiter); err != nil {
		return opts, err
	}
	if opts.MaxSteps < 0 || opts.MaxConcurrency < 0 || opts.MaxInvocations < 0 {
		return opts, errors.New("limits must not be negative")
	}
	if opts.FailureOverride != nil && !validScope(*opts.FailureOverride) {
		return opts, errors.New("invalid failure override")
	}
	if opts.MaxSteps == 0 {
		opts.MaxSteps = defaultMaxSteps
	}
	if opts.MaxConcurrency == 0 {
		opts.MaxConcurrency = runtime.GOMAXPROCS(0)
	}
	if opts.MaxConcurrency < 1 {
		opts.MaxConcurrency = 1
	}
	return opts, nil
}

func (r *Runner[S]) resultWithHeader(header Header, final *S, status Status) (Result[S], error) {
	result := Result[S]{Status: status, Header: header}
	if final == nil {
		return result, nil
	}
	copy, err := r.cloneState(*final, "")
	if err != nil {
		return result, fmt.Errorf("clone final result: %w", err)
	}
	result.Final = &copy
	return result, nil
}

func (r *Runner[S]) resultFromLive(l *live[S], status Status) (Result[S], error) {
	return r.resultWithHeader(l.header.Header(), l.header.Final, status)
}

func (r *Runner[S]) resultFromLiveWithBudget(l *live[S], reason BudgetReason) (Result[S], error) {
	result, err := r.resultFromLive(l, StatusBudget)
	result.BudgetReason = reason
	return result, err
}

func (r *Runner[S]) resultFromCheckpoint(cp Checkpoint[S], status Status) (Result[S], error) {
	return r.resultWithHeader(cp.Header(), cp.Final, status)
}

func errorStatus(err error) Status {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return StatusCancelled
	}
	return StatusFailed
}

func statusOf(header Header, readyNodes int, exhausted bool) Status {
	if header.Completed {
		if header.Failure != nil {
			return StatusFailed
		}
		if header.HadLocalFailures {
			return StatusCompletedWithFailures
		}
		return StatusCompleted
	}
	if exhausted && readyNodes > 0 {
		return StatusBudget
	}
	return StatusWaiting
}

func invocationNumber(id string) uint64 {
	if len(id) <= 1 || id[0] != 'i' {
		return 0
	}
	var n uint64
	for i := 1; i < len(id); i++ {
		c := id[i]
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + uint64(c-'0')
	}
	return n
}

// Start creates a new execution and runs until completion, waiting, or budget
// exhaustion. It returns the live handle and this call's Result.
func (r *Runner[S]) Start(ctx context.Context, runID string, initial S, store Store[S], opts Options) (execution *Execution[S], result Result[S], retErr error) {
	if r == nil {
		return nil, Result[S]{Status: StatusFailed}, errors.New("runner is nil")
	}
	if ctx == nil {
		return nil, Result[S]{Status: StatusFailed}, errors.New("context must not be nil")
	}
	rawStore := store
	activeStore := store
	obs := observation.New(opts.Observer, r.id, runID)
	if obs != nil {
		span := obs.Begin(ctx, model.Event{Operation: model.OperationStart})
		defer func() {
			// A Store panic is not a returned result and leaves spans incomplete.
			if result.Status != "" {
				obs.DiscardPending(ctx, retErr)
				span.End(ctx, model.Event{Revision: result.Header.Revision, Status: result.Status, Err: retErr})
			}
		}()
		activeStore = observation.WrapStore(store, obs)
	}
	if !validName(runID) {
		return nil, Result[S]{Status: StatusFailed}, errors.New("run ID must be non-empty and have no surrounding whitespace")
	}
	var err error
	if opts, err = normalizeOptions(opts); err != nil {
		return nil, Result[S]{Status: StatusFailed}, err
	}
	if err = ctx.Err(); err != nil {
		return nil, Result[S]{Status: StatusCancelled}, err
	}
	state, err := r.cloneState(initial, "")
	if err != nil {
		return nil, Result[S]{Status: StatusFailed}, fmt.Errorf("clone initial state: %w", err)
	}
	s := Checkpoint[S]{FormatVersion: CheckpointFormatVersion, RunID: runID, MachineID: r.id, Revision: 1, NextID: 3, Invocations: []Invocation[S]{{ID: "i1", CallID: "c2", Node: r.entry, State: state, Status: InvocationReady}}}
	if activeStore != nil {
		if err := activeStore.Create(ctx, s); err != nil {
			return nil, Result[S]{Status: errorStatus(err)}, err
		}
	}
	l := newLive(s, r.joins)
	execution = &Execution[S]{runner: r, live: l, store: rawStore}
	result, retErr = r.driveLive(ctx, l, opts, obs, activeStore)
	return execution, result, retErr
}

// Fork creates an independent checkpoint from an active source, assigning it
// newRunID and resetting its Revision to 1. State values are cloned using the
// runner's Clone function. If store is non-nil, the checkpoint is created there.
// Fork does not execute callbacks or emit execution events; use Recover or
// Resume to drive the fork.
func (r *Runner[S]) Fork(ctx context.Context, newRunID string, source Checkpoint[S], store Store[S]) (Checkpoint[S], error) {
	if r == nil {
		return Checkpoint[S]{}, errors.New("runner is nil")
	}
	if ctx == nil {
		return Checkpoint[S]{}, errors.New("context must not be nil")
	}
	if !validName(newRunID) {
		return Checkpoint[S]{}, errors.New("run ID must be non-empty and have no surrounding whitespace")
	}
	if err := r.validateCheckpoint(source); err != nil {
		return Checkpoint[S]{}, err
	}
	if source.Completed {
		return Checkpoint[S]{}, ErrRunCompleted
	}
	if newRunID == source.RunID {
		return Checkpoint[S]{}, errors.New("fork run ID must differ from source run ID")
	}
	if err := ctx.Err(); err != nil {
		return Checkpoint[S]{}, err
	}
	forked, err := source.Clone(r.clone)
	if err != nil {
		return Checkpoint[S]{}, fmt.Errorf("clone forked checkpoint: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Checkpoint[S]{}, err
	}
	forked.RunID = newRunID
	forked.Revision = 1
	if store != nil {
		if err := store.Create(ctx, forked); err != nil {
			return Checkpoint[S]{}, err
		}
	}
	return forked, nil
}

func (r *Runner[S]) scope(declared FailureScope, opts Options) FailureScope {
	if opts.FailureOverride != nil {
		return *opts.FailureOverride
	}
	return declared
}
