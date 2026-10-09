package executor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"

	"github.com/afterlune/luneGraph/internal/limit"
	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

const defaultMaxSteps = 10_000

func normalizeOptions[S any](opts Options[S]) (Options[S], error) {
	if err := limit.Validate(opts.Limiter); err != nil {
		return opts, err
	}
	if opts.MaxSteps < 0 || opts.MaxConcurrency < 0 {
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

func resultWith[S any](s Checkpoint[S], status Status) Result[S] {
	return Result[S]{Status: status, Checkpoint: s}
}

func errorStatus(err error) Status {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return StatusCancelled
	}
	return StatusFailed
}

func statusOf[S any](s Checkpoint[S], exhausted bool) Status {
	if s.Completed {
		if s.Failure != nil {
			return StatusFailed
		}
		if s.HadLocalFailures {
			return StatusCompletedWithFailures
		}
		return StatusCompleted
	}
	if exhausted {
		for _, inv := range s.Invocations {
			if inv.Status == InvocationReady {
				return StatusBudget
			}
		}
	}
	return StatusWaiting
}

func markCompleted[S any](s *Checkpoint[S]) {
	if len(s.Groups) != 0 {
		return
	}
	for _, inv := range s.Invocations {
		if inv.Status == InvocationReady || inv.Status == InvocationWaiting || inv.Status == InvocationGroup {
			return
		}
	}
	s.Completed = true
	s.Invocations = nil
	s.Groups = nil
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

func (r *Runner[S]) commit(ctx context.Context, before, after Checkpoint[S], store Store[S], obs *observation.Session) (Checkpoint[S], error) {
	if err := ctx.Err(); err != nil {
		obs.ResolveSelected(ctx, model.OutcomeDiscarded, 0, err)
		return before, err
	}
	if before.Revision == math.MaxUint64 {
		err := fmt.Errorf("commit revision: %w", ErrExecutionLimit)
		obs.ResolveSelected(ctx, model.OutcomeDiscarded, 0, err)
		return before, err
	}
	after.Revision = before.Revision + 1
	if store != nil {
		if err := store.CompareAndSwap(ctx, before.Revision, after); err != nil {
			outcome := model.OutcomeUnknown
			if errors.Is(err, ErrConflict) {
				outcome = model.OutcomeDiscarded
			}
			obs.ResolveSelected(ctx, outcome, after.Revision, err)
			return before, err
		}
	}
	obs.ResolveSelected(ctx, model.OutcomeCommitted, after.Revision, nil)
	return after, nil
}

// Start creates a new execution and runs until completion, waiting, or budget exhaustion.
func (r *Runner[S]) Start(ctx context.Context, runID string, initial S, opts Options[S]) (result Result[S], retErr error) {
	var empty Checkpoint[S]
	if r == nil {
		return resultWith(empty, StatusFailed), errors.New("runner is nil")
	}
	if ctx == nil {
		return resultWith(empty, StatusFailed), errors.New("context must not be nil")
	}
	obs := observation.New(opts.Observer, r.id, runID)
	if obs != nil {
		span := obs.Begin(ctx, model.Event{Operation: model.OperationStart})
		defer func() {
			// A Store panic is not a returned result and leaves spans incomplete.
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
	if err = ctx.Err(); err != nil {
		return resultWith(empty, StatusCancelled), err
	}
	state, err := r.cloneState(initial, "")
	if err != nil {
		return resultWith(empty, StatusFailed), fmt.Errorf("clone initial state: %w", err)
	}
	s := Checkpoint[S]{FormatVersion: CheckpointFormatVersion, RunID: runID, MachineID: r.id, Revision: 1, NextID: 3, Invocations: []Invocation[S]{{ID: "i1", CallID: "c2", Node: r.entry, State: state, Status: InvocationReady}}}
	if opts.Store != nil {
		if err := opts.Store.Create(ctx, s); err != nil {
			return resultWith(empty, errorStatus(err)), err
		}
	}
	return r.drive(ctx, s, opts, obs)
}

// Fork creates an independent execution starting from source's execution position,
// assigning it newRunID and resetting its Revision to 1. State values are cloned
// using the runner's Clone function to ensure physical memory isolation.
// If opts.Store is provided, the initial forked checkpoint is created in the store.
// Fork then drives the execution forward according to opts, just like Start.
func (r *Runner[S]) Fork(ctx context.Context, newRunID string, source Checkpoint[S], opts Options[S]) (result Result[S], retErr error) {
	var empty Checkpoint[S]
	if r == nil {
		return resultWith(empty, StatusFailed), errors.New("runner is nil")
	}
	if ctx == nil {
		return resultWith(empty, StatusFailed), errors.New("context must not be nil")
	}
	if !validName(newRunID) {
		return resultWith(empty, StatusFailed), errors.New("run ID must be non-empty and have no surrounding whitespace")
	}
	if err := r.validateCheckpoint(source); err != nil {
		return resultWith(empty, StatusFailed), err
	}
	if source.Completed {
		return resultWith(empty, StatusFailed), ErrRunCompleted
	}
	obs := observation.New(opts.Observer, r.id, newRunID)
	if obs != nil {
		span := obs.Begin(ctx, model.Event{Operation: model.OperationStart})
		defer func() {
			if result.Status != "" {
				obs.DiscardPending(ctx, retErr)
				span.End(ctx, model.Event{Revision: result.Checkpoint.Revision, Status: result.Status, Err: retErr})
			}
		}()
		opts.Store = observation.WrapStore(opts.Store, obs)
	}
	var err error
	if opts, err = normalizeOptions(opts); err != nil {
		return resultWith(empty, StatusFailed), err
	}
	if err = ctx.Err(); err != nil {
		return resultWith(empty, StatusCancelled), err
	}
	forked, err := source.Clone(r.clone)
	if err != nil {
		return resultWith(empty, StatusFailed), fmt.Errorf("clone forked checkpoint: %w", err)
	}
	forked.RunID = newRunID
	forked.Revision = 1
	if opts.Store != nil {
		if err := opts.Store.Create(ctx, forked); err != nil {
			return resultWith(empty, errorStatus(err)), err
		}
	}
	return r.drive(ctx, forked, opts, obs)
}

func (r *Runner[S]) scope(declared FailureScope, opts Options[S]) FailureScope {
	if opts.FailureOverride != nil {
		return *opts.FailureOverride
	}
	return declared
}
