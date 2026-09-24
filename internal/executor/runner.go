package executor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

const defaultMaxSteps = 10_000

func normalizeOptions[S any](opts Options[S]) (Options[S], error) {
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
		if len(s.Failures) != 0 {
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
}

func invocationNumber(id string) uint64 {
	number, _ := strconv.ParseUint(strings.TrimPrefix(id, "i"), 10, 64)
	return number
}

func (r *Runner[S]) commit(ctx context.Context, before, after Checkpoint[S], store Store[S]) (Checkpoint[S], error) {
	if err := ctx.Err(); err != nil {
		return before, err
	}
	if before.Revision == math.MaxUint64 {
		return before, fmt.Errorf("commit revision: %w", ErrExecutionLimit)
	}
	after.Revision = before.Revision + 1
	sort.SliceStable(after.Terminals, func(i, j int) bool {
		return invocationNumber(after.Terminals[i].InvocationID) < invocationNumber(after.Terminals[j].InvocationID)
	})
	sort.SliceStable(after.Failures, func(i, j int) bool {
		return invocationNumber(after.Failures[i].InvocationID) < invocationNumber(after.Failures[j].InvocationID)
	})
	if store != nil {
		if err := store.CompareAndSwap(ctx, before.Revision, after); err != nil {
			return before, err
		}
	}
	return after, nil
}

// Start creates a new execution and runs until completion, waiting, or budget exhaustion.
func (r *Runner[S]) Start(ctx context.Context, runID string, initial S, opts Options[S]) (Result[S], error) {
	var empty Checkpoint[S]
	if r == nil {
		return resultWith(empty, StatusFailed), errors.New("runner is nil")
	}
	if ctx == nil {
		return resultWith(empty, StatusFailed), errors.New("context must not be nil")
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
	s := Checkpoint[S]{FormatVersion: CheckpointFormatVersion, RunID: runID, MachineID: r.id, Revision: 1, NextID: 2, Invocations: []Invocation[S]{{ID: "i1", Node: r.entry, State: state, Status: InvocationReady}}}
	if opts.Store != nil {
		if err := opts.Store.Create(ctx, s); err != nil {
			return resultWith(empty, errorStatus(err)), err
		}
	}
	return r.drive(ctx, s, opts)
}

func (r *Runner[S]) scope(declared FailureScope, opts Options[S]) FailureScope {
	if opts.FailureOverride != nil {
		return *opts.FailureOverride
	}
	return declared
}
