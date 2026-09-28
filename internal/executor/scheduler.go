package executor

import (
	"context"
	"fmt"
	"math"
)

type workResult[S any] struct {
	id         string
	transition Transition[S]
	err        error
}

// nextReady rotates over invocation IDs rather than slice positions. The
// cursor remains meaningful when a group removes invocations from the slice.
func nextReady[S any](s Checkpoint[S], running map[string]context.CancelFunc, cursor uint64) *Invocation[S] {
	var after, wrapped *Invocation[S]
	var afterNumber, wrappedNumber uint64
	for i := range s.Invocations {
		inv := &s.Invocations[i]
		if inv.Status != InvocationReady {
			continue
		}
		if _, active := running[inv.ID]; active {
			continue
		}
		number := invocationNumber(inv.ID)
		if number > cursor {
			if after == nil || number < afterNumber {
				after, afterNumber = inv, number
			}
		} else if wrapped == nil || number < wrappedNumber {
			wrapped, wrappedNumber = inv, number
		}
	}
	if after != nil {
		return after
	}
	return wrapped
}

func hasReady[S any](s Checkpoint[S]) bool {
	for _, inv := range s.Invocations {
		if inv.Status == InvocationReady {
			return true
		}
	}
	return false
}

func availableCommits[S any](s Checkpoint[S]) uint64 {
	steps := math.MaxUint64 - s.Steps
	revisions := math.MaxUint64 - s.Revision
	if steps < revisions {
		return steps
	}
	return revisions
}

func (r *Runner[S]) drive(ctx context.Context, start Checkpoint[S], opts Options[S]) (Result[S], error) {
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	results := make(chan workResult[S], opts.MaxConcurrency)
	running := make(map[string]context.CancelFunc)
	s := start
	used := 0
	cursor := s.ScheduleCursor
	drain := func() {
		stop()
		for _, cancel := range running {
			cancel()
		}
		for len(running) != 0 {
			finished := <-results
			delete(running, finished.id)
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			drain()
			return resultWith(s, StatusCancelled), err
		}
		for len(running) < opts.MaxConcurrency && used < opts.MaxSteps {
			if uint64(len(running)) >= availableCommits(s) {
				break
			}
			next := nextReady(s, running, cursor)
			if next == nil {
				break
			}
			state, err := r.cloneState(next.State, next.ID)
			if err != nil {
				drain()
				return resultWith(s, StatusFailed), fmt.Errorf("clone node state: %w", err)
			}
			id := next.ID
			call := CallInfo{RunID: s.RunID, InvocationID: id, CallID: next.CallID}
			spec := r.nodes[next.Node]
			nodeCtx, cancel := context.WithCancel(runCtx)
			running[id] = cancel
			used++
			cursor = invocationNumber(id)
			go func() {
				transition, err := r.runNode(nodeCtx, call, spec, state)
				results <- workResult[S]{id: id, transition: transition, err: err}
			}()
		}
		if len(running) == 0 {
			if hasReady(s) && availableCommits(s) == 0 {
				return resultWith(s, StatusFailed), fmt.Errorf("schedule next node: %w", ErrExecutionLimit)
			}
			markCompleted(&s)
			if !s.Completed {
				waiting := false
				for _, inv := range s.Invocations {
					if inv.Status == InvocationWaiting || inv.Status == InvocationReady {
						waiting = true
						break
					}
				}
				if !waiting {
					return resultWith(s, StatusFailed), invalidCheckpoint("execution has unresolved groups without runnable or waiting invocations")
				}
			}
			return resultWith(s, statusOf(s, used >= opts.MaxSteps)), nil
		}
		var finished workResult[S]
		select {
		case <-ctx.Done():
			drain()
			return resultWith(s, StatusCancelled), ctx.Err()
		case finished = <-results:
		}
		running[finished.id]()
		delete(running, finished.id)
		if err := ctx.Err(); err != nil {
			drain()
			return resultWith(s, StatusCancelled), err
		}
		_, inv := invocation(&s, finished.id)
		if inv == nil || inv.Status != InvocationReady {
			continue
		}
		candidate := copyCheckpoint(s)
		candidate.Steps++
		var processErr error
		ended := false
		if finished.err == nil {
			ended, processErr = r.applyTransition(&candidate, finished.id, finished.transition)
			if processErr != nil {
				drain()
				return resultWith(s, errorStatus(processErr)), processErr
			}
		} else {
			processErr = finished.err
		}
		if processErr != nil {
			candidate = copyCheckpoint(s)
			candidate.Steps++
			scope := r.scope(r.nodes[inv.Node].OnError, opts)
			if failureErr := r.recordFailure(&candidate, inv.ID, inv.Node, scope, processErr); failureErr != nil {
				if terminalFailureRecord(candidate) != nil {
					candidate.ScheduleCursor = cursor
					result, commitErr := r.commitTerminalFailure(ctx, s, candidate, failureErr, opts.Store)
					drain()
					return result, commitErr
				}
				drain()
				return resultWith(s, StatusFailed), failureErr
			}
		}
		candidate.ScheduleCursor = cursor
		if !ended {
			if err := r.settleGroups(ctx, &candidate, opts.FailureOverride); err != nil {
				if terminalFailureRecord(candidate) != nil {
					result, commitErr := r.commitTerminalFailure(ctx, s, candidate, err, opts.Store)
					drain()
					return result, commitErr
				}
				drain()
				return resultWith(s, errorStatus(err)), err
			}
			markCompleted(&candidate)
		}
		var err error
		if s, err = r.commit(ctx, s, candidate, opts.Store); err != nil {
			drain()
			return resultWith(s, errorStatus(err)), err
		}
		for id, cancel := range running {
			_, current := invocation(&s, id)
			if current == nil || current.Status != InvocationReady {
				cancel()
			}
		}
		if ended {
			drain()
			return resultWith(s, statusOf(s, false)), nil
		}
	}
}
