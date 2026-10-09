package executor

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/afterlune/luneGraph/internal/limit"
	"github.com/afterlune/luneGraph/internal/observation"
)

type workResult[S any] struct {
	id         string
	transition Transition[S]
	err        error
}

type workerTask[S any] struct {
	ctx      context.Context
	id       string
	call     CallInfo
	spec     NodeSpec[S]
	state    S
	obs      *observation.Session
	revision uint64
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

func (r *Runner[S]) drive(ctx context.Context, start Checkpoint[S], opts Options[S], obs *observation.Session) (Result[S], error) {
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	limiter := opts.Limiter
	workerLimit := limit.Capacity(limiter, opts.MaxConcurrency)
	results := make(chan workResult[S], workerLimit)
	running := make(map[string]context.CancelFunc, workerLimit)
	tasks := make(chan workerTask[S], workerLimit)
	var workers sync.WaitGroup
	workerCount := 0
	startWorker := func() {
		workerCount++
		workers.Add(1)
		go func() {
			defer workers.Done()
			for t := range tasks {
				var transition Transition[S]
				var err error
				if t.obs == nil {
					transition, err = r.runNode(t.ctx, t.call, t.spec, t.state)
				} else {
					transition, err = r.observedNode(t.ctx, t.obs, t.revision, t.call, t.spec, t.state)
				}
				limit.Release(limiter)
				results <- workResult[S]{id: t.id, transition: transition, err: err}
			}
		}()
	}
	defer func() {
		// Also cancel on an escaping Store panic before waiting for workers.
		// Callback permits are released by their workers on every return.
		stop()
		for _, cancel := range running {
			cancel()
		}
		close(tasks)
		workers.Wait()
	}()
	s := start
	index := newInvocationIndex(s)
	var progress groupProgress
	var spare Checkpoint[S]
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
	reserved := false
	defer func() {
		if reserved {
			limit.Release(limiter)
		}
	}()
	for {
		capacityBlocked := false
		if err := ctx.Err(); err != nil {
			drain()
			return resultWith(s, StatusCancelled), err
		}
		for len(running) < workerLimit && used < opts.MaxSteps {
			if uint64(len(running)) >= availableCommits(s) {
				break
			}
			next, number := selectReady(s, &index, running, cursor)
			if next == nil {
				break
			}
			if !reserved && !limit.TryAcquire(limiter) {
				capacityBlocked = true
				break
			}
			reserved = false
			state, err := r.cloneState(next.State, next.ID)
			if err != nil {
				limit.Release(limiter)
				drain()
				return resultWith(s, StatusFailed), fmt.Errorf("clone node state: %w", err)
			}
			if err := ctx.Err(); err != nil {
				limit.Release(limiter)
				drain()
				return resultWith(s, StatusCancelled), err
			}
			id := next.ID
			call := CallInfo{RunID: s.RunID, InvocationID: id, CallID: next.CallID, Node: next.Node, Step: s.Steps, BranchIndex: next.BranchIndex}
			spec := r.nodes[next.Node]
			nodeCtx, cancel := context.WithCancel(runCtx)
			running[id] = cancel
			used++
			cursor = number
			if len(running) > workerCount {
				startWorker()
			}
			tasks <- workerTask[S]{
				ctx:      nodeCtx,
				id:       id,
				call:     call,
				spec:     spec,
				state:    state,
				obs:      obs,
				revision: s.Revision,
			}
		}
		if len(running) == 0 && !capacityBlocked {
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
		var slots chan<- struct{}
		if capacityBlocked {
			slots = limit.Slots(limiter)
		}
		select {
		case slots <- struct{}{}:
			reserved = true
			continue
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
		_, inv := indexedInvocation(&index, &s, finished.id)
		if inv == nil || inv.Status != InvocationReady {
			obs.DiscardNode(ctx, finished.id)
			continue
		}
		obs.SelectNode(finished.id)
		if isCallbackInterruption(finished.err) {
			drain()
			return interruptedResult(ctx, s, finished.err)
		}
		candidate := copyCheckpointInto(&spare, s)
		candidate.Steps++
		var processErr error
		ended := false
		if finished.err == nil {
			ended, processErr = r.applyTransition(&candidate, &index, finished.id, finished.transition)
			if processErr != nil {
				drain()
				return resultWith(s, errorStatus(processErr)), processErr
			}
		} else {
			processErr = finished.err
		}
		if processErr != nil {
			candidate = copyCheckpointInto(&spare, s)
			candidate.Steps++
			scope := r.scope(r.nodes[inv.Node].OnError, opts)
			if failureErr := r.recordFailure(&candidate, &index, inv.ID, inv.Node, scope, processErr); failureErr != nil {
				if candidate.Failure != nil {
					candidate.ScheduleCursor = cursor
					result, commitErr := r.commitTerminalFailure(ctx, s, candidate, failureErr, opts.Store, obs)
					drain()
					return result, commitErr
				}
				drain()
				return resultWith(s, StatusFailed), failureErr
			}
		}
		candidate.ScheduleCursor = cursor
		if !ended {
			if err := r.settleGroups(ctx, &candidate, &index, &progress, opts.FailureOverride, opts.Limiter, obs); err != nil {
				if isJoinInterruption(err) {
					drain()
					return interruptedResult(ctx, s, err)
				}
				if candidate.Failure != nil {
					result, commitErr := r.commitTerminalFailure(ctx, s, candidate, err, opts.Store, obs)
					drain()
					return result, commitErr
				}
				drain()
				return resultWith(s, errorStatus(err)), err
			}
			markCompleted(&candidate)
		}
		previous := s
		committed, err := r.commit(ctx, s, candidate, opts.Store, obs)
		if err != nil {
			drain()
			return resultWith(previous, errorStatus(err)), err
		}
		s = committed
		spare = previous
		clearCheckpointStateValues(&spare)
		for id, cancel := range running {
			_, current := indexedInvocation(&index, &s, id)
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
