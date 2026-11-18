package executor

import (
	"context"
	"errors"
	"math"

	"github.com/afterlune/luneGraph/internal/limit"
	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

func (r *Runner[S]) driveLive(ctx context.Context, l *live[S], opts Options, obs *observation.Session, store Store[S]) (Result[S], error) {
	workerLimit := limit.Capacity(opts.Limiter, opts.MaxConcurrency)
	var pool *workerPool[S]
	var running map[taskRef]context.CancelFunc
	runningNodes := uint64(0)
	var prepared map[string]workerTask[S]
	defer func() {
		for _, cancel := range running {
			cancel()
		}
		if pool != nil {
			pool.close()
		}
	}()
	var budgetReason BudgetReason
	drain := func() {
		for _, cancel := range running {
			cancel()
		}
		for len(running) > 0 {
			out := <-pool.results
			delete(running, out.ref)
			if l.valid(out.ref) {
				l.enqueue(out.ref)
			}
		}
		runningNodes = 0
	}
	finish := func(status Status, err error) (Result[S], error) {
		drain()
		if status == StatusInterrupted && ctx.Err() != nil {
			status = StatusCancelled
			err = ctx.Err()
		}
		var result Result[S]
		var copyErr error
		if status == StatusBudget {
			result, copyErr = r.resultFromLiveWithBudget(l, budgetReason)
		} else {
			result, copyErr = r.resultFromLive(l, status)
		}
		if err == nil {
			err = copyErr
		}
		return result, err
	}
	l.failureOverride = opts.FailureOverride
	used := 0
	cursor := l.header.ScheduleCursor
	reserved := false
	defer func() {
		if reserved {
			limit.Release(opts.Limiter)
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return finish(StatusCancelled, err)
		}
		var immediateResult workResult[S]
		immediate := false
		blocked := false
		for len(running) < workerLimit {
			if uint64(len(running)) >= math.MaxUint64-l.header.Revision && len(running) > 0 {
				break
			}
			ref, ok := l.next(used < opts.MaxSteps && runningNodes < math.MaxUint64-l.header.Steps, running)
			if !ok {
				break
			}
			if lg, _, ok := l.group(ref.id, ref.slot); ref.join && ok && lg.group.JoinNode == "" {
				outcome := r.commitResult(ctx, l, workResult[S]{ref: ref}, opts.MaxInvocations, cursor, store, obs, prepared)
				if err := outcome.CommitErr; err != nil {
					l.enqueue(ref)
					return finish(errorStatus(err), err)
				}
				if outcome.TerminalErr != nil {
					return finish(StatusFailed, outcome.TerminalErr)
				}
				if l.header.Completed {
					return finish(statusOf(l.header.Header(), l.readyNodes, false), nil)
				}
				cancelInvalidTasks(l, running)
				continue
			}
			if l.header.Revision == math.MaxUint64 || (!ref.join && l.header.Steps == math.MaxUint64) {
				return finish(StatusFailed, ErrExecutionLimit)
			}

			var task workerTask[S]
			var err error
			// Ready join inputs cannot change while this activation is pending.
			// Reuse admission-blocked inputs rather than cloning on every result.
			if ref.join {
				var cached bool
				task, cached = prepared[ref.id]
				if !cached {
					if reserved {
						limit.Release(opts.Limiter)
						reserved = false
					}
					task, err = r.prepareTask(ctx, l, ref, obs)
					if err != nil {
						l.enqueue(ref)
						return finish(errorStatus(err), err)
					}
					if !task.ready {
						if lg, _, ok := l.group(ref.id, ref.slot); ok {
							lg.joinEnqueued = false
						}
						continue
					}
					if task.preflightErr != nil {
						immediateResult = workResult[S]{ref: ref, callID: task.call.CallID, err: task.preflightErr}
						immediate = true
						break
					}
					if prepared == nil {
						prepared = make(map[string]workerTask[S])
					}
					prepared[ref.id] = task
				}
			}
			if !reserved && !limit.TryAcquire(opts.Limiter) {
				l.enqueue(ref)
				blocked = true
				break
			}
			reserved = false
			if !ref.join {
				task, err = r.prepareTask(ctx, l, ref, obs)
				if err != nil {
					limit.Release(opts.Limiter)
					l.enqueue(ref)
					return finish(errorStatus(err), err)
				}
			}
			if err := ctx.Err(); err != nil {
				limit.Release(opts.Limiter)
				l.enqueue(ref)
				return finish(StatusCancelled, err)
			}

			// When no other task is running and no concurrent task can be dispatched,
			// execute directly on the driver to avoid worker pool channel and context overhead.
			if len(running) == 0 && (workerLimit == 1 || (l.joinQueue.len() == 0 && (l.nodeQueue.len() == 0 || (!ref.join && used+1 >= opts.MaxSteps)))) {
				task.call.Step = l.header.Steps
				task.revision = l.header.Revision
				if ref.join && prepared != nil {
					delete(prepared, ref.id)
				}
				if !ref.join {
					used++
					cursor = invocationNumber(ref.id)
				}
				if task.timeout > 0 {
					var cancel context.CancelFunc
					task.ctx, cancel = context.WithTimeout(ctx, task.timeout)
					immediateResult = r.executeTask(task)
					cancel()
				} else {
					task.ctx = ctx
					immediateResult = r.executeTask(task)
				}
				limit.Release(opts.Limiter)
				immediate = true
				break
			}

			var nodeCtx context.Context
			var cancel context.CancelFunc
			if task.timeout > 0 {
				nodeCtx, cancel = context.WithTimeout(ctx, task.timeout)
			} else {
				nodeCtx, cancel = context.WithCancel(ctx)
			}
			task.ctx = nodeCtx
			task.call.Step = l.header.Steps
			task.revision = l.header.Revision
			delete(prepared, ref.id)
			if running == nil {
				running = make(map[taskRef]context.CancelFunc, workerLimit)
			}
			running[ref] = cancel
			if !ref.join {
				runningNodes++
				used++
				cursor = invocationNumber(ref.id)
			}
			if pool == nil {
				pool = &workerPool[S]{runner: r, limiter: opts.Limiter, capacity: workerLimit}
			}
			pool.dispatch(task, len(running))
		}
		if len(running) == 0 && !blocked && !immediate {
			if l.hasReadyNode() && (l.header.Steps == math.MaxUint64 || l.header.Revision == math.MaxUint64) {
				return finish(StatusFailed, ErrExecutionLimit)
			}
			if l.header.Completed {
				return finish(statusOf(l.header.Header(), l.readyNodes, false), nil)
			}
			if !l.hasReadyNode() && len(l.groups) > 0 && l.waitingInvocations == 0 {
				return finish(StatusFailed, invalidCheckpoint("unresolved execution has no runnable work"))
			}
			status := statusOf(l.header.Header(), l.readyNodes, used >= opts.MaxSteps)
			if status == StatusBudget {
				budgetReason = BudgetMaxSteps
			}
			return finish(status, nil)
		}
		var slots chan<- struct{}
		if blocked {
			slots = limit.Slots(opts.Limiter)
		}
		var out workResult[S]
		if immediate {
			out = immediateResult
		} else {
			var results <-chan workResult[S]
			if pool != nil {
				results = pool.results
			}
			select {
			case slots <- struct{}{}:
				reserved = true
				continue
			case <-ctx.Done():
				return finish(StatusCancelled, ctx.Err())
			case out = <-results:
			}
			running[out.ref]()
			delete(running, out.ref)
			if !out.ref.join {
				runningNodes--
			}
		}
		if err := ctx.Err(); err != nil {
			if l.valid(out.ref) {
				l.enqueue(out.ref)
			}
			return finish(StatusCancelled, err)
		}
		callID := out.callID
		if !l.valid(out.ref) {
			obs.DiscardCallback(ctx, callID)
			continue
		}
		obs.SelectCallback(callID)
		if isCallbackInterruption(out.err) {
			l.enqueue(out.ref)
			return finish(StatusInterrupted, out.err)
		}
		outcome := r.commitResult(ctx, l, out, opts.MaxInvocations, cursor, store, obs, prepared)
		if err := outcome.CommitErr; err != nil {
			l.enqueue(out.ref)
			if errors.Is(err, errInvocationBudget) {
				obs.ResolveSelected(ctx, model.OutcomeDiscarded, l.header.Revision, err)
				budgetReason = BudgetMaxInvocations
				return finish(StatusBudget, nil)
			}
			return finish(errorStatus(err), err)
		}
		reconcileTasks(l, prepared, running)
		if outcome.TerminalErr != nil {
			return finish(StatusFailed, outcome.TerminalErr)
		}
		if l.header.Completed {
			return finish(statusOf(l.header.Header(), l.readyNodes, false), nil)
		}
	}
}
