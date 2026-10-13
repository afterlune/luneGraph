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
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	workerLimit := limit.Capacity(opts.Limiter, opts.MaxConcurrency)
	pool := workerPool[S]{runner: r, limiter: opts.Limiter, tasks: make(chan workerTask[S], workerLimit), results: make(chan workResult[S], workerLimit)}
	running := make(map[taskRef]context.CancelFunc)
	var prepared map[taskRef]workerTask[S]
	defer func() {
		stop()
		for _, cancel := range running {
			cancel()
		}
		pool.close()
	}()
	var budgetReason BudgetReason
	drain := func() {
		stop()
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
			runningNodes := uint64(0)
			for ref := range running {
				if !ref.join {
					runningNodes++
				}
			}
			ref, ok := l.next(used < opts.MaxSteps && runningNodes < math.MaxUint64-l.header.Steps, running)
			if !ok {
				break
			}
			if ref.join && l.groups[ref.id].JoinNode == "" {
				_, terminalErr, err := r.commitResult(ctx, l, workResult[S]{ref: ref}, opts.MaxInvocations, cursor, store, obs)
				if err != nil {
					l.enqueue(ref)
					return finish(errorStatus(err), err)
				}
				if terminalErr != nil {
					return finish(StatusFailed, terminalErr)
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
				task, cached = prepared[ref]
				if !cached {
					if reserved {
						limit.Release(opts.Limiter)
						reserved = false
					}
					task, err = r.prepareTask(runCtx, l, ref, obs)
					if err != nil {
						l.enqueue(ref)
						return finish(errorStatus(err), err)
					}
					if !task.ready {
						delete(l.joinEnqueued, ref.id)
						continue
					}
					if task.preflightErr != nil {
						immediateResult = workResult[S]{ref: ref, callID: task.call.CallID, err: task.preflightErr}
						running[ref] = func() {}
						immediate = true
						break
					}
					if prepared == nil {
						prepared = make(map[taskRef]workerTask[S])
					}
					prepared[ref] = task
				}
			}
			if !reserved && !limit.TryAcquire(opts.Limiter) {
				l.enqueue(ref)
				blocked = true
				break
			}
			reserved = false
			if !ref.join {
				task, err = r.prepareTask(runCtx, l, ref, obs)
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

			var nodeCtx context.Context
			var cancel context.CancelFunc
			if task.timeout > 0 {
				nodeCtx, cancel = context.WithTimeout(runCtx, task.timeout)
			} else {
				nodeCtx, cancel = context.WithCancel(runCtx)
			}
			task.ctx = nodeCtx
			task.call.Step = l.header.Steps
			task.revision = l.header.Revision
			delete(prepared, ref)
			running[ref] = cancel
			if !ref.join {
				used++
				cursor = invocationNumber(ref.id)
			}
			// A lone root path has no other callback to schedule alongside it.
			// Execute it on the driver to avoid a worker round trip per step.
			if !ref.join && l.invocationCount() == 1 && len(l.groups) == 0 {
				immediateResult = r.executeTask(task)
				limit.Release(opts.Limiter)
				immediate = true
				break
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
			select {
			case slots <- struct{}{}:
				reserved = true
				continue
			case <-ctx.Done():
				return finish(StatusCancelled, ctx.Err())
			case out = <-pool.results:
			}
		}
		running[out.ref]()
		delete(running, out.ref)
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
		c, terminalErr, err := r.commitResult(ctx, l, out, opts.MaxInvocations, cursor, store, obs)
		if err != nil {
			l.enqueue(out.ref)
			if errors.Is(err, errInvocationBudget) {
				obs.ResolveSelected(ctx, model.OutcomeDiscarded, l.header.Revision, err)
				budgetReason = BudgetMaxInvocations
				return finish(StatusBudget, nil)
			}
			return finish(errorStatus(err), err)
		}
		reconcileTasks(l, c, prepared, running)
		if terminalErr != nil {
			return finish(StatusFailed, terminalErr)
		}
		if l.header.Completed {
			return finish(statusOf(l.header.Header(), l.readyNodes, false), nil)
		}
	}
}
