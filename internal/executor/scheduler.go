package executor

import (
	"context"
	"errors"
	"fmt"
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
				terminalErr, err := r.commitResult(ctx, l, workResult[S]{ref: ref}, opts.MaxInvocations, cursor, store, obs, prepared)
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

			// Tight inline fast-path for linear root node sequences without persistence/observation/limiter.
			if !ref.join && store == nil && obs == nil && opts.Limiter == nil && len(running) == 0 && l.joinQueue.len() == 0 && l.nodeQueue.len() == 0 {
				if inv, slot, ok := l.invocation(ref.id, ref.slot); ok && inv.GroupID == "" {
					var n *CompiledNode[S]
					if uint(ref.nodeIdx) < uint(len(r.nodes)) && r.nodes[ref.nodeIdx].Spec.Name == inv.Node {
						n = &r.nodes[ref.nodeIdx]
					} else {
						n = r.node(inv.Node)
					}
					if n != nil && n.Spec.Timeout <= 0 {
						invCursor := invocationNumber(inv.ID)
						for {
							if l.header.Revision == math.MaxUint64 || l.header.Steps == math.MaxUint64 {
								l.nodeQueue.push(taskRef{id: inv.ID, slot: slot, nodeIdx: n.Index, join: false})
								return finish(StatusFailed, ErrExecutionLimit)
							}
							if err := ctx.Err(); err != nil {
								l.nodeQueue.push(taskRef{id: inv.ID, slot: slot, nodeIdx: n.Index, join: false})
								return finish(StatusCancelled, err)
							}
							state, err := r.cloneState(inv.State, inv.ID)
							if err != nil {
								l.nodeQueue.push(taskRef{id: inv.ID, slot: slot, nodeIdx: n.Index, join: false})
								outErr := fmt.Errorf("clone node state: %w", err)
								return finish(errorStatus(outErr), outErr)
							}
							call := CallInfo{
								RunID:        l.header.RunID,
								InvocationID: inv.ID,
								CallID:       inv.CallID,
								Node:         inv.Node,
								Step:         l.header.Steps,
								BranchIndex:  inv.BranchIndex,
							}
							used++
							cursor = invCursor
							tr, runErr := r.runNode(ctx, call, n.Spec, state)
							if err := ctx.Err(); err != nil {
								l.nodeQueue.push(taskRef{id: inv.ID, slot: slot, nodeIdx: n.Index, join: false})
								return finish(StatusCancelled, err)
							}
							if runErr == nil {
								d := model.ReadTransition(tr)
								if d.Action == ActionContinue && len(d.Targets) == 0 && d.Target != "" {
									edges := r.edges[n.EdgeStart:n.EdgeEnd]
									edgeIdx := -1
									for i := range edges {
										if edges[i].Target == d.Target {
											edgeIdx = i
											break
										}
									}
									if edgeIdx >= 0 {
										edge := edges[edgeIdx]
										if edge.JoinIndex < 0 && edge.NodeIndex >= 0 && l.header.NextID < math.MaxUint64 {
											inv.State = d.State
											inv.Continuation = ""
											inv.Next = nil
											inv.Status = InvocationReady
											inv.CallID = model.AllocID(&l.header.NextID, "c")
											inv.Node = edge.Target
											l.header.Revision++
											l.header.Steps++
											l.header.ScheduleCursor = cursor
											n = &r.nodes[edge.NodeIndex]
											if used < opts.MaxSteps && n.Spec.Timeout <= 0 {
												continue
											}
											l.nodeQueue.push(taskRef{id: inv.ID, slot: slot, nodeIdx: edge.NodeIndex, join: false})
											break
										}
									}
								} else if d.Action == ActionEndExecution {
									l.finalVal = d.State
									l.header.Revision++
									l.header.Steps++
									l.header.ScheduleCursor = cursor
									l.header.Final = &l.finalVal
									l.header.Completed = true
									l.clearActive()
									return finish(statusOf(l.header.Header(), 0, false), nil)
								}
							}
							immediateResult = workResult[S]{
								ref:        taskRef{id: inv.ID, slot: slot, nodeIdx: n.Index, join: false},
								callID:     inv.CallID,
								node:       n,
								transition: tr,
								err:        runErr,
							}
							immediate = true
							break
						}
						break
					}
				}
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
		terminalErr, err := r.commitResult(ctx, l, out, opts.MaxInvocations, cursor, store, obs, prepared)
		if err != nil {
			l.enqueue(out.ref)
			if errors.Is(err, errInvocationBudget) {
				obs.ResolveSelected(ctx, model.OutcomeDiscarded, l.header.Revision, err)
				budgetReason = BudgetMaxInvocations
				return finish(StatusBudget, nil)
			}
			return finish(errorStatus(err), err)
		}
		reconcileTasks(l, prepared, running)
		if terminalErr != nil {
			return finish(StatusFailed, terminalErr)
		}
		if l.header.Completed {
			return finish(statusOf(l.header.Header(), l.readyNodes, false), nil)
		}
	}
}
