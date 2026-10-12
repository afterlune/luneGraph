package executor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/afterlune/luneGraph/internal/limit"
	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

type workResult[S any] struct {
	ref        taskRef
	callID     string
	transition Transition[S]
	state      S
	err        error
}
type workerTask[S any] struct {
	ctx      context.Context
	ref      taskRef
	call     CallInfo
	spec     NodeSpec[S]
	join     JoinSpec[S]
	timeout  time.Duration
	state    S
	branches []BranchResult[S]
	obs      *observation.Session
	revision uint64
}

func (r *Runner[S]) driveLive(ctx context.Context, l *live[S], opts Options, obs *observation.Session, store Store[S]) (Result[S], error) {
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	workerLimit := limit.Capacity(opts.Limiter, opts.MaxConcurrency)
	results := make(chan workResult[S], workerLimit)
	tasks := make(chan workerTask[S], workerLimit)
	running := make(map[taskRef]context.CancelFunc)
	var prepared map[taskRef]workerTask[S]
	var workers sync.WaitGroup
	workerCount := 0
	startWorker := func() {
		workerCount++
		workers.Add(1)
		go func() {
			defer workers.Done()
			for t := range tasks {
				out := r.executeTask(t)
				limit.Release(opts.Limiter)
				results <- out
			}
		}()
	}
	defer func() {
		stop()
		for _, cancel := range running {
			cancel()
		}
		close(tasks)
		workers.Wait()
	}()
	drain := func() {
		stop()
		for _, cancel := range running {
			cancel()
		}
		for len(running) > 0 {
			out := <-results
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
		result, copyErr := r.resultFromLive(l, status)
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
				c, terminalErr, err := r.resultChange(l, workResult[S]{ref: ref}, opts.MaxInvocations)
				if err != nil {
					l.enqueue(ref)
					return finish(errorStatus(err), err)
				}
				c.Header.ScheduleCursor = cursor
				if err := r.submit(ctx, l, c, store, obs); err != nil {
					l.enqueue(ref)
					return finish(errorStatus(err), err)
				}
				if terminalErr != nil {
					return finish(StatusFailed, terminalErr)
				}
				if l.header.Completed {
					return finish(statusOf(l.header.Header(), l.readyNodes, false), nil)
				}
				for pending, cancel := range running {
					if !l.valid(pending) {
						cancel()
					}
				}
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
			if len(running) > workerCount {
				startWorker()
			}
			tasks <- task
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
			return finish(statusOf(l.header.Header(), l.readyNodes, used >= opts.MaxSteps), nil)
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
			case out = <-results:
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
		c, terminalErr, err := r.resultChange(l, out, opts.MaxInvocations)
		if err != nil {
			l.enqueue(out.ref)
			if errors.Is(err, errInvocationBudget) {
				obs.ResolveSelected(ctx, model.OutcomeDiscarded, l.header.Revision, err)
				return finish(StatusBudget, nil)
			}
			return finish(errorStatus(err), err)
		}
		c.Header.ScheduleCursor = cursor
		if err := r.submit(ctx, l, c, store, obs); err != nil {
			l.enqueue(out.ref)
			return finish(errorStatus(err), err)
		}
		for ref := range prepared {
			if !l.valid(ref) {
				delete(prepared, ref)
			}
		}
		for _, v := range c.UpsertInvocations {
			if v.GroupID != "" {
				delete(prepared, taskRef{id: v.GroupID, join: true})
			}
		}
		for ref, cancel := range running {
			if !l.valid(ref) {
				cancel()
			}
		}
		if terminalErr != nil {
			return finish(StatusFailed, terminalErr)
		}
		if l.header.Completed {
			return finish(statusOf(l.header.Header(), l.readyNodes, false), nil)
		}
	}
}

func (r *Runner[S]) executeTask(t workerTask[S]) workResult[S] {
	out := workResult[S]{ref: t.ref, callID: t.call.CallID}
	if t.ref.join {
		if t.join.Merge != nil {
			out.state, out.err = r.observedMerge(t.ctx, t.obs, t.revision, t.call, t.join, t.state, t.branches)
		}
	} else if t.obs == nil {
		out.transition, out.err = r.runNode(t.ctx, t.call, t.spec, t.state)
	} else {
		out.transition, out.err = r.observedNode(t.ctx, t.obs, t.revision, t.call, t.spec, t.state)
	}
	return out
}

func (r *Runner[S]) prepareTask(ctx context.Context, l *live[S], ref taskRef, obs *observation.Session) (workerTask[S], error) {
	t := workerTask[S]{ctx: ctx, ref: ref, obs: obs, revision: l.header.Revision}
	if ref.join {
		g := l.groups[ref.id]
		parent, _ := l.invocation(g.ParentID)
		parentState, err := r.cloneState(parent.State, parent.ID)
		if err != nil {
			return t, fmt.Errorf("clone join parent: %w", err)
		}
		t.state = parentState
		t.join = r.joins[g.JoinNode]
		t.timeout = t.join.Timeout
		t.call = CallInfo{RunID: l.header.RunID, InvocationID: parent.ID, CallID: g.CallID, Node: g.JoinNode, Step: l.header.Steps, BranchIndex: parent.BranchIndex}
		t.branches = make([]BranchResult[S], 0, len(g.Branches))
		for branchIndex, branch := range g.Branches {
			id := branch.InvocationID
			child, _ := l.invocation(id)
			res := BranchResult[S]{
				Index:  branchIndex,
				Target: branch.Target,
				Node:   child.Node,
				Status: child.Status,
				Error:  branch.Error,
			}
			if child.Status == InvocationJoined {
				value, err := r.cloneState(child.State, id)
				if err != nil {
					return t, fmt.Errorf("clone join input: %w", err)
				}
				res.State = value
			}
			t.branches = append(t.branches, res)
		}
	} else {
		v, _ := l.invocation(ref.id)
		state, err := r.cloneState(v.State, v.ID)
		if err != nil {
			return t, fmt.Errorf("clone node state: %w", err)
		}
		t.state = state
		t.spec = r.nodes[v.Node]
		if v.GroupID != "" {
			if activation, ok := l.groups[v.GroupID]; ok && v.BranchIndex >= 0 && v.BranchIndex < len(activation.Branches) {
				t.timeout = activation.Branches[v.BranchIndex].Timeout
			}
		}
		if t.timeout <= 0 {
			t.timeout = t.spec.Timeout
		}
		t.call = CallInfo{RunID: l.header.RunID, InvocationID: v.ID, CallID: v.CallID, Node: v.Node, Step: l.header.Steps, BranchIndex: v.BranchIndex}
	}
	return t, nil
}

func (r *Runner[S]) submit(ctx context.Context, l *live[S], c Change[S], store Store[S], obs *observation.Session) error {
	if err := ctx.Err(); err != nil {
		obs.ResolveSelected(ctx, model.OutcomeDiscarded, 0, err)
		return err
	}
	if l.header.Revision == math.MaxUint64 {
		obs.ResolveSelected(ctx, model.OutcomeDiscarded, 0, ErrExecutionLimit)
		return ErrExecutionLimit
	}
	c.Header.Revision = l.header.Revision + 1
	l.complete(&c)
	if store != nil {
		if err := store.Commit(ctx, l.header.Revision, c); err != nil {
			outcome := model.OutcomeUnknown
			if errors.Is(err, ErrConflict) {
				outcome = model.OutcomeDiscarded
			}
			obs.ResolveSelected(ctx, outcome, c.Header.Revision, err)
			return err
		}
	}
	l.install(c)
	obs.ResolveSelected(ctx, model.OutcomeCommitted, c.Header.Revision, nil)
	return nil
}
