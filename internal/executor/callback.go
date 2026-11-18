package executor

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/afterlune/luneGraph/internal/limit"
	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

func panicAsError(callback, id string, recovered any) error {
	return &PanicError{Callback: callback, InvocationID: id, Value: recovered, Stack: debug.Stack()}
}

func (r *Runner[S]) cloneState(state S, id string) (out S, err error) {
	return callClone(r.clone, state, id)
}

func callClone[S any](clone Clone[S], state S, id string) (out S, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = panicAsError("clone", id, value)
		}
	}()
	return clone(state)
}

func (r *Runner[S]) runNode(ctx context.Context, call CallInfo, spec NodeSpec[S], state S) (out Transition[S], err error) {
	defer func() {
		if value := recover(); value != nil {
			err = panicAsError("node "+spec.Name, call.InvocationID, value)
		}
	}()
	out, err = spec.Run(ctx, call, state)
	if err == nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = ctx.Err()
	}
	return out, err
}

func (r *Runner[S]) mergeStates(ctx context.Context, call CallInfo, spec JoinSpec[S], parent S, branches []BranchResult[S]) (out S, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = panicAsError("join "+spec.Name, call.InvocationID, value)
		}
	}()
	out, err = spec.Merge(ctx, call, parent, branches)
	if err == nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = ctx.Err()
	}
	return out, err
}

func canMerge[S any](spec JoinSpec[S], call CallInfo, parent S, branches []BranchResult[S]) (ready bool, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = panicAsError("can-merge "+spec.Name, call.InvocationID, value)
		}
	}()
	return spec.CanMerge(parent, branches), nil
}

func (r *Runner[S]) decodeInput(id, key string, payload []byte) (out any, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = panicAsError("decode "+key, id, value)
		}
	}()
	c := r.continuation(key)
	if c == nil || c.Decode == nil {
		return nil, fmt.Errorf("unknown continuation %q", key)
	}
	return c.Decode(payload)
}

func (r *Runner[S]) applyInput(ctx context.Context, call CallInfo, key string, state S, value any) (out S, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicAsError("apply "+key, call.InvocationID, recovered)
		}
	}()
	c := r.continuation(key)
	if c == nil || c.Apply == nil {
		return state, fmt.Errorf("unknown continuation %q", key)
	}
	return c.Apply(ctx, call, state, value)
}

func panicStack(err error) string {
	if panicErr, ok := errors.AsType[*PanicError](err); ok {
		return string(panicErr.Stack)
	}
	return ""
}

// Only errors returned by effect-capable callbacks are control signals.
// PanicError unwraps its panic value, so exclude panics before checking the marker.
func isCallbackInterruption(err error) bool {
	if err == nil || !errors.Is(err, ErrInterrupted) {
		return false
	}
	var panicErr *PanicError
	return !errors.As(err, &panicErr)
}

func callbackEvent(operation model.EventOperation, revision uint64, call CallInfo, node, continuation string) model.Event {
	return model.Event{Operation: operation, Revision: revision, InvocationID: call.InvocationID, CallID: call.CallID, Node: node, Continuation: continuation}
}

func (r *Runner[S]) observedNode(ctx context.Context, obs *observation.Session, revision uint64, call CallInfo, spec NodeSpec[S], state S) (Transition[S], error) {
	span := obs.Begin(ctx, callbackEvent(model.OperationNode, revision, call, spec.Name, ""))
	out, err := r.runNode(ctx, call, spec, state)
	span.End(ctx, model.Event{Revision: revision, Action: model.ReadTransition(out).Action, Err: err})
	return out, err
}

func (r *Runner[S]) observedMerge(ctx context.Context, obs *observation.Session, revision uint64, call CallInfo, spec JoinSpec[S], parent S, branches []BranchResult[S]) (S, error) {
	if obs == nil {
		return r.mergeStates(ctx, call, spec, parent, branches)
	}
	span := obs.Begin(ctx, callbackEvent(model.OperationJoin, revision, call, spec.Name, ""))
	out, err := r.mergeStates(ctx, call, spec, parent, branches)
	span.End(ctx, model.Event{Revision: revision, Err: err})
	return out, err
}

func (r *Runner[S]) observedDecode(ctx context.Context, obs *observation.Session, revision uint64, inv Invocation[S], payload []byte) (any, error) {
	if obs == nil {
		return r.decodeInput(inv.ID, inv.Continuation, payload)
	}
	span := obs.Begin(ctx, callbackEvent(model.OperationDecode, revision, CallInfo{InvocationID: inv.ID}, inv.Node, inv.Continuation))
	out, err := r.decodeInput(inv.ID, inv.Continuation, payload)
	span.End(ctx, model.Event{Revision: revision, Err: err})
	return out, err
}

func (r *Runner[S]) observedApply(ctx context.Context, obs *observation.Session, revision uint64, call CallInfo, inv Invocation[S], state S, value any) (S, error) {
	if obs == nil {
		return r.applyInput(ctx, call, inv.Continuation, state, value)
	}
	span := obs.Begin(ctx, callbackEvent(model.OperationApply, revision, call, inv.Node, inv.Continuation))
	out, err := r.applyInput(ctx, call, inv.Continuation, state, value)
	span.End(ctx, model.Event{Revision: revision, Err: err})
	return out, err
}

func (r *Runner[S]) limitedApply(ctx context.Context, limiter *limit.Limiter, obs *observation.Session, revision uint64, call CallInfo, inv Invocation[S], state S, value any) (S, error) {
	if err := limit.Acquire(ctx, limiter); err != nil {
		var zero S
		return zero, err
	}
	defer limit.Release(limiter)
	return r.observedApply(ctx, obs, revision, call, inv, state, value)
}
