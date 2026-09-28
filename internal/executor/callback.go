package executor

import (
	"context"
	"errors"
	"runtime/debug"
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
	return spec.Run(ctx, call, state)
}

func (r *Runner[S]) mergeStates(ctx context.Context, call CallInfo, spec JoinSpec[S], states []S) (out S, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = panicAsError("join "+spec.Name, call.InvocationID, value)
		}
	}()
	return spec.Merge(ctx, call, states)
}

func (r *Runner[S]) decodeInput(id, key string, payload []byte) (out any, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = panicAsError("decode "+key, id, value)
		}
	}()
	return r.continuations[key].Decode(payload)
}

func (r *Runner[S]) applyInput(ctx context.Context, call CallInfo, key string, state S, value any) (out S, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicAsError("apply "+key, call.InvocationID, recovered)
		}
	}()
	return r.continuations[key].Apply(ctx, call, state, value)
}

func panicStack(err error) string {
	var panicErr *PanicError
	if errors.As(err, &panicErr) {
		return string(panicErr.Stack)
	}
	return ""
}
