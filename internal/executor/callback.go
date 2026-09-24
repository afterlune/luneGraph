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

func (r *Runner[S]) runNode(ctx context.Context, id string, spec NodeSpec[S], state S) (out Transition[S], err error) {
	defer func() {
		if value := recover(); value != nil {
			err = panicAsError("node "+spec.Name, id, value)
		}
	}()
	return spec.Run(ctx, state)
}

func (r *Runner[S]) mergeStates(ctx context.Context, id string, spec JoinSpec[S], states []S) (out S, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = panicAsError("join "+spec.Name, id, value)
		}
	}()
	return spec.Merge(ctx, states)
}

func (r *Runner[S]) decodeInput(id, key string, payload []byte) (out any, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = panicAsError("decode "+key, id, value)
		}
	}()
	return r.continuations[key].Decode(payload)
}

func (r *Runner[S]) applyInput(ctx context.Context, id, key string, state S, value any) (out S, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicAsError("apply "+key, id, recovered)
		}
	}()
	return r.continuations[key].Apply(ctx, state, value)
}

func panicStack(err error) string {
	var panicErr *PanicError
	if errors.As(err, &panicErr) {
		return string(panicErr.Stack)
	}
	return ""
}
