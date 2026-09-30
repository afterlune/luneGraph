package executor

import (
	"context"

	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

func callbackEvent(operation model.EventOperation, revision uint64, call CallInfo, node, continuation string) model.Event {
	return model.Event{Operation: operation, Revision: revision, InvocationID: call.InvocationID, CallID: call.CallID, Node: node, Continuation: continuation}
}

func (r *Runner[S]) observedNode(ctx context.Context, obs *observation.Session, revision uint64, call CallInfo, spec NodeSpec[S], state S) (Transition[S], error) {
	span := obs.Begin(ctx, callbackEvent(model.OperationNode, revision, call, spec.Name, ""))
	out, err := r.runNode(ctx, call, spec, state)
	span.End(ctx, model.Event{Revision: revision, Action: out.Action, Err: err})
	return out, err
}

func (r *Runner[S]) observedMerge(ctx context.Context, obs *observation.Session, revision uint64, call CallInfo, spec JoinSpec[S], states []S) (S, error) {
	if obs == nil {
		return r.mergeStates(ctx, call, spec, states)
	}
	span := obs.Begin(ctx, callbackEvent(model.OperationJoin, revision, call, spec.Name, ""))
	out, err := r.mergeStates(ctx, call, spec, states)
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
