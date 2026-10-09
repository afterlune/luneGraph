package executor

import (
	"context"

	"github.com/afterlune/luneGraph/internal/limit"
	"github.com/afterlune/luneGraph/internal/observation"
)

func (r *Runner[S]) limitedMerge(ctx context.Context, limiter *limit.Limiter, obs *observation.Session, revision uint64, call CallInfo, spec JoinSpec[S], states []S) (S, error) {
	if err := limit.Acquire(ctx, limiter); err != nil {
		var zero S
		return zero, err
	}
	defer limit.Release(limiter)
	return r.observedMerge(ctx, obs, revision, call, spec, states)
}

func (r *Runner[S]) limitedApply(ctx context.Context, limiter *limit.Limiter, obs *observation.Session, revision uint64, call CallInfo, inv Invocation[S], state S, value any) (S, error) {
	if err := limit.Acquire(ctx, limiter); err != nil {
		var zero S
		return zero, err
	}
	defer limit.Release(limiter)
	return r.observedApply(ctx, obs, revision, call, inv, state, value)
}
