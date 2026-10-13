package executor

import (
	"context"
	"errors"
	"math"

	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

// commitResult builds and submits one scheduler outcome. Candidate errors never
// reach Store; terminal callback errors are returned only after a successful commit.
func (r *Runner[S]) commitResult(ctx context.Context, l *live[S], out workResult[S], maxInvocations int, cursor uint64, store Store[S], obs *observation.Session) (Change[S], error, error) {
	c, terminalErr, err := r.resultChange(l, out, maxInvocations)
	if err != nil {
		return Change[S]{}, nil, err
	}
	c.Header.ScheduleCursor = cursor
	if err := r.submit(ctx, l, c, store, obs); err != nil {
		return Change[S]{}, nil, err
	}
	return c, terminalErr, nil
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
