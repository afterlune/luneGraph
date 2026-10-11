package observation

import (
	"context"

	"github.com/afterlune/luneGraph/internal/model"
)

// WrapStore observes only actual Store calls, preserving errors and ownership.
func WrapStore[S any](store model.Store[S], session *Session) model.Store[S] {
	if store == nil || session == nil {
		return store
	}
	return &observedStore[S]{store: store, session: session}
}

type observedStore[S any] struct {
	store   model.Store[S]
	session *Session
}

func (s *observedStore[S]) Create(ctx context.Context, checkpoint model.Checkpoint[S]) error {
	span := s.session.Begin(ctx, model.Event{Operation: model.OperationCreate, Revision: checkpoint.Revision})
	err := s.store.Create(ctx, checkpoint)
	span.End(ctx, model.Event{Revision: checkpoint.Revision, Err: err})
	return err
}

func (s *observedStore[S]) Load(ctx context.Context, runID string) (model.Checkpoint[S], error) {
	span := s.session.Begin(ctx, model.Event{Operation: model.OperationLoad})
	checkpoint, err := s.store.Load(ctx, runID)
	var revision uint64
	if err == nil {
		revision = checkpoint.Revision
	}
	span.End(ctx, model.Event{Revision: revision, Err: err})
	return checkpoint, err
}

func (s *observedStore[S]) LoadHeader(ctx context.Context, runID string) (model.Header, error) {
	span := s.session.Begin(ctx, model.Event{Operation: model.OperationLoad})
	header, err := s.store.LoadHeader(ctx, runID)
	span.End(ctx, model.Event{Revision: header.Revision, Err: err})
	return header, err
}

func (s *observedStore[S]) Commit(ctx context.Context, expected uint64, change model.Change[S]) error {
	checkpoint := change.Checkpoint()
	span := s.session.Begin(ctx, model.Event{Operation: model.OperationCommit, Revision: checkpoint.Revision})
	err := s.store.Commit(ctx, expected, change)
	span.End(ctx, model.Event{Revision: checkpoint.Revision, Err: err})
	return err
}

func (s *observedStore[S]) Delete(ctx context.Context, runID string) error {
	span := s.session.Begin(ctx, model.Event{Operation: model.OperationDelete})
	err := s.store.Delete(ctx, runID)
	span.End(ctx, model.Event{Err: err})
	return err
}
