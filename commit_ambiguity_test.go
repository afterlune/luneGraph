package graph_test

import (
	"context"
	"errors"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint/memory"
)

type ambiguousStore struct {
	*memory.Store[int]
	report error
}

func (s *ambiguousStore) CompareAndSwap(ctx context.Context, expected uint64, next graph.Checkpoint[int]) error {
	if err := s.Store.CompareAndSwap(ctx, expected, next); err != nil {
		return err
	}
	if s.report != nil {
		err := s.report
		s.report = nil
		return err
	}
	return nil
}

func TestStoreErrorAfterCommitRequiresReload(t *testing.T) {
	g := graph.New[int]("first")
	node(t, g, "first", func(_ context.Context, _ graph.CallInfo, value int) (graph.Transition[int], error) {
		return graph.To(value+1, "done"), nil
	})
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, value int) (graph.Transition[int], error) {
		return graph.EndExecution(value), nil
	})
	edge(t, g, "first", "done")
	r := intRunner(t, g)
	underlying := newMemoryStore(t)
	boom := errors.New("commit acknowledgement lost")
	store := &ambiguousStore{Store: underlying, report: boom}
	returned, err := r.Start(context.Background(), "ambiguous", 0, graph.Options[int]{Store: store})
	if !errors.Is(err, boom) || returned.Checkpoint.Revision != 1 {
		t.Fatalf("returned checkpoint = %+v, %v", returned, err)
	}
	loaded, err := store.Load(context.Background(), "ambiguous")
	if err != nil || loaded.Revision != 2 || loaded.Invocations[0].Node != "done" {
		t.Fatalf("committed checkpoint = %+v, %v", loaded, err)
	}
	completed, err := r.Resume(context.Background(), loaded, nil, graph.Options[int]{Store: store})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Checkpoint.Final == nil || *completed.Checkpoint.Final != 1 {
		t.Fatalf("recovered execution = %+v, %v", completed, err)
	}
}
