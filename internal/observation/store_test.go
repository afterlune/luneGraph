package observation

import (
	"context"
	"errors"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
)

type mockStore struct {
	created model.Checkpoint[int]
	loaded  model.Checkpoint[int]
	casRev  uint64
	casCp   model.Checkpoint[int]
	deleted string
}

func (m *mockStore) Create(_ context.Context, cp model.Checkpoint[int]) error {
	m.created = cp
	return nil
}

func (m *mockStore) Load(_ context.Context, runID string) (model.Checkpoint[int], error) {
	if runID == "missing" {
		return model.Checkpoint[int]{}, errors.New("not found")
	}
	m.loaded = model.Checkpoint[int]{RunID: runID, Revision: 5}
	return m.loaded, nil
}

func (m *mockStore) CompareAndSwap(_ context.Context, expected uint64, cp model.Checkpoint[int]) error {
	m.casRev = expected
	m.casCp = cp
	return nil
}

func (m *mockStore) Delete(_ context.Context, runID string) error {
	m.deleted = runID
	return nil
}

func (m *mockStore) DeleteMany(_ context.Context, ids []string) error {
	for _, id := range ids {
		m.deleted = id
	}
	return nil
}

type recordObserver struct {
	events []model.Event
}

func (r *recordObserver) Observe(_ context.Context, e model.Event) {
	r.events = append(r.events, e)
}

func TestObservedStore(t *testing.T) {
	// Nil checks
	if WrapStore[int](nil, nil) != nil {
		t.Fatal("expected nil store")
	}
	raw := &mockStore{}
	if WrapStore[int](raw, nil) != raw {
		t.Fatal("expected raw store")
	}

	obs := &recordObserver{}
	sess := New(obs, "m-1", "run-1")
	wrapped := WrapStore[int](raw, sess)

	ctx := context.Background()

	// 1. Create
	cp := model.Checkpoint[int]{RunID: "run-1", Revision: 1}
	if err := wrapped.Create(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if raw.created.RunID != "run-1" {
		t.Fatalf("create failed: %+v", raw.created)
	}

	// 2. Load success & failure
	loaded, err := wrapped.Load(ctx, "run-1")
	if err != nil || loaded.Revision != 5 {
		t.Fatalf("load failed: %+v, %v", loaded, err)
	}
	if _, err := wrapped.Load(ctx, "missing"); err == nil {
		t.Fatal("expected load error")
	}

	// 3. CompareAndSwap
	casCp := model.Checkpoint[int]{RunID: "run-1", Revision: 6}
	if err := wrapped.CompareAndSwap(ctx, 5, casCp); err != nil {
		t.Fatal(err)
	}
	if raw.casRev != 5 || raw.casCp.Revision != 6 {
		t.Fatalf("cas failed: rev=%d, cp=%+v", raw.casRev, raw.casCp)
	}

	// 4. Delete
	if err := wrapped.Delete(ctx, "run-1"); err != nil {
		t.Fatal(err)
	}
	if raw.deleted != "run-1" {
		t.Fatalf("delete failed: %s", raw.deleted)
	}

	// Verify events
	if len(obs.events) < 10 { // 5 operations * 2 phases = 10
		t.Fatalf("insufficient events: %d", len(obs.events))
	}
}
