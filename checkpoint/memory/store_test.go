package memory_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/memory"
	"github.com/afterlune/luneGraph/internal/storetest"
)

type state struct{ Values map[string]int }

func cloneState(s state) (state, error) {
	values := make(map[string]int, len(s.Values))
	for key, value := range s.Values {
		values[key] = value
	}
	return state{Values: values}, nil
}

func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) graph.Store[storetest.State] {
		store, err := memory.New(storetest.CloneState)
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}

func TestStoreCopiesAndComparesCheckpoints(t *testing.T) {
	store, err := memory.New(cloneState)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	initial := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "run", MachineID: "machine-v1", Revision: 1, Invocations: []graph.Invocation[state]{{ID: "i1", State: state{Values: map[string]int{"n": 1}}}}}
	if err := store.Create(ctx, initial); err != nil {
		t.Fatal(err)
	}
	initial.Invocations[0].State.Values["n"] = 99
	loaded, err := store.Load(ctx, "run")
	if err != nil || loaded.Invocations[0].State.Values["n"] != 1 {
		t.Fatalf("load = %+v, %v", loaded, err)
	}
	loaded.Invocations[0].State.Values["n"] = 88
	again, err := store.Load(ctx, "run")
	if err != nil || again.Invocations[0].State.Values["n"] != 1 {
		t.Fatalf("second load = %+v, %v", again, err)
	}
	again.Revision = 2
	again.Invocations[0].State.Values["n"] = 2
	if err := store.CompareAndSwap(ctx, 1, again); err != nil {
		t.Fatal(err)
	}
	if err := store.CompareAndSwap(ctx, 1, again); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("stale write = %v", err)
	}
	otherMachine := again
	otherMachine.Revision = 3
	otherMachine.MachineID = "other"
	if err := store.CompareAndSwap(ctx, 2, otherMachine); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("different machine = %v", err)
	}
	if err := store.Create(ctx, initial); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("duplicate create = %v", err)
	}
	if _, err := store.Load(ctx, "missing"); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("missing load = %v", err)
	}
	stored, err := store.Load(ctx, "run")
	if err != nil || stored.Revision != 2 || stored.Invocations[0].State.Values["n"] != 2 {
		t.Fatalf("stored = %+v, %v", stored, err)
	}
}

func TestConcurrentCompareAndSwap(t *testing.T) {
	store, err := memory.New(func(v int) (int, error) { return v, nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Create(ctx, graph.Checkpoint[int]{FormatVersion: graph.CheckpointFormatVersion, RunID: "run", MachineID: "machine-v1", Revision: 1}); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for value := range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			results <- store.CompareAndSwap(ctx, 1, graph.Checkpoint[int]{FormatVersion: graph.CheckpointFormatVersion, RunID: "run", MachineID: "machine-v1", Revision: 2, Steps: uint64(value + 1)})
		}()
	}
	workers.Wait()
	close(results)
	var succeeded, conflicted int
	for err := range results {
		if err == nil {
			succeeded++
		} else if errors.Is(err, graph.ErrConflict) {
			conflicted++
		} else {
			t.Fatalf("unexpected CAS error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("CAS outcomes: %d success, %d conflict", succeeded, conflicted)
	}
}

func TestStoreEdgeCases(t *testing.T) {
	if _, err := memory.New[int](nil); err == nil {
		t.Fatal("expected error with nil clone")
	}

	var nilStore *memory.Store[int]
	ctx := context.Background()
	cp := graph.Checkpoint[int]{FormatVersion: graph.CheckpointFormatVersion, RunID: "run-nil", MachineID: "m-1", Revision: 1}

	if err := nilStore.Create(ctx, cp); err == nil {
		t.Fatal("expected error on Create with nil store")
	}
	if _, err := nilStore.Load(ctx, "run-nil"); err == nil {
		t.Fatal("expected error on Load with nil store")
	}
	if err := nilStore.CompareAndSwap(ctx, 1, cp); err == nil {
		t.Fatal("expected error on CAS with nil store")
	}
	if err := nilStore.Delete(ctx, "run-nil"); err == nil {
		t.Fatal("expected error on Delete with nil store")
	}

	store, err := memory.New[int](func(v int) (int, error) { return v, nil })
	if err != nil {
		t.Fatal(err)
	}

	// nil ctx
	if err := store.Delete(nil, "run-1"); err == nil {
		t.Fatal("expected error with nil ctx")
	}
	// invalid runID
	if err := store.Delete(ctx, ""); err == nil {
		t.Fatal("expected error with empty runID")
	}
	if err := store.Delete(ctx, "  spaces  "); err == nil {
		t.Fatal("expected error with spaces in runID")
	}

	// canceled ctx
	cancCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.Delete(cancCtx, "run-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}
