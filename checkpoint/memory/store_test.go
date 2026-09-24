package memory_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/memory"
)

type state struct{ Values map[string]int }

func cloneState(s state) (state, error) {
	values := make(map[string]int, len(s.Values))
	for key, value := range s.Values {
		values[key] = value
	}
	return state{Values: values}, nil
}

func TestStoreCopiesAndComparesCheckpoints(t *testing.T) {
	store, err := memory.New(cloneState)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	initial := graph.Checkpoint[state]{RunID: "run", MachineID: "machine-v1", Revision: 1, Invocations: []graph.Invocation[state]{{ID: "i1", State: state{Values: map[string]int{"n": 1}}}}}
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
	if err := store.Create(ctx, graph.Checkpoint[int]{RunID: "run", Revision: 1}); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for value := range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			results <- store.CompareAndSwap(ctx, 1, graph.Checkpoint[int]{RunID: "run", Revision: 2, Steps: uint64(value + 1)})
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
