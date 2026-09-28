// Package storetest checks the shared contract of checkpoint stores.
package storetest

import (
	"context"
	"errors"
	"math"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint"
)

type State struct {
	Values map[string]int
}

func CloneState(value State) (State, error) {
	copy := State{Values: make(map[string]int, len(value.Values))}
	for key, item := range value.Values {
		copy.Values[key] = item
	}
	return copy, nil
}

// Run applies the same valid-input and conflict guarantees to each Store.
// open must return a fresh store for every subtest and register its cleanup.
func Run(t *testing.T, open func(*testing.T) graph.Store[State]) {
	t.Helper()
	t.Run("copies", func(t *testing.T) {
		store := open(t)
		ctx := context.Background()
		initial := fixture()
		if err := store.Create(ctx, initial); err != nil {
			t.Fatal(err)
		}
		initial.Invocations[0].State.Values["n"] = 99
		initial.Invocations[0].Next[0] = "changed"
		initial.Groups[0].Children[0] = "changed"
		initial.Terminals[0].State.Values["n"] = 99
		initial.Final.Values["n"] = 99
		loaded, err := store.Load(ctx, "run")
		if err != nil {
			t.Fatal(err)
		}
		assertFixture(t, loaded, 1)
		loaded.Invocations[0].State.Values["n"] = 88
		loaded.Invocations[0].Next[0] = "changed"
		loaded.Groups[0].Children[0] = "changed"
		loaded.Terminals[0].State.Values["n"] = 88
		loaded.Final.Values["n"] = 88
		again, err := store.Load(ctx, "run")
		if err != nil {
			t.Fatal(err)
		}
		assertFixture(t, again, 1)
		again.Revision = 2
		again.Invocations[0].State.Values["n"] = 2
		if err := store.CompareAndSwap(ctx, 1, again); err != nil {
			t.Fatal(err)
		}
		again.Invocations[0].State.Values["n"] = 99
		stored, err := store.Load(ctx, "run")
		if err != nil || stored.Revision != 2 || stored.Invocations[0].State.Values["n"] != 2 {
			t.Fatalf("CAS copy = %+v, %v", stored, err)
		}
	})

	t.Run("conflicts", func(t *testing.T) {
		store := open(t)
		ctx := context.Background()
		initial := fixture()
		if _, err := store.Load(ctx, "run"); !errors.Is(err, checkpoint.ErrNotFound) {
			t.Fatalf("missing run = %v", err)
		}
		if err := store.Create(ctx, initial); err != nil {
			t.Fatal(err)
		}
		duplicate := fixture()
		duplicate.Steps = 99
		if err := store.Create(ctx, duplicate); !errors.Is(err, graph.ErrConflict) {
			t.Fatalf("duplicate run = %v", err)
		}
		next := fixture()
		next.Revision = 2
		next.Steps = 1
		if err := store.CompareAndSwap(ctx, 2, next); !errors.Is(err, graph.ErrConflict) {
			t.Fatalf("stale revision = %v", err)
		}
		next.MachineID = "other-machine"
		if err := store.CompareAndSwap(ctx, 1, next); !errors.Is(err, graph.ErrConflict) {
			t.Fatalf("wrong machine = %v", err)
		}
		stored, err := store.Load(ctx, "run")
		if err != nil || stored.Revision != 1 || stored.Steps != 0 {
			t.Fatalf("conflict changed checkpoint = %+v, %v", stored, err)
		}
	})

	t.Run("invalid input", func(t *testing.T) {
		store := open(t)
		ctx := context.Background()
		for _, change := range []func(*graph.Checkpoint[State]){
			func(value *graph.Checkpoint[State]) { value.RunID = "" },
			func(value *graph.Checkpoint[State]) { value.MachineID = " other " },
			func(value *graph.Checkpoint[State]) { value.FormatVersion++ },
			func(value *graph.Checkpoint[State]) { value.Revision = 0 },
			func(value *graph.Checkpoint[State]) { value.Revision = 2 },
		} {
			value := fixture()
			change(&value)
			if err := store.Create(ctx, value); !errors.Is(err, graph.ErrInvalidCheckpoint) {
				t.Fatalf("invalid create = %v", err)
			}
		}
		if _, err := store.Load(ctx, "run"); !errors.Is(err, checkpoint.ErrNotFound) {
			t.Fatalf("invalid create wrote run: %v", err)
		}
		if err := store.Create(ctx, fixture()); err != nil {
			t.Fatal(err)
		}
		next := fixture()
		next.Revision = 2
		next.FormatVersion++
		if err := store.CompareAndSwap(ctx, 1, next); !errors.Is(err, graph.ErrInvalidCheckpoint) {
			t.Fatalf("invalid CAS header = %v", err)
		}
		next.FormatVersion = graph.CheckpointFormatVersion
		next.Revision = 3
		if err := store.CompareAndSwap(ctx, 1, next); !errors.Is(err, graph.ErrConflict) {
			t.Fatalf("skipped revision = %v", err)
		}
		next.Revision = math.MaxUint64
		if err := store.CompareAndSwap(ctx, math.MaxUint64, next); !errors.Is(err, graph.ErrExecutionLimit) {
			t.Fatalf("exhausted revision = %v", err)
		}
		stored, err := store.Load(ctx, "run")
		if err != nil || stored.Revision != 1 {
			t.Fatalf("invalid CAS changed checkpoint = %+v, %v", stored, err)
		}
	})
}

func fixture() graph.Checkpoint[State] {
	// Store copy tests populate every reference-bearing field; this is not a
	// resumable runner checkpoint.
	return graph.Checkpoint[State]{
		FormatVersion: graph.CheckpointFormatVersion,
		RunID:         "run",
		MachineID:     "machine-v1",
		Revision:      1,
		Final:         &State{Values: map[string]int{"n": 1}},
		Invocations:   []graph.Invocation[State]{{ID: "i1", CallID: "c2", State: State{Values: map[string]int{"n": 1}}, Next: []string{"next"}}},
		Groups:        []graph.ActivationGroup{{ID: "g1", CallID: "c3", Children: []string{"i1"}}},
		Terminals:     []graph.Terminal[State]{{InvocationID: "i1", State: State{Values: map[string]int{"n": 1}}}},
	}
}

func assertFixture(t *testing.T, value graph.Checkpoint[State], revision uint64) {
	t.Helper()
	if value.Revision != revision || value.Invocations[0].CallID != "c2" || value.Groups[0].CallID != "c3" || value.Invocations[0].State.Values["n"] != 1 || value.Invocations[0].Next[0] != "next" || value.Groups[0].Children[0] != "i1" || value.Terminals[0].State.Values["n"] != 1 || value.Final.Values["n"] != 1 {
		t.Fatalf("stored checkpoint was mutated: %+v", value)
	}
}
