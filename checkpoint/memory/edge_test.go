package memory_test

import (
	"context"
	"errors"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/memory"
)

func TestNewRequiresClone(t *testing.T) {
	if _, err := memory.New[state](nil); err == nil {
		t.Fatal("New accepted a nil clone function")
	}
}

func TestStoreRejectsNilAndCanceledContextsAndNilReceiver(t *testing.T) {
	store, err := memory.New(func(value int) (int, error) { return value, nil })
	if err != nil {
		t.Fatal(err)
	}
	value := graph.Checkpoint[int]{FormatVersion: graph.CheckpointFormatVersion, RunID: "run", MachineID: "machine-v1", Revision: 1}
	next := value
	next.Revision = 2

	var nilStore *memory.Store[int]
	if err := nilStore.Create(context.Background(), value); err == nil {
		t.Fatal("nil store Create succeeded")
	}
	if _, err := nilStore.Load(context.Background(), "run"); err == nil {
		t.Fatal("nil store Load succeeded")
	}
	if err := nilStore.CompareAndSwap(context.Background(), 1, next); err == nil {
		t.Fatal("nil store CompareAndSwap succeeded")
	}

	var nilContext context.Context
	if err := store.Create(nilContext, value); err == nil {
		t.Fatal("Create accepted a nil context")
	}
	if _, err := store.Load(nilContext, "run"); err == nil {
		t.Fatal("Load accepted a nil context")
	}
	if err := store.CompareAndSwap(nilContext, 1, next); err == nil {
		t.Fatal("CompareAndSwap accepted a nil context")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Create(ctx, value); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Create = %v", err)
	}
	if _, err := store.Load(ctx, "run"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Load = %v", err)
	}
	if err := store.CompareAndSwap(ctx, 1, next); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CompareAndSwap = %v", err)
	}
}

func TestCloneFailuresDoNotWriteOrOverwrite(t *testing.T) {
	boom := errors.New("clone failed")
	fail := true
	store, err := memory.New(func(value state) (state, error) {
		if fail {
			return state{}, boom
		}
		return cloneState(value)
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	initial := stateCheckpoint("run", 1, 1)
	if err := store.Create(ctx, initial); !errors.Is(err, boom) {
		t.Fatalf("failed Create = %v", err)
	}
	fail = false
	if _, err := store.Load(ctx, "run"); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("failed Create wrote checkpoint: %v", err)
	}
	if err := store.Create(ctx, initial); err != nil {
		t.Fatal(err)
	}

	fail = true
	if _, err := store.Load(ctx, "run"); !errors.Is(err, boom) {
		t.Fatalf("failed Load clone = %v", err)
	}
	next := stateCheckpoint("run", 2, 2)
	if err := store.CompareAndSwap(ctx, 1, next); !errors.Is(err, boom) {
		t.Fatalf("failed CAS clone = %v", err)
	}
	fail = false
	stored, err := store.Load(ctx, "run")
	if err != nil || stored.Revision != 1 || stored.Invocations[0].State.Values["n"] != 1 {
		t.Fatalf("failed clone changed stored checkpoint = %+v, %v", stored, err)
	}
}

func TestCancellationDuringClonePreventsCreateAndCAS(t *testing.T) {
	var cancelOnClone context.CancelFunc
	cancelClone := false
	store, err := memory.New(func(value state) (state, error) {
		if cancelClone {
			cancelClone = false
			cancelOnClone()
		}
		return cloneState(value)
	})
	if err != nil {
		t.Fatal(err)
	}

	createCtx, cancelCreate := context.WithCancel(context.Background())
	cancelOnClone = cancelCreate
	cancelClone = true
	if err := store.Create(createCtx, stateCheckpoint("create", 1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create canceled during clone = %v", err)
	}
	if _, err := store.Load(context.Background(), "create"); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("canceled Create wrote checkpoint: %v", err)
	}

	if err := store.Create(context.Background(), stateCheckpoint("cas", 1, 1)); err != nil {
		t.Fatal(err)
	}
	casCtx, cancelCAS := context.WithCancel(context.Background())
	cancelOnClone = cancelCAS
	cancelClone = true
	if err := store.CompareAndSwap(casCtx, 1, stateCheckpoint("cas", 2, 2)); !errors.Is(err, context.Canceled) {
		t.Fatalf("CAS canceled during clone = %v", err)
	}
	stored, err := store.Load(context.Background(), "cas")
	if err != nil || stored.Revision != 1 {
		t.Fatalf("canceled CAS changed checkpoint = %+v, %v", stored, err)
	}
}

func stateCheckpoint(runID string, revision uint64, value int) graph.Checkpoint[state] {
	return graph.Checkpoint[state]{
		FormatVersion: graph.CheckpointFormatVersion,
		RunID:         runID,
		MachineID:     "machine-v1",
		Revision:      revision,
		Invocations:   []graph.Invocation[state]{{ID: "i1", State: state{Values: map[string]int{"n": value}}}},
	}
}
