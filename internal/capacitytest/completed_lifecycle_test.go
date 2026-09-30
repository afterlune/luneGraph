package capacitytest

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestCompletedPopulationRetentionAndRecovery(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			const count = 64
			r, store := compile(t, chainGraph(t, 2)), openStore(t, kind)
			var calls atomic.Int64
			observer := graph.ObserverFunc(func(_ context.Context, e graph.Event) {
				if e.Phase == graph.PhaseStarted && (e.Operation == graph.OperationNode || e.Operation == graph.OperationJoin || e.Operation == graph.OperationApply || e.Operation == graph.OperationDecode || e.Operation == graph.OperationCreate || e.Operation == graph.OperationCompareAndSwap) {
					calls.Add(1)
				}
			})
			opts := graph.Options[state]{Store: store, Observer: observer}
			before := resources(true)
			pool := newBatchPool(count, 8, func(i int) error {
				id := fmt.Sprintf("completed-%d", i)
				out, err := r.Start(context.Background(), id, initial(id), opts)
				if err != nil {
					return err
				}
				if out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || out.Checkpoint.Final.Owner != id || out.Checkpoint.Final.Total != 2 || out.Checkpoint.Final.Values["steps"] != 2 {
					return fmt.Errorf("invalid completed execution %s: %+v", id, out)
				}
				// Mutating the returned result must not contaminate the Store.
				out.Checkpoint.Final.Values["steps"] = -1
				return nil
			})
			err := pool.batch()
			pool.close()
			if err != nil {
				t.Fatal(err)
			}
			after := resources(true)
			t.Logf("store=%s retained_runs=%d callers=8 GC_heap_before=%d GC_heap_after=%d goroutines_before=%d goroutines_after=%d", kind, count, before.HeapBytes, after.HeapBytes, before.Goroutines, after.Goroutines)
			if got := calls.Load(); got != count*5 { // Create, two nodes, two CAS.
				t.Fatalf("operation count=%d want=%d", got, count*5)
			}
			for i := range count {
				id := fmt.Sprintf("completed-%d", i)
				saved, err := store.Load(context.Background(), id)
				if err != nil || !saved.Completed || saved.Final == nil || saved.Final.Owner != id || saved.Final.Values["steps"] != 2 || saved.Steps != 2 || saved.Revision != 3 || len(saved.Invocations) != 0 || len(saved.Groups) != 0 {
					t.Fatalf("retained %s: %+v, %v", id, saved, err)
				}
				for range 2 {
					out, err := r.Recover(context.Background(), id, nil, opts)
					if err != nil || out.Status != graph.StatusCompleted || !reflect.DeepEqual(out.Checkpoint, saved) {
						t.Fatalf("completed recovery %s changed checkpoint: %v", id, err)
					}
				}
			}
			if got := calls.Load(); got != count*5 {
				t.Fatalf("completed recovery ran callbacks or writes: operations=%d", got)
			}
		})
	}
}
