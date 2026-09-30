package capacitytest

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
)

// Inspect a waiting run after a fixed number of branch-outcome rounds. Seeding and encoding are
// outside timing; repeated recovery neither writes nor grows the checkpoint.
func BenchmarkCapacityHistory(b *testing.B) {
	for _, kind := range []string{"none", "memory", "sqlite"} {
		for _, mode := range []string{"terminal", "failure", "mixed"} {
			for _, rounds := range []int{32, 128, 512} {
				b.Run(fmt.Sprintf("store=%s/mode=%s/rounds=%d", kind, mode, rounds), func(b *testing.B) {
					r, store := historyRunner(b, mode), openStore(b, kind)
					before := resources(true)
					opts := graph.Options[state]{Store: store, MaxSteps: historyStepsPerRound, MaxConcurrency: 8}
					cp := seedHistory(b, r, opts, mode, rounds)
					// Persisted measurements retain only the Store's copy.
					if store != nil {
						cp = graph.Checkpoint[state]{}
					}
					after := resources(true)
					if store != nil {
						var err error
						cp, err = store.Load(context.Background(), "history")
						if err != nil {
							b.Fatal(err)
						}
					}
					payload, err := (checkpoint.JSON[state]{}).Append(nil, cp)
					if err != nil {
						b.Fatal(err)
					}
					encodedBytes := len(payload)
					payload = nil
					var last graph.Result[state]
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if store == nil {
							last, err = r.Resume(context.Background(), cp, nil, opts)
						} else {
							last, err = r.Recover(context.Background(), cp.RunID, nil, opts)
						}
						if err != nil || last.Status != graph.StatusWaiting || last.Checkpoint.Revision != cp.Revision || last.Checkpoint.Failure != nil || last.Checkpoint.HadLocalFailures != cp.HadLocalFailures {
							b.Fatalf("history inspection changed position: %v", err)
						}
					}
					b.StopTimer()
					if !reflect.DeepEqual(last.Checkpoint, cp) {
						b.Fatal("history inspection changed checkpoint")
					}
					b.ReportMetric(float64(encodedBytes), "checkpoint-B")
					b.ReportMetric(float64(int64(after.HeapBytes)-int64(before.HeapBytes)), "seed-heap-B")
					b.ReportMetric(float64(after.Goroutines), "idle-goroutines")
					runtime.KeepAlive(cp)
					runtime.KeepAlive(store)
				})
			}
		}
	}
}
