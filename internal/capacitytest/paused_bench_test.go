package capacitytest

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func BenchmarkCapacityPaused(b *testing.B) {
	for _, kind := range []string{"memory", "sqlite"} {
		counts := []int{128, 1024, 8192}
		if kind == "sqlite" {
			counts = counts[:2]
		}
		for _, count := range counts {
			for _, mode := range []string{"inspect", "advance"} {
				b.Run(fmt.Sprintf("store=%s/runs=%d/mode=%s", kind, count, mode), func(b *testing.B) {
					r, store := waitingRunner(b), openStore(b, kind)
					runtime.GC()
					var before, seeded runtime.MemStats
					runtime.ReadMemStats(&before)
					slots := population(b, r, store, count)
					runtime.GC()
					runtime.ReadMemStats(&seeded)
					goroutines := runtime.NumGoroutine()
					b.ReportAllocs()
					b.ResetTimer()
					for i := range b.N {
						s := &slots[i%count]
						if mode == "advance" {
							if err := advance(context.Background(), r, store, s); err != nil {
								b.Fatal(err)
							}
						} else {
							out, err := r.Recover(context.Background(), s.id, nil, graph.Options[state]{Store: store})
							value, stateErr := rootState(out.Checkpoint)
							if err != nil || stateErr != nil || out.Status != graph.StatusWaiting || value.Owner != s.id || value.Round != s.round || out.Checkpoint.Revision != s.revision {
								b.Fatalf("inspect %s: %+v, %v, %v", s.id, out, err, stateErr)
							}
						}
					}
					b.StopTimer()
					verifyPopulation(b, store, slots)
					if seeded.HeapAlloc > before.HeapAlloc {
						b.ReportMetric(float64(seeded.HeapAlloc-before.HeapAlloc), "seed-heap-B")
					}
					b.ReportMetric(float64(goroutines), "idle-goroutines")
					b.ReportMetric(float64(count), "paused-runs")
					runtime.KeepAlive(store)
					runtime.KeepAlive(slots)
				})
			}
		}
	}
}
