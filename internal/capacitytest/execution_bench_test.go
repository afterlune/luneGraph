package capacitytest

import (
	"context"
	"fmt"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func BenchmarkCapacityFanout(b *testing.B) {
	for _, width := range []int{32, 128, 512} {
		for _, concurrency := range []int{1, 8} {
			b.Run(fmt.Sprintf("width=%d/concurrency=%d", width, concurrency), func(b *testing.B) {
				r := fanoutRunner(b, width, 1, 0)
				opts := graph.Options[state]{MaxSteps: width + 2, MaxConcurrency: concurrency}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					out, err := r.Start(context.Background(), "fanout", initial("fanout"), opts)
					if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || out.Checkpoint.Final.Round != 1 || out.Checkpoint.Final.Total != width || out.Checkpoint.Final.Values["sum"] != width*(width+1)/2 || out.Checkpoint.Steps != uint64(width+2) {
						b.Fatalf("invalid fan-out: %+v, %v", out, err)
					}
				}
				b.ReportMetric(float64(width), "branches/op")
			})
		}
	}
}

func BenchmarkCapacityShared(b *testing.B) {
	for _, kind := range []string{"none", "memory", "sqlite"} {
		for _, profile := range []struct{ count, workers int }{{16, 1}, {64, 8}, {256, 32}} {
			b.Run(fmt.Sprintf("store=%s/runs=%d/workers=%d", kind, profile.count, profile.workers), func(b *testing.B) {
				r, store := waitingRunner(b), openStore(b, kind)
				slots := population(b, r, store, profile.count)
				pool := newBatchPool(profile.count, profile.workers, func(i int) error { return advance(context.Background(), r, store, &slots[i]) })
				defer pool.close()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := pool.batch(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				verifyPopulation(b, store, slots)
				b.ReportMetric(float64(profile.count), "executions/op")
				b.ReportMetric(float64(profile.workers), "workers")
			})
		}
	}
}
