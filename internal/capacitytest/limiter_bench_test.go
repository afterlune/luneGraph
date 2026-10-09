package capacitytest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// One operation advances all 64 executions by one fan-out/join/wait round.
// The finite workload samples resources and individual call latency, and
// keeps setup, Store creation, seeding and final reloads outside timing.
func BenchmarkCapacitySharedLimiter(b *testing.B) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, capacity := range []int{1, 8} {
			b.Run(fmt.Sprintf("%s/capacity=%d", kind, capacity), func(b *testing.B) {
				p := newLimiterPopulation(b, kind, capacity)
				var m measurement
				seed := resources(true)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				pool := newBatchPool(64, 8, func(i int) error {
					started := time.Now()
					before := p.slots[i].checkpoint.Steps
					out, err := p.advance(ctx, i)
					if err == nil {
						m.record(time.Since(started), false, out.Checkpoint.Steps-before)
					}
					return err
				})
				defer func() {
					if pool != nil {
						pool.close()
					}
				}()
				var sampler sync.WaitGroup
				stopSamples := make(chan struct{})
				sampler.Go(func() {
					ticker := time.NewTicker(10 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-stopSamples:
							return
						case <-ticker.C:
							current := resources(false)
							m.mu.Lock()
							if current.HeapBytes > m.peak.HeapBytes {
								m.peak.HeapBytes = current.HeapBytes
							}
							if current.Goroutines > m.peak.Goroutines {
								m.peak.Goroutines = current.Goroutines
							}
							m.mu.Unlock()
						}
					}
				})
				var stopOnce sync.Once
				stopSampling := func() { stopOnce.Do(func() { close(stopSamples); sampler.Wait() }) }
				defer stopSampling()
				b.ReportAllocs()
				b.ResetTimer()
				started := time.Now()
				for range b.N {
					if err := pool.batch(); err != nil {
						b.Fatal(err)
					}
					m.snapshot("sample", time.Since(started), resources(false))
				}
				elapsed := time.Since(started)
				b.StopTimer()
				pool.close()
				// The deferred cleanup also drains workers after a failed iteration.
				pool = nil
				stopSampling()
				p.verify(b, capacity)
				drained := resources(true)
				r := m.snapshot("drained", elapsed, drained)
				encoded, _ := json.Marshal(struct {
					Seed          resourceSample `json:"seed"`
					Report        report         `json:"report"`
					PeakCallbacks int32          `json:"peak_callbacks"`
				}{seed, r, p.tracker.peak.Load()})
				b.Logf("limited workload: %s", encoded)
				b.ReportMetric(64, "executions/op")
				b.ReportMetric(8, "callers")
				b.ReportMetric(float64(capacity), "callback-limit")
				b.ReportMetric(r.CallsPerSecond, "calls/s")
				b.ReportMetric(r.P50MS, "p50-ms")
				b.ReportMetric(r.P95MS, "p95-ms")
			})
		}
	}
}
