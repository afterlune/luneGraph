package capacitytest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

type limiterSoakReport struct {
	Report        report           `json:"report"`
	Callbacks     limiterCallbacks `json:"callbacks"`
	DrainMS       float64          `json:"drain_ms"`
	ReloadMS      float64          `json:"reload_ms"`
	RecoveryMS    float64          `json:"recovery_ms"`
	ReloadedSteps uint64           `json:"reloaded_committed_steps"`
	RecoverySteps uint64           `json:"recovery_steps"`
}

func logLimiterSoak(t *testing.T, p *limiterPopulation, m *measurement, phase string, elapsed time.Duration, extra limiterSoakReport) {
	t.Helper()
	extra.Report = m.snapshot(phase, elapsed, resources(phase != "progress"))
	extra.Callbacks = p.tracker.snapshot()
	data, err := json.Marshal(extra)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
}

func TestCapacitySharedLimiterSoak(t *testing.T) {
	duration, err := soakDuration(os.Getenv("LUNEGRAPH_SOAK_DURATION"))
	if err != nil {
		t.Fatal(err)
	}
	if duration == 0 {
		t.Skip("set LUNEGRAPH_SOAK_DURATION to enable the manual shared-limiter soak")
	}
	for _, kind := range []string{"memory", "sqlite"} {
		for _, capacity := range []int{1, 8} {
			t.Run(fmt.Sprintf("%s/capacity=%d", kind, capacity), func(t *testing.T) {
				t.Logf("Go=%s OS=%s arch=%s GOMAXPROCS=%d duration=%s runs=64 callers=8 width=8 node_concurrency=8 callback_capacity=%d store=%s", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), duration, capacity, kind)
				beforeSeed := resources(true)
				p := newLimiterPopulation(t, kind, capacity)
				var m measurement
				t.Logf("before_seed_gc=%+v", beforeSeed)
				logLimiterSoak(t, p, &m, "after_seed_gc", 0, limiterSoakReport{})
				ctx, cancel := context.WithTimeout(context.Background(), duration)
				defer cancel()
				started := time.Now()
				errorsOut := make(chan error, 8)
				var workers, sampler sync.WaitGroup
				// Every caller owns eight fixed slots. There is never more than one
				// invocation of Recover for a given execution at a time.
				for caller := range 8 {
					workers.Go(func() {
						for ctx.Err() == nil {
							for i := caller; i < len(p.slots) && ctx.Err() == nil; i += 8 {
								before := p.slots[i].checkpoint.Steps
								callStarted := time.Now()
								out, err := p.advance(ctx, i)
								elapsed := time.Since(callStarted)
								if err != nil {
									if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
										m.record(elapsed, true, 0)
										return
									}
									errorsOut <- err
									cancel()
									return
								}
								m.record(elapsed, false, out.Checkpoint.Steps-before)
							}
						}
					})
				}
				sampler.Go(func() {
					samples, progress := time.NewTicker(500*time.Millisecond), time.NewTicker(5*time.Second)
					defer samples.Stop()
					defer progress.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-samples.C:
							m.snapshot("sample", time.Since(started), resources(false))
						case <-progress.C:
							logLimiterSoak(t, p, &m, "progress", time.Since(started), limiterSoakReport{})
						}
					}
				})
				// Cleanup also drains after a failed assertion before the normal stop.
				t.Cleanup(func() { cancel(); workers.Wait(); sampler.Wait() })
				<-ctx.Done()
				drainStarted := time.Now()
				workers.Wait()
				sampler.Wait()
				extra := limiterSoakReport{DrainMS: float64(time.Since(drainStarted)) / float64(time.Millisecond)}
				elapsed := time.Since(started)
				close(errorsOut)
				for err := range errorsOut {
					t.Error(err)
				}
				p.tracker.verifyDrained(t, capacity)
				logLimiterSoak(t, p, &m, "after_drain_gc", elapsed, extra)
				if t.Failed() {
					return
				}
				fresh, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				for i := range p.slots {
					r, err := p.reconcile(fresh, i)
					if err != nil {
						t.Fatal(err)
					}
					extra.ReloadMS += float64(r.Reload) / float64(time.Millisecond)
					extra.RecoveryMS += float64(r.Recover) / float64(time.Millisecond)
					extra.ReloadedSteps += r.ReloadedSteps
					extra.RecoverySteps += r.RecoverySteps
				}
				p.tracker.verifyDrained(t, capacity)
				logLimiterSoak(t, p, &m, "after_recovery_gc", elapsed, extra)
				// A complete new round on the same limiter also checks continued
				// admission after cancellation and authoritative recovery.
				if _, err := p.advance(fresh, 0); err != nil {
					t.Fatal("post-recovery admission:", err)
				}
				p.tracker.verifyDrained(t, capacity)
				runtime.KeepAlive(p)
			})
		}
	}
}
