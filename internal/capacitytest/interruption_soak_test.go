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

	graph "github.com/afterlune/luneGraph"
)

func logInterruptionReport(t *testing.T, r interruptionReport) {
	t.Helper()
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
}

func TestCapacityInterruptionSoak(t *testing.T) {
	duration, err := soakDuration(os.Getenv("LUNEGRAPH_SOAK_DURATION"))
	if err != nil {
		t.Fatal(err)
	}
	if duration == 0 {
		t.Skip("set LUNEGRAPH_SOAK_DURATION to enable the manual interruption soak")
	}
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			t.Logf("Go=%s OS=%s arch=%s GOMAXPROCS=%d duration=%s runs=64 callers=8 width=8 node_concurrency=8", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), duration)
			c := &interruptionController{}
			r, store := interruptionRunner(t, 8, c), openStore(t, kind)
			var metrics interruptionMeasurement
			logInterruptionReport(t, metrics.snapshot("before_seed_gc", 0, resources(true)))
			slots := make([]interruptionSlot, 64)
			for i := range slots {
				slots[i] = seedInterrupted(t, r, store, c, fmt.Sprintf("interruption-%d", i), 8, 8)
				if err := advanceInterruptedRound(context.Background(), r, store, c, &slots[i], 8, 8); err != nil {
					t.Fatal(err)
				}
			}
			logInterruptionReport(t, metrics.snapshot("after_seed_gc", 0, resources(true)))
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			started := time.Now()
			errorsOut := make(chan error, 8)
			var workers sync.WaitGroup
			for worker := range 8 {
				workers.Go(func() {
					for ctx.Err() == nil {
						for i := worker; i < len(slots) && ctx.Err() == nil; i += 8 {
							s := &slots[i]
							beforeSteps := s.checkpoint.Steps
							callStarted := time.Now()
							out, err := interruptionCall(ctx, r, store, c, s, 8, 8)
							elapsed := time.Since(callStarted)
							var committed uint64
							if out.Checkpoint.Steps >= beforeSteps {
								committed = out.Checkpoint.Steps - beforeSteps
							}
							if err != nil && ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
								metrics.record(elapsed, graph.StatusCancelled, committed)
								return
							}
							if err != nil && !errors.Is(err, graph.ErrInterrupted) {
								errorsOut <- err
								cancel()
								return
							}
							metrics.record(elapsed, out.Status, committed)
						}
					}
				})
			}
			samplerDone := make(chan struct{})
			go func() {
				defer close(samplerDone)
				samples := time.NewTicker(500 * time.Millisecond)
				defer samples.Stop()
				progress := time.NewTicker(5 * time.Second)
				defer progress.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-samples.C:
						metrics.snapshot("sample", time.Since(started), resources(false))
					case <-progress.C:
						logInterruptionReport(t, metrics.snapshot("progress", time.Since(started), resources(false)))
					}
				}
			}()
			<-ctx.Done()
			drainStarted := time.Now()
			workers.Wait()
			<-samplerDone
			measuredElapsed := time.Since(started)
			t.Logf("drain_ms=%.4f", float64(time.Since(drainStarted))/float64(time.Millisecond))
			close(errorsOut)
			for err := range errorsOut {
				t.Error(err)
			}
			if c.active.Load() != 0 {
				t.Errorf("callbacks survived public calls: %d", c.active.Load())
			}
			logInterruptionReport(t, metrics.snapshot("after_drain_gc", measuredElapsed, resources(true)))
			if t.Failed() {
				return
			}
			recoveryStarted := time.Now()
			var reloadedSteps, recoverySteps uint64
			for i := range slots {
				s := &slots[i]
				cp, err := store.Load(context.Background(), s.id)
				if err != nil {
					t.Fatal(err)
				}
				if cp.Revision < s.checkpoint.Revision {
					t.Fatalf("revision regressed: %s", s.id)
				}
				if err := checkInterruptionCheckpoint(cp, 8); err != nil {
					t.Fatal(err)
				}
				reloadedSteps += cp.Steps
				pending := c.reconcile(cp)
				s.remember(cp, true)
				waiting := len(interruptionInputs(cp)) != 0
				if !waiting || pending != 0 {
					before := cp.Steps
					if err := advanceInterruptedRound(context.Background(), r, store, c, s, 8, 8); err != nil {
						t.Fatal(err)
					}
					recoverySteps += s.checkpoint.Steps - before
				}
				final, err := store.Load(context.Background(), s.id)
				if err != nil {
					t.Fatal(err)
				}
				value, err := rootState(final)
				if err != nil || len(final.Invocations) != 1 || len(final.Groups) != 0 || final.Invocations[0].Status != graph.InvocationWaiting || final.Revision != s.checkpoint.Revision || value.Total != value.Round*8 || value.Values["inputs"] != value.Round-1 || final.Steps != uint64(value.Round*10) || final.Revision != final.Steps+uint64(value.Round) {
					t.Fatalf("recovery=%+v %v", final, err)
				}
			}
			c.mu.Lock()
			pendingCount := len(c.pending)
			counts := c.counts
			c.mu.Unlock()
			if pendingCount != 0 || c.active.Load() != 0 || counts[0] == 0 || counts[1] == 0 || counts[2] == 0 {
				t.Fatalf("controller pending=%d active=%d counts=%v", pendingCount, c.active.Load(), counts)
			}
			t.Logf("recovery_ms=%.4f reloaded_committed_steps=%d recovery_steps=%d interrupted_node_join_apply=%v pending_controller_entries=%d", float64(time.Since(recoveryStarted))/float64(time.Millisecond), reloadedSteps, recoverySteps, counts, pendingCount)
			logInterruptionReport(t, metrics.snapshot("after_recovery_gc", measuredElapsed, resources(true)))
			runtime.KeepAlive(store)
			runtime.KeepAlive(r)
		})
	}
}
