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

func logReport(t *testing.T, r report) {
	t.Helper()
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
}

func checkRound(cp graph.Checkpoint[state], id string, minimumRound int) error {
	s, err := rootState(cp)
	if err != nil {
		return err
	}
	inputs := (s.Round - 1) / 4
	if s.Owner != id || s.Round < minimumRound || s.Total != s.Round*8 || s.Values["sum"] != s.Round*36 || s.Values["inputs"] != inputs || cp.Steps != uint64(s.Round*10) || cp.Revision != cp.Steps+1+uint64(inputs) || cp.Completed || len(cp.Invocations) != 1 || len(cp.Groups) != 0 || (cp.Failure != nil || cp.HadLocalFailures) {
		return fmt.Errorf("invalid round boundary %s: %+v", id, cp)
	}
	waiting := cp.Invocations[0].Status == graph.InvocationWaiting
	if waiting != (s.Round%4 == 0) {
		return fmt.Errorf("incorrect wait boundary %s: %+v", id, cp)
	}
	return nil
}

func TestCapacitySoak(t *testing.T) {
	duration, err := soakDuration(os.Getenv("LUNEGRAPH_SOAK_DURATION"))
	if err != nil {
		t.Fatal(err)
	}
	if duration == 0 {
		t.Skip("set LUNEGRAPH_SOAK_DURATION to enable the manual capacity soak")
	}
	for _, workload := range []string{"fanout", "history"} {
		for _, kind := range []string{"memory", "sqlite"} {
			t.Run(workload+"/"+kind, func(t *testing.T) {
				stepsPerRound, width := 10, 8
				check := checkRound
				var r *graph.Runner[state]
				if workload == "history" {
					stepsPerRound, width = historyStepsPerRound, 3
					r = historyRunner(t, "mixed")
					check = func(cp graph.Checkpoint[state], id string, minimumRound int) error {
						value, err := rootState(cp)
						if err != nil {
							return err
						}
						if value.Owner != id || value.Round < minimumRound {
							return fmt.Errorf("mixed execution or regressed round: %+v", value)
						}
						return checkHistory(cp, "mixed", value.Round)
					}
				} else {
					r = fanoutRunner(t, 8, 0, 4)
				}
				t.Logf("Go=%s OS=%s arch=%s GOMAXPROCS=%d duration=%s workload=%s runs=64 callers=8 width=%d node_concurrency=8", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), duration, workload, width)
				store := openStore(t, kind)
				var metrics measurement
				logReport(t, metrics.snapshot("before_seed_gc", 0, resources(true)))
				slots := make([]slot, 64)
				opts := graph.Options[state]{Store: store, MaxSteps: stepsPerRound, MaxConcurrency: 8}
				for i := range slots {
					id := fmt.Sprintf("soak-%d", i)
					out, err := r.Start(context.Background(), id, initial(id), opts)
					if err != nil {
						t.Fatal(err)
					}
					if err := check(out.Checkpoint, id, 1); err != nil {
						t.Fatal(err)
					}
					slots[i] = slot{id: id, round: 1, revision: out.Checkpoint.Revision}
				}
				logReport(t, metrics.snapshot("after_seed_gc", 0, resources(true)))
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
								var inputs []graph.ResumeInput
								if s.round%4 == 0 {
									inputs = []graph.ResumeInput{{InvocationID: "i1", Payload: []byte("1")}}
								}
								callStart := time.Now()
								out, err := r.Recover(ctx, s.id, inputs, opts)
								elapsed := time.Since(callStart)
								if err != nil {
									if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
										metrics.record(elapsed, true, 0)
										return
									}
									errorsOut <- err
									cancel()
									return
								}
								if err := check(out.Checkpoint, s.id, s.round+1); err != nil {
									errorsOut <- err
									cancel()
									return
								}
								value, _ := rootState(out.Checkpoint)
								s.round, s.revision = value.Round, out.Checkpoint.Revision
								metrics.record(elapsed, false, uint64(stepsPerRound))
							}
						}
					})
				}
				samplerDone := make(chan struct{})
				go func() {
					defer close(samplerDone)
					sampleTicker := time.NewTicker(500 * time.Millisecond)
					progressTicker := time.NewTicker(5 * time.Second)
					defer sampleTicker.Stop()
					defer progressTicker.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-sampleTicker.C:
							metrics.snapshot("sample", time.Since(started), resources(false))
						case <-progressTicker.C:
							logReport(t, metrics.snapshot("progress", time.Since(started), resources(false)))
						}
					}
				}()
				workers.Wait()
				<-samplerDone
				close(errorsOut)
				for err := range errorsOut {
					t.Error(err)
				}
				measuredElapsed := time.Since(started)
				logReport(t, metrics.snapshot("after_drain_gc", measuredElapsed, resources(true)))
				if t.Failed() {
					return
				}
				// Cancellation can leave a partially committed round or an uncertain
				// acknowledgement. Reload the authoritative checkpoint and finish only
				// its current round, using a fresh context and the remaining node budget.
				for _, s := range slots {
					cp, err := store.Load(context.Background(), s.id)
					if err != nil {
						t.Fatal(err)
					}
					if cp.Revision < s.revision {
						t.Fatalf("revision regressed for %s", s.id)
					}
					if remainder := cp.Steps % uint64(stepsPerRound); remainder != 0 {
						recoveryOpts := opts
						recoveryOpts.MaxSteps = stepsPerRound - int(remainder)
						out, err := r.Recover(context.Background(), s.id, nil, recoveryOpts)
						if err != nil {
							t.Fatal(err)
						}
						cp = out.Checkpoint
					} else {
						// An accepted continuation may precede a cancelled fork.
						value, err := rootState(cp)
						if err != nil {
							t.Fatal(err)
						}
						if value.Round%4 == 0 && cp.Invocations[0].Status != graph.InvocationWaiting {
							out, err := r.Recover(context.Background(), s.id, nil, opts)
							if err != nil {
								t.Fatal(err)
							}
							cp = out.Checkpoint
						}
					}
					if err := check(cp, s.id, s.round); err != nil {
						t.Fatal(err)
					}
					saved, err := store.Load(context.Background(), s.id)
					if err != nil || saved.Revision != cp.Revision || saved.Steps != cp.Steps {
						t.Fatalf("recovery checkpoint %s: %+v, %v", s.id, saved, err)
					}
				}
				logReport(t, metrics.snapshot("after_recovery_gc", measuredElapsed, resources(true)))
				runtime.KeepAlive(store)
				runtime.KeepAlive(r)
			})
		}
	}
}
