package capacitytest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/internal/limit"
)

func checkLimitedBoundary(cp graph.Checkpoint[state], id string) error {
	v, err := rootState(cp)
	if err != nil {
		return err
	}
	if v.Owner != id || v.Round < 1 || v.Total != v.Round*8 || v.Values["sum"] != v.Round*36 || v.Values["inputs"] != v.Round-1 || cp.Steps != uint64(v.Round*10) || cp.Revision != uint64(v.Round*11) || cp.Completed || cp.HadLocalFailures || cp.Failure != nil || len(cp.Groups) != 0 || len(cp.Invocations) != 1 || cp.Invocations[0].Status != graph.InvocationWaiting {
		return fmt.Errorf("invalid limited boundary %s: %+v", id, cp)
	}
	return nil
}

type limiterRecovery struct {
	Reload        time.Duration
	Recover       time.Duration
	ReloadedSteps uint64
	RecoverySteps uint64
}

// Reload first: a cancelled Store write can leave the persisted position ahead
// of both the returned checkpoint and the last successful round reference.
func (p *limiterPopulation) reconcile(ctx context.Context, i int) (limiterRecovery, error) {
	var result limiterRecovery
	s := &p.slots[i]
	started := time.Now()
	cp, err := p.store.Load(ctx, s.id)
	result.Reload = time.Since(started)
	if err != nil {
		return result, err
	}
	result.ReloadedSteps = cp.Steps
	if cp.RunID != s.id || cp.Revision < s.checkpoint.Revision || cp.Steps < s.checkpoint.Steps {
		return result, fmt.Errorf("limited execution regressed: %+v", cp)
	}
	if err := checkInterruptionCheckpoint(cp, 8); err != nil {
		return result, err
	}
	if len(cp.Invocations) != 1 || len(cp.Groups) != 0 || cp.Invocations[0].Status != graph.InvocationWaiting {
		opts := p.opts
		opts.MaxSteps = 10 - int(cp.Steps%10)
		started = time.Now()
		out, recoverErr := p.runner.Recover(ctx, s.id, nil, opts)
		result.Recover = time.Since(started)
		if recoverErr != nil {
			return result, recoverErr
		}
		if out.Checkpoint.Steps < cp.Steps {
			return result, fmt.Errorf("recovery steps regressed")
		}
		result.RecoverySteps = out.Checkpoint.Steps - cp.Steps
		cp = out.Checkpoint
	}
	if err := checkLimitedBoundary(cp, s.id); err != nil {
		return result, err
	}
	started = time.Now()
	saved, err := p.store.Load(ctx, s.id)
	result.Reload += time.Since(started)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(saved, cp) {
		return result, fmt.Errorf("limited recovery differs from Store")
	}
	s.remember(cp, true)
	return result, nil
}

func TestCapacitySharedLimiterCancellationRecovery(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, capacity := range []int{1, 8} {
			t.Run(fmt.Sprintf("%s/capacity=%d", kind, capacity), func(t *testing.T) {
				p := newLimiterPopulation(t, kind, capacity)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				started := make(chan struct{})
				opts := p.opts
				opts.Observer = graph.ObserverFunc(func(ctx context.Context, e graph.Event) {
					p.tracker.Observe(ctx, e)
					if e.Operation == graph.OperationNode && e.Phase == graph.PhaseStarted && e.Node == "b0" {
						close(started)
						<-ctx.Done()
					}
				})
				type answer struct {
					out graph.Result[state]
					err error
				}
				done := make(chan answer, 1)
				var workers sync.WaitGroup
				workers.Add(1)
				t.Cleanup(func() { cancel(); workers.Wait() })
				go func() {
					defer workers.Done()
					out, err := p.runner.Recover(ctx, p.slots[0].id, interruptionInputs(p.slots[0].checkpoint), opts)
					done <- answer{out, err}
				}()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("branch did not reach cancellation gate")
				}
				cancel()
				got := <-done
				if !errors.Is(got.err, context.Canceled) || got.out.Status != graph.StatusCancelled || got.out.Checkpoint.Steps < 11 || got.out.Checkpoint.Completed || got.out.Checkpoint.Failure != nil {
					t.Fatalf("cancel=%+v %v", got.out, got.err)
				}
				p.tracker.verifyDrained(t, capacity)
				fresh, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				recovery, err := p.reconcile(fresh, 0)
				if err != nil || recovery.RecoverySteps == 0 {
					t.Fatalf("recovery=%+v %v", recovery, err)
				}
				if p.slots[0].checkpoint.Steps != 20 || p.slots[0].checkpoint.Revision != 22 {
					t.Fatal("continuation replayed or round skipped")
				}
				if _, err := p.advance(fresh, 1); err != nil {
					t.Fatal("independent execution:", err)
				}
				p.tracker.verifyDrained(t, capacity)
				if p.tracker.discarded.Load() == 0 {
					t.Fatal("cancelled callbacks did not resolve discarded")
				}
				acquired := 0
				defer func() {
					for range acquired {
						limit.Release(p.opts.Limiter)
					}
				}()
				for range capacity {
					if err := limit.Acquire(fresh, p.opts.Limiter); err != nil {
						t.Fatal("permit leaked:", err)
					}
					acquired++
				}
			})
		}
	}
}
