package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

var errAcknowledgement = errors.New("checkpoint acknowledgement unavailable")

type faultStore struct {
	graph.Store[state]
	mode  string
	armed bool
}

func (s *faultStore) Commit(ctx context.Context, expected uint64, change graph.Change[state]) error {
	if !s.armed {
		return s.Store.Commit(ctx, expected, change)
	}
	s.armed = false
	if s.mode == "conflict" {
		return graph.ErrConflict
	}
	if s.mode == "before" {
		return errAcknowledgement
	}
	if err := s.Store.Commit(ctx, expected, change); err != nil {
		return err
	}
	return errAcknowledgement
}

func TestCallbackRecoveryWithEffectReceipts(t *testing.T) {
	for _, kind := range []string{"node", "continuation", "join"} {
		for _, mode := range []string{"conflict", "before", "after"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				store, l, path := openFixture(t)
				fault := &faultStore{Store: store, mode: mode}
				var calls []graph.CallInfo
				add := effect(l)
				r := callbackRunner(t, kind, func(ctx context.Context, call graph.CallInfo, delta int64) (int64, error) {
					calls = append(calls, call)
					value, err := add(ctx, call, delta)
					if err == nil && len(calls) == 1 {
						fault.armed = true
					}
					return value, err
				})
				ctx := context.Background()
				opts := graph.Options[state]{Store: fault, MaxConcurrency: 1}
				first, err := r.Start(ctx, "replay", state{Delta: 3}, opts)
				if kind == "continuation" {
					check(t, err)
					first, err = r.Resume(ctx, first.Checkpoint, inputsFor(first.Checkpoint), opts)
				}
				wantErr := errAcknowledgement
				if mode == "conflict" {
					wantErr = graph.ErrConflict
				}
				if !errors.Is(err, wantErr) {
					t.Fatalf("first execution: %v", err)
				}
				assertEffects(t, path, 3, 1)
				saved, err := store.Load(ctx, "replay")
				check(t, err)
				wantRevision := first.Checkpoint.Revision
				if mode == "after" {
					wantRevision++
				}
				if saved.Revision != wantRevision {
					t.Fatalf("saved revision=%d, want %d", saved.Revision, wantRevision)
				}
				// Another run advances the counter before the pending call replays.
				// Recovery must restore this call's receipt value, not today's counter.
				other, err := newRunner(effect(l))
				check(t, err)
				advanced, err := other.Start(ctx, "other", state{Delta: 4}, graph.Options[state]{Store: store})
				check(t, err)
				if advanced.Checkpoint.Final == nil || advanced.Checkpoint.Final.Value != 7 {
					t.Fatalf("other run: %+v", advanced)
				}
				out, err := r.Recover(ctx, "replay", inputsFor(saved), opts)
				check(t, err)
				if out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || out.Checkpoint.Final.Value != 3 {
					t.Fatalf("recovery: %+v", out)
				}
				wantCalls := 2
				if mode == "after" {
					wantCalls = 1
				}
				if len(calls) != wantCalls {
					t.Fatalf("calls=%+v, want %d", calls, wantCalls)
				}
				if wantCalls == 2 && calls[0] != calls[1] {
					t.Fatalf("replay identity changed: %+v", calls)
				}
				assertEffects(t, path, 7, 2)
				_, err = r.Recover(ctx, "replay", nil, opts)
				check(t, err)
				if len(calls) != wantCalls {
					t.Fatal("completed recovery executed callback")
				}
			})
		}
	}
}

func TestConcurrentRecoverEffectReceipts(t *testing.T) {
	for _, kind := range []string{"node", "continuation", "join"} {
		for _, protected := range []bool{true, false} {
			label := "unprotected"
			if protected {
				label = "receipt"
			}
			t.Run(kind+"/"+label, func(t *testing.T) {
				store, l, path := openFixture(t)
				var seeding atomic.Bool
				seeding.Store(true)
				entered := make(chan graph.CallInfo, 2)
				release := make(chan struct{})
				var releaseOnce sync.Once
				releaseCallbacks := func() { releaseOnce.Do(func() { close(release) }) }
				var rawEffects atomic.Int64
				var callsMu sync.Mutex
				var calls []graph.CallInfo
				apply := func(ctx context.Context, call graph.CallInfo, delta int64) (int64, error) {
					if seeding.Load() {
						return 0, graph.Interrupt(nil)
					}
					callsMu.Lock()
					calls = append(calls, call)
					callsMu.Unlock()
					entered <- call
					select {
					case <-release:
					case <-ctx.Done():
						return 0, ctx.Err()
					}
					if protected {
						return effect(l)(ctx, call, delta)
					}
					return rawEffects.Add(delta), nil
				}
				runner := callbackRunner(t, kind, apply)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				opts := graph.Options[state]{Store: store, MaxConcurrency: 1}
				seedOptions := opts
				if kind == "join" {
					seedOptions.MaxSteps = 10
				}
				seed, seedErr := runner.Start(ctx, "concurrent-replay", state{Delta: 3}, seedOptions)
				switch kind {
				case "continuation":
					if seedErr != nil || seed.Status != graph.StatusWaiting {
						t.Fatalf("seed=%+v err=%v", seed, seedErr)
					}
				case "node":
					if !errors.Is(seedErr, graph.ErrInterrupted) || seed.Status != graph.StatusInterrupted {
						t.Fatalf("seed=%+v err=%v", seed, seedErr)
					}
				case "join":
					if !errors.Is(seedErr, graph.ErrInterrupted) || seed.Status != graph.StatusInterrupted || len(seed.Checkpoint.Groups) != 1 {
						t.Fatalf("seed=%+v err=%v", seed, seedErr)
					}
				}
				seeding.Store(false)

				type outcome struct {
					result graph.Result[state]
					err    error
				}
				results := make(chan outcome, 2)
				var workers sync.WaitGroup
				t.Cleanup(func() {
					cancel()
					releaseCallbacks()
					workers.Wait()
				})
				for range 2 {
					workers.Add(1)
					go func() {
						defer workers.Done()
						var inputs []graph.ResumeInput
						if kind == "continuation" {
							inputs = inputsFor(seed.Checkpoint)
						}
						result, err := runner.Recover(ctx, "concurrent-replay", inputs, opts)
						results <- outcome{result, err}
					}()
				}

				var first graph.CallInfo
				for range 2 {
					select {
					case call := <-entered:
						if call.RunID != "concurrent-replay" || call.CallID == "" || (first.CallID != "" && (call.CallID != first.CallID || call.Node != first.Node)) {
							t.Fatalf("recoveries did not enter the same logical callback: first=%+v next=%+v", first, call)
						}
						first = call
					case <-ctx.Done():
						t.Fatalf("both callbacks did not reach the barrier: %v", ctx.Err())
					}
				}
				releaseCallbacks()

				var succeeded, conflicted int
				for range 2 {
					select {
					case got := <-results:
						if got.err == nil && got.result.Status == graph.StatusCompleted && got.result.Checkpoint.Final != nil {
							succeeded++
						} else if errors.Is(got.err, graph.ErrConflict) {
							conflicted++
						} else {
							t.Fatalf("unexpected recovery=%+v err=%v", got.result, got.err)
						}
					case <-ctx.Done():
						t.Fatalf("recoveries did not settle: %v", ctx.Err())
					}
				}
				if succeeded != 1 || conflicted != 1 {
					t.Fatalf("recoveries: succeeded=%d conflicted=%d", succeeded, conflicted)
				}
				callsMu.Lock()
				observed := append([]graph.CallInfo(nil), calls...)
				callsMu.Unlock()
				if len(observed) != 2 || observed[0].CallID != observed[1].CallID || observed[0].RunID != observed[1].RunID {
					t.Fatalf("callback attempts did not reuse one id: %+v", observed)
				}
				stored, err := store.Load(ctx, "concurrent-replay")
				if err != nil || !stored.Completed || stored.Final == nil {
					t.Fatalf("authoritative position=%+v err=%v", stored, err)
				}
				if protected {
					if stored.Final.Value != 3 {
						t.Fatalf("stored receipt result=%+v", *stored.Final)
					}
					assertEffects(t, path, 3, 1)
				} else if rawEffects.Load() != 6 {
					t.Fatalf("unprotected effect count=%d want 6", rawEffects.Load())
				}
			})
		}
	}
}

func TestCancellationAfterEffectCommit(t *testing.T) {
	store, l, path := openFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls []graph.CallInfo
	add := effect(l)
	r, err := newRunner(func(ctx context.Context, call graph.CallInfo, delta int64) (int64, error) {
		calls = append(calls, call)
		value, err := add(ctx, call, delta)
		if len(calls) == 1 && err == nil {
			cancel()
		}
		return value, err
	})
	check(t, err)
	opts := graph.Options[state]{Store: store}
	first, err := r.Start(ctx, "cancel", state{Delta: 3}, opts)
	if !errors.Is(err, context.Canceled) || first.Status != graph.StatusCancelled {
		t.Fatalf("cancellation: %+v, %v", first, err)
	}
	assertEffects(t, path, 3, 1)
	out, err := r.Recover(context.Background(), "cancel", nil, opts)
	check(t, err)
	if out.Checkpoint.Final == nil || out.Checkpoint.Final.Value != 3 || len(calls) != 2 || calls[0] != calls[1] {
		t.Fatalf("recovery=%+v calls=%+v", out, calls)
	}
	assertEffects(t, path, 3, 1)
}

func TestSharedRunnerAndStores(t *testing.T) {
	store, l, path := openFixture(t)
	r, err := newRunner(effect(l))
	check(t, err)
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Go(func() {
			for n := range 4 {
				id := fmt.Sprintf("worker-%d-run-%d", worker, n)
				out, err := r.Start(context.Background(), id, state{Delta: 1}, graph.Options[state]{Store: store})
				if err != nil || out.Status != graph.StatusCompleted {
					t.Errorf("%s: %+v, %v", id, out, err)
					return
				}
				again, err := r.Recover(context.Background(), id, nil, graph.Options[state]{Store: store})
				if err != nil || again.Checkpoint.Final == nil || *again.Checkpoint.Final != *out.Checkpoint.Final {
					t.Errorf("%s recovered: %+v, %v", id, again, err)
				}
			}
		})
	}
	wg.Wait()
	assertEffects(t, path, 16, 16)
}
