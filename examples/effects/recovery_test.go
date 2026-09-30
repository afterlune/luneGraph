package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

var errAcknowledgement = errors.New("checkpoint acknowledgement unavailable")

type faultStore struct {
	graph.Store[state]
	mode  string
	armed bool
}

func (s *faultStore) CompareAndSwap(ctx context.Context, expected uint64, next graph.Checkpoint[state]) error {
	if !s.armed {
		return s.Store.CompareAndSwap(ctx, expected, next)
	}
	s.armed = false
	if s.mode == "conflict" {
		return graph.ErrConflict
	}
	if s.mode == "before" {
		return errAcknowledgement
	}
	if err := s.Store.CompareAndSwap(ctx, expected, next); err != nil {
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
