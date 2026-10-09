package main

import (
	"context"
	"errors"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestInterruptionAfterEffectCommit(t *testing.T) {
	for _, kind := range []string{"node", "continuation", "join"} {
		t.Run(kind, func(t *testing.T) {
			store, l, path := openFixture(t)
			add := effect(l)
			var calls []graph.CallInfo
			r := callbackRunner(t, kind, func(ctx context.Context, c graph.CallInfo, delta int64) (int64, error) {
				calls = append(calls, c)
				value, err := add(ctx, c, delta)
				if err == nil && len(calls) == 1 {
					return value, graph.Interrupt(errors.New("confirmation deferred"))
				}
				return value, err
			})
			ctx := context.Background()
			opts := graph.Options[state]{Store: store, MaxConcurrency: 1}
			first, err := r.Start(ctx, "interrupted-effect", state{Delta: 3}, opts)
			if kind == "continuation" {
				check(t, err)
				first, err = r.Resume(ctx, first.Checkpoint, inputsFor(first.Checkpoint), opts)
			}
			if first.Status != graph.StatusInterrupted || !errors.Is(err, graph.ErrInterrupted) || first.Checkpoint.Completed || first.Checkpoint.Failure != nil {
				t.Fatalf("first=%+v %v", first, err)
			}
			assertEffects(t, path, 3, 1)
			// A separate execution changes the counter, so replay must return the
			// original receipt result rather than the counter's latest value.
			other, err := newRunner(add)
			check(t, err)
			_, err = other.Start(ctx, "other-effect", state{Delta: 4}, graph.Options[state]{Store: store})
			check(t, err)
			out, err := r.Recover(ctx, "interrupted-effect", inputsFor(first.Checkpoint), opts)
			check(t, err)
			if out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || out.Checkpoint.Final.Value != 3 || len(calls) != 2 || calls[0] != calls[1] {
				t.Fatalf("out=%+v calls=%+v", out, calls)
			}
			assertEffects(t, path, 7, 2)
		})
	}
}

func TestInterruptionCancellationAndPanicBoundaries(t *testing.T) {
	for _, kind := range []string{"node", "continuation", "join"} {
		for _, mode := range []string{"cancel", "panic"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				r := callbackRunner(t, kind, func(_ context.Context, _ graph.CallInfo, _ int64) (int64, error) {
					if mode == "panic" {
						panic(graph.Interrupt(nil))
					}
					cancel()
					return 0, graph.Interrupt(nil)
				})
				opts := graph.Options[state]{MaxConcurrency: 1}
				out, err := r.Start(ctx, "boundary", state{Delta: 3}, opts)
				if kind == "continuation" {
					check(t, err)
					out, err = r.Resume(ctx, out.Checkpoint, inputsFor(out.Checkpoint), opts)
				}
				if mode == "cancel" {
					if out.Status != graph.StatusCancelled || !errors.Is(err, context.Canceled) || out.Checkpoint.Completed {
						t.Fatalf("cancel=%+v %v", out, err)
					}
				} else {
					var pe *graph.PanicError
					if out.Status != graph.StatusFailed || !errors.As(err, &pe) || !out.Checkpoint.Completed || out.Checkpoint.Failure == nil {
						t.Fatalf("panic=%+v %v", out, err)
					}
				}
			})
		}
	}
}
