package graph_test

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

func TestSharedLimiterSynchronousDependencyContract(t *testing.T) {
	for _, role := range []string{"node", "join", "apply"} {
		for _, capacity := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/capacity=%d", role, capacity), func(t *testing.T) {
				limiter, err := graph.NewLimiter(capacity)
				if err != nil {
					t.Fatal(err)
				}
				var innerEntered atomic.Int32
				inner := limiterFixture(t, "node", func(_ context.Context, _ graph.CallInfo, s int) (int, error) {
					innerEntered.Add(1)
					return s + 1, nil
				})
				nestedStarted := make(chan struct{}, 1)
				outer := limiterFixture(t, role, func(ctx context.Context, _ graph.CallInfo, s int) (int, error) {
					result, err := inner.Start(ctx, "inner", s, graph.Options[int]{Limiter: limiter,
						Observer: graph.ObserverFunc(func(_ context.Context, event graph.Event) {
							if event.Operation == graph.OperationStart && event.Phase == graph.PhaseStarted {
								nestedStarted <- struct{}{}
							}
						}),
					})
					if err != nil {
						return s, err
					}
					if result.Checkpoint.Final == nil {
						return s, errors.New("inner execution has no final value")
					}
					return *result.Checkpoint.Final, nil
				})
				var waiting graph.Checkpoint[int]
				if role == "apply" {
					seed, err := outer.Start(context.Background(), "outer", 0, graph.Options[int]{})
					if err != nil || seed.Status != graph.StatusWaiting {
						t.Fatalf("waiting seed=%+v err=%v", seed, err)
					}
					waiting = seed.Checkpoint
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				type response struct {
					result graph.Result[int]
					err    error
				}
				results := make(chan response, 1)
				finished := make(chan struct{})
				t.Cleanup(func() {
					cancel()
					select {
					case <-finished:
					case <-time.After(5 * time.Second):
						t.Error("nested calls did not exit after cancellation")
					}
				})
				go func() {
					opts := graph.Options[int]{Limiter: limiter}
					var result graph.Result[int]
					var err error
					if role == "apply" {
						result, err = outer.Resume(ctx, waiting, []graph.ResumeInput{{InvocationID: waiting.Invocations[0].ID, Payload: []byte("1")}}, opts)
					} else {
						result, err = outer.Start(ctx, "outer", 0, opts)
					}
					results <- response{result, err}
					close(finished)
				}()
				select {
				case <-nestedStarted:
				case <-ctx.Done():
					t.Fatal("outer callback did not start its nested call")
				}
				if capacity == 1 {
					if innerEntered.Load() != 0 {
						t.Fatal("inner callback acquired the outer callback's occupied permit")
					}
					select {
					case got := <-results:
						t.Fatalf("synchronous dependency unexpectedly completed: %+v", got)
					default:
					}
					cancel() // Explicitly break the wait; the deadline is only a fuse.
				}
				var got response
				select {
				case got = <-results:
				case <-time.After(5 * time.Second):
					t.Fatal("nested calls did not return")
				}
				if capacity == 1 {
					if !errors.Is(got.err, context.Canceled) || got.result.Status != graph.StatusCancelled || got.result.Checkpoint.Failure != nil || innerEntered.Load() != 0 {
						t.Fatalf("cancellation=%+v entered=%d", got, innerEntered.Load())
					}
				} else if got.err != nil || got.result.Status != graph.StatusCompleted || innerEntered.Load() != 1 {
					t.Fatalf("two-permit control=%+v entered=%d", got, innerEntered.Load())
				}
				assertDependencyPermitsReusable(t, limiter, capacity)
			})
		}
	}
}

func assertDependencyPermitsReusable(t *testing.T, limiter *graph.Limiter, capacity int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	entered := make(chan struct{}, capacity)
	release := make(chan struct{})
	results := make(chan error, capacity)
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	probe := limiterFixture(t, "node", func(ctx context.Context, _ graph.CallInfo, s int) (int, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return s, nil
		case <-ctx.Done():
			return s, ctx.Err()
		}
	})
	for i := range capacity {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := probe.Start(ctx, fmt.Sprintf("probe-%d", i), 0, graph.Options[int]{Limiter: limiter})
			results <- err
		}()
	}
	for range capacity {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("not all permits could be reused")
		}
	}
	close(release)
	for range capacity {
		if err := <-results; err != nil {
			t.Fatalf("permit probe failed: %v", err)
		}
	}
}
