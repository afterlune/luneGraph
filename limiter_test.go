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

func limiterFixture(t *testing.T, role string, callback func(context.Context, graph.CallInfo, int) (int, error)) *graph.Runner[int] {
	t.Helper()
	return intRunner(t, limiterGraph(t, role, callback))
}

func limiterGraph(t *testing.T, role string, callback func(context.Context, graph.CallInfo, int) (int, error)) *graph.Graph[int] {
	t.Helper()
	g := graph.New[int]("entry")
	add := func(name string, run graph.Node[int]) {
		t.Helper()
		if err := g.AddNode(graph.NodeSpec[int]{Name: name, Run: run, OnError: graph.FailExecution}); err != nil {
			t.Fatal(err)
		}
	}
	link := func(from, to string) {
		t.Helper()
		if err := g.AddEdge(from, to); err != nil {
			t.Fatal(err)
		}
	}
	switch role {
	case "node":
		add("entry", func(ctx context.Context, call graph.CallInfo, s int) (graph.Transition[int], error) {
			value, err := callback(ctx, call, s)
			return graph.EndExecution(value), err
		})
	case "join":
		add("entry", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.To(s, "a", "b"), nil
		})
		for _, name := range []string{"a", "b"} {
			add(name, func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.To(s, "join"), nil
			})
			link("entry", name)
		}
		if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "entry", OnError: graph.FailExecution, Merge: func(ctx context.Context, call graph.CallInfo, values []int) (int, error) {
			return callback(ctx, call, values[0]+values[1])
		}}); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"a", "b"} {
			link(name, "join")
		}
	case "apply":
		add("entry", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.Wait(s, "input", "end"), nil
		})
		add("end", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.EndExecution(s), nil
		})
		link("entry", "end")
		if err := graph.RegisterJSONContinuation(g, "input", func(ctx context.Context, call graph.CallInfo, s, p int) (int, error) { return callback(ctx, call, s+p) }); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal(role)
	}
	return g
}

func TestLimiterValidation(t *testing.T) {
	r := limiterFixture(t, "node", func(_ context.Context, _ graph.CallInfo, s int) (int, error) { return s, nil })
	for _, n := range []int{0, -1} {
		if _, err := graph.NewLimiter(n); err == nil {
			t.Fatal("invalid limit")
		}
	}
	store := newMemoryStore(t)
	out, err := r.Start(context.Background(), "invalid", 0, graph.Options[int]{Store: store, Limiter: &graph.Limiter{}})
	if err == nil || out.Checkpoint.Revision != 0 {
		t.Fatalf("invalid limiter: %+v %v", out, err)
	}
	if _, err := store.Load(context.Background(), "invalid"); err == nil {
		t.Fatal("validation created checkpoint")
	}
}

func TestSharedLimiterAcrossCallbackKindsAndRunners(t *testing.T) {
	for _, capacity := range []int{1, 3} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			limiter, _ := graph.NewLimiter(capacity)
			var active, peak, calls atomic.Int32
			var observedActive, observedPeak atomic.Int32
			observer := graph.ObserverFunc(func(_ context.Context, e graph.Event) {
				if e.Operation != graph.OperationNode && e.Operation != graph.OperationJoin && e.Operation != graph.OperationApply {
					return
				}
				if e.Phase == graph.PhaseStarted {
					n := observedActive.Add(1)
					for old := observedPeak.Load(); n > old && !observedPeak.CompareAndSwap(old, n); old = observedPeak.Load() {
					}
				}
				if e.Phase == graph.PhaseFinished {
					observedActive.Add(-1)
				}
			})
			callback := func(_ context.Context, _ graph.CallInfo, s int) (int, error) {
				n := active.Add(1)
				for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
				}
				calls.Add(1)
				active.Add(-1)
				return s + 1, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			errs := make(chan error, 24)
			for _, role := range []string{"node", "join", "apply"} {
				r := limiterFixture(t, role, callback)
				for i := range 8 {
					id := fmt.Sprintf("%s-%d", role, i)
					var cp graph.Checkpoint[int]
					if role == "apply" {
						out, err := r.Start(ctx, id, 1, graph.Options[int]{})
						if err != nil {
							t.Fatal(err)
						}
						cp = out.Checkpoint
					}
					wg.Go(func() {
						opts := graph.Options[int]{Limiter: limiter, MaxConcurrency: 8, Observer: observer}
						var out graph.Result[int]
						var err error
						if role == "apply" {
							out, err = r.Resume(ctx, cp, []graph.ResumeInput{{InvocationID: "i1", Payload: []byte("1")}}, opts)
						} else {
							out, err = r.Start(ctx, id, 1, opts)
						}
						if err == nil && !out.Checkpoint.Completed {
							err = fmt.Errorf("not completed: %+v", out)
						}
						errs <- err
					})
				}
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			if active.Load() != 0 || peak.Load() > int32(capacity) || calls.Load() != 24 || observedActive.Load() != 0 || observedPeak.Load() > int32(capacity) {
				t.Fatalf("active=%d peak=%d calls=%d observed=%d/%d", active.Load(), peak.Load(), calls.Load(), observedActive.Load(), observedPeak.Load())
			}
		})
	}
}

func TestLimiterWaitCancellationPreservesPosition(t *testing.T) {
	limiter, _ := graph.NewLimiter(1)
	holderCtx, release := context.WithCancel(context.Background())
	defer release()
	started := make(chan struct{})
	holder := limiterFixture(t, "node", func(ctx context.Context, _ graph.CallInfo, s int) (int, error) {
		close(started)
		<-ctx.Done()
		return s, ctx.Err()
	})
	holderDone := make(chan error, 1)
	go func() {
		_, err := holder.Start(holderCtx, "holder", 0, graph.Options[int]{Limiter: limiter})
		holderDone <- err
	}()
	<-started
	for _, role := range []string{"node", "join", "apply"} {
		t.Run(role, func(t *testing.T) {
			var entered atomic.Bool
			r := limiterFixture(t, role, func(_ context.Context, _ graph.CallInfo, s int) (int, error) { entered.Store(true); return s, nil })
			var cp graph.Checkpoint[int]
			seedCtx := context.Background()
			if role == "apply" {
				out, err := r.Start(seedCtx, "waiting", 0, graph.Options[int]{})
				if err != nil {
					t.Fatal(err)
				}
				cp = out.Checkpoint
			}
			if role == "join" {
				out, err := r.Start(seedCtx, "waiting", 0, graph.Options[int]{MaxSteps: 2, MaxConcurrency: 1})
				if err != nil {
					t.Fatal(err)
				}
				cp = out.Checkpoint
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			log := make(chan struct{}, 1)
			opts := graph.Options[int]{Limiter: limiter, MaxConcurrency: 8, Observer: graph.ObserverFunc(func(_ context.Context, e graph.Event) {
				if (e.Operation == graph.OperationStart || e.Operation == graph.OperationResume) && e.Phase == graph.PhaseStarted {
					log <- struct{}{}
				}
			})}
			done := make(chan struct {
				out graph.Result[int]
				err error
			}, 1)
			go func() {
				var out graph.Result[int]
				var err error
				if role == "apply" {
					out, err = r.Resume(ctx, cp, []graph.ResumeInput{{InvocationID: "i1", Payload: []byte("1")}}, opts)
				} else if role == "join" {
					out, err = r.Resume(ctx, cp, nil, opts)
				} else {
					out, err = r.Start(ctx, "waiting", 0, opts)
				}
				done <- struct {
					out graph.Result[int]
					err error
				}{out, err}
			}()
			<-log
			cancel()
			got := <-done
			if !errors.Is(got.err, context.Canceled) || got.out.Status != graph.StatusCancelled || entered.Load() || got.out.Checkpoint.Failure != nil {
				t.Fatalf("waiting result=%+v err=%v entered=%v", got.out, got.err, entered.Load())
			}
			if cp.Revision != 0 && (cp.Revision != got.out.Checkpoint.Revision || cp.Steps != got.out.Checkpoint.Steps || cp.Invocations[0].CallID != got.out.Checkpoint.Invocations[0].CallID) {
				t.Fatal("waiting changed position")
			}
		})
	}
	release()
	if !errors.Is(<-holderDone, context.Canceled) {
		t.Fatal("holder cancellation")
	}
	r := limiterFixture(t, "node", func(_ context.Context, _ graph.CallInfo, s int) (int, error) { return s, nil })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := r.Start(ctx, "reuse", 0, graph.Options[int]{Limiter: limiter}); err != nil {
		t.Fatal(err)
	}
}

func TestLimiterReleasesAfterCallbackFailures(t *testing.T) {
	for _, role := range []string{"node", "join", "apply"} {
		for _, mode := range []string{"error", "panic", "interrupt"} {
			t.Run(role+"/"+mode, func(t *testing.T) {
				limiter, _ := graph.NewLimiter(1)
				var attempts int
				r := limiterFixture(t, role, func(_ context.Context, _ graph.CallInfo, s int) (int, error) {
					attempts++
					if attempts == 1 {
						switch mode {
						case "error":
							return s, errors.New("callback")
						case "panic":
							panic("callback")
						case "interrupt":
							return s, graph.Interrupt(nil)
						}
					}
					return s, nil
				})
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				opts := graph.Options[int]{Limiter: limiter}
				var out graph.Result[int]
				var err error
				if role == "apply" {
					out, err = r.Start(ctx, "first", 0, opts)
					if err != nil {
						t.Fatal(err)
					}
					out, err = r.Resume(ctx, out.Checkpoint, []graph.ResumeInput{{InvocationID: "i1", Payload: []byte("1")}}, opts)
				} else {
					out, err = r.Start(ctx, "first", 0, opts)
				}
				if err == nil {
					t.Fatal("missing callback failure")
				}
				if mode == "interrupt" {
					if !errors.Is(err, graph.ErrInterrupted) || out.Checkpoint.Completed {
						t.Fatal("interruption committed")
					}
				}
				healthy := limiterFixture(t, "node", func(_ context.Context, _ graph.CallInfo, s int) (int, error) { return s, nil })
				if _, err := healthy.Start(ctx, "reuse", 0, opts); err != nil {
					t.Fatal("permit leaked:", err)
				}
			})
		}
	}
}
