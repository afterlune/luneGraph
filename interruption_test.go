package graph_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

type interruptionStore struct {
	graph.Store[int]
	commits int
	fail    error
}

func (s *interruptionStore) CompareAndSwap(ctx context.Context, revision uint64, cp graph.Checkpoint[int]) error {
	s.commits++
	if s.fail != nil {
		return s.fail
	}
	return s.Store.CompareAndSwap(ctx, revision, cp)
}

func TestInterruptError(t *testing.T) {
	cause := &graph.TransitionError{Node: "remote", Cause: errors.New("unconfirmed")}
	err := fmt.Errorf("wrapped: %w", graph.Interrupt(cause))
	var got *graph.TransitionError
	if !errors.Is(err, graph.ErrInterrupted) || !errors.Is(err, cause) || !errors.As(err, &got) || got != cause || graph.Interrupt(nil) != graph.ErrInterrupted {
		t.Fatalf("interruption lost identity or cause: %v", err)
	}
}

func TestNodeInterruptionPoliciesAndRecovery(t *testing.T) {
	for _, scope := range []graph.FailureScope{graph.FailInvocation, graph.FailGroup, graph.FailExecution} {
		for _, override := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/override=%v", scope, override), func(t *testing.T) {
				store := &interruptionStore{Store: newMemoryStore(t)}
				cause := errors.New("remote unconfirmed")
				var calls []graph.CallInfo
				g := graph.New[int]("work")
				if err := g.AddNode(graph.NodeSpec[int]{Name: "work", OnError: scope, Run: func(_ context.Context, call graph.CallInfo, s int) (graph.Transition[int], error) {
					calls = append(calls, call)
					if len(calls) == 1 {
						return graph.EndExecution(99), fmt.Errorf("tool: %w", graph.Interrupt(cause))
					}
					return graph.EndExecution(s + 1), nil
				}}); err != nil {
					t.Fatal(err)
				}
				r := intRunner(t, g)
				opts := graph.Options[int]{Store: store}
				forced := graph.FailExecution
				if override {
					opts.FailureOverride = &forced
				}
				first, err := r.Start(context.Background(), "interrupt", 7, opts)
				if first.Status != graph.StatusInterrupted || !errors.Is(err, cause) || !errors.Is(err, graph.ErrInterrupted) || first.Checkpoint.Completed || first.Checkpoint.Failure != nil || first.Checkpoint.HadLocalFailures || first.Checkpoint.Revision != 1 || first.Checkpoint.Steps != 0 || store.commits != 0 {
					t.Fatalf("first=%+v err=%v commits=%d", first, err, store.commits)
				}
				saved, err := store.Load(context.Background(), "interrupt")
				if err != nil || !reflect.DeepEqual(saved, first.Checkpoint) {
					t.Fatalf("saved=%+v err=%v", saved, err)
				}
				out, err := r.Recover(context.Background(), "interrupt", nil, opts)
				if err != nil || out.Status != graph.StatusCompleted || *out.Checkpoint.Final != 8 || len(calls) != 2 || calls[0] != calls[1] || store.commits != 1 {
					t.Fatalf("out=%+v calls=%+v err=%v", out, calls, err)
				}
			})
		}
	}
}

func TestInterruptionSQLiteReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runs.db")
	store, err := sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	var calls []graph.CallInfo
	g := graph.New[int]("work")
	node(t, g, "work", func(_ context.Context, c graph.CallInfo, s int) (graph.Transition[int], error) {
		calls = append(calls, c)
		if len(calls) == 1 {
			return graph.Transition[int]{}, graph.Interrupt(nil)
		}
		return graph.EndExecution(s), nil
	})
	r := intRunner(t, g)
	first, err := r.Start(ctx, "reopen", 3, graph.Options[int]{Store: store})
	if first.Status != graph.StatusInterrupted || !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("first=%+v %v", first, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	out, err := r.Recover(ctx, "reopen", nil, graph.Options[int]{Store: store})
	if err != nil || out.Status != graph.StatusCompleted || calls[0] != calls[1] {
		t.Fatalf("out=%+v calls=%+v %v", out, calls, err)
	}
}

func TestInterruptionDoesNotChangeOtherErrorBoundaries(t *testing.T) {
	for _, kind := range []string{"panic", "clone", "decode", "store", "store-marker", "cancel", "deadline-cause"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			marker := graph.Interrupt(context.DeadlineExceeded)
			g := graph.New[int]("work")
			if err := g.AddNode(graph.NodeSpec[int]{Name: "work", OnError: graph.FailExecution, Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				switch kind {
				case "panic":
					panic(marker)
				case "cancel":
					cancel()
					return graph.Transition[int]{}, marker
				case "deadline-cause":
					return graph.Transition[int]{}, marker
				case "decode":
					return graph.Wait(s, "input", "done"), nil
				}
				return graph.EndExecution(s), nil
			}}); err != nil {
				t.Fatal(err)
			}
			node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.EndExecution(s), nil
			})
			edge(t, g, "work", "done")
			if err := graph.RegisterContinuation(g, "input", func([]byte) (int, error) { return 0, marker }, func(_ context.Context, _ graph.CallInfo, s, p int) (int, error) { return s + p, nil }); err != nil {
				t.Fatal(err)
			}
			r, err := g.Compile(graph.Config[int]{MachineID: "boundary", Clone: func(s int) (int, error) {
				if kind == "clone" {
					return 0, marker
				}
				return s, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			store := &interruptionStore{Store: newMemoryStore(t)}
			if kind == "store" {
				store.fail = marker
			}
			if kind == "store-marker" {
				store.fail = graph.Interrupt(nil)
			}
			out, err := r.Start(ctx, "boundary", 0, graph.Options[int]{Store: store})
			want := graph.StatusFailed
			if kind == "cancel" {
				want = graph.StatusCancelled
			}
			if kind == "store" {
				want = graph.StatusCancelled
			} // Existing context-error classification remains intact.
			if kind == "deadline-cause" {
				want = graph.StatusInterrupted
			}
			if kind == "decode" {
				out, err = r.Resume(ctx, out.Checkpoint, []graph.ResumeInput{{InvocationID: out.Checkpoint.Invocations[0].ID}}, graph.Options[int]{Store: store})
				want = graph.StatusWaiting
			}
			if err == nil || out.Status != want {
				t.Fatalf("out=%+v err=%v want=%s", out, err, want)
			}
			if kind == "panic" {
				var pe *graph.PanicError
				if !errors.As(err, &pe) || !out.Checkpoint.Completed || out.Checkpoint.Failure == nil {
					t.Fatalf("panic swallowed: %+v %v", out, err)
				}
			}
		})
	}
}

func TestCloneInterruptionMarkerAtResumeAndJoin(t *testing.T) {
	for _, kind := range []string{"resume", "join"} {
		t.Run(kind, func(t *testing.T) {
			var armed atomic.Bool
			g := graph.New[int]("fork")
			node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.To(s, "a", "b"), nil
			})
			for _, name := range []string{"a", "b"} {
				node(t, g, name, func(_ context.Context, c graph.CallInfo, s int) (graph.Transition[int], error) {
					if kind == "resume" {
						return graph.Wait(s, "input", "join"), nil
					}
					if c.Node == "b" {
						armed.Store(true)
					}
					return graph.To(s, "join"), nil
				})
				edge(t, g, "fork", name)
			}
			if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, values []int) (int, error) {
				t.Fatal("merge ran after clone error")
				return 0, nil
			}}); err != nil {
				t.Fatal(err)
			}
			edge(t, g, "a", "join")
			edge(t, g, "b", "join")
			if err := graph.RegisterJSONContinuation(g, "input", func(_ context.Context, _ graph.CallInfo, s, p int) (int, error) {
				t.Fatal("apply ran after clone error")
				return 0, nil
			}); err != nil {
				t.Fatal(err)
			}
			r, err := g.Compile(graph.Config[int]{MachineID: "clone-boundary", Clone: func(s int) (int, error) {
				if armed.Load() {
					return 0, graph.Interrupt(nil)
				}
				return s, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			opts := graph.Options[int]{MaxConcurrency: 1, Store: newMemoryStore(t)}
			out, err := r.Start(context.Background(), "clone-boundary", 0, opts)
			if kind == "resume" {
				if err != nil || out.Status != graph.StatusWaiting {
					t.Fatalf("start=%+v %v", out, err)
				}
				armed.Store(true)
				var input graph.ResumeInput
				for _, inv := range out.Checkpoint.Invocations {
					if inv.Status == graph.InvocationWaiting {
						input.InvocationID = inv.ID
						input.Payload = []byte("1")
						break
					}
				}
				out, err = r.Resume(context.Background(), out.Checkpoint, []graph.ResumeInput{input}, opts)
			}
			if out.Status != graph.StatusFailed || !errors.Is(err, graph.ErrInterrupted) || out.Checkpoint.Completed || out.Checkpoint.HadLocalFailures || out.Checkpoint.Failure != nil {
				t.Fatalf("out=%+v %v", out, err)
			}
		})
	}
}

func TestInterruptionResumeWithoutStore(t *testing.T) {
	var calls []graph.CallInfo
	g := graph.New[int]("work")
	node(t, g, "work", func(_ context.Context, c graph.CallInfo, s int) (graph.Transition[int], error) {
		calls = append(calls, c)
		if len(calls) == 1 {
			return graph.EndExecution(99), graph.Interrupt(nil)
		}
		return graph.EndExecution(s), nil
	})
	r := intRunner(t, g)
	out, err := r.Start(context.Background(), "in-memory", 4, graph.Options[int]{})
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatal(err)
	}
	out, err = r.Resume(context.Background(), out.Checkpoint, nil, graph.Options[int]{})
	if err != nil || out.Status != graph.StatusCompleted || *out.Checkpoint.Final != 4 || len(calls) != 2 || calls[0] != calls[1] {
		t.Fatalf("out=%+v calls=%+v %v", out, calls, err)
	}
}
