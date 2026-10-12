package graph_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type panicRecorder struct {
	atomic.Pointer[graph.PanicError]
}

func (r *panicRecorder) Observe(_ context.Context, e graph.Event) {
	var p *graph.PanicError
	if e.Phase == graph.PhaseFinished && errors.As(e.Err, &p) {
		r.Store(p)
	}
}

func TestCallbackPanicPolicies(t *testing.T) {
	boom := errors.New("callback boom")
	for _, scope := range []graph.FailureScope{graph.FailInvocation, graph.FailExecution} {
		t.Run(string(rune('0'+scope)), func(t *testing.T) {
			g := graph.New[int]("panic")
			if err := g.AddNode(graph.NodeSpec[int]{Name: "panic", OnError: scope, Run: func(context.Context, graph.CallInfo, int) (graph.Transition[int], error) {
				panic(boom)
			}}); err != nil {
				t.Fatal(err)
			}
			var observed panicRecorder
			_, out, err := intRunner(t, g).Start(context.Background(), "node-panic", 1, nil, graph.Options{Observer: &observed})
			if scope == graph.FailExecution {
				if !errors.Is(err, boom) || out.Header.Revision != 2 || out.Header.Steps != 1 || !out.Header.Completed || out.Header.Failure == nil || out.Header.Failure.PanicStack == "" {
					t.Fatalf("execution panic = %+v, %v", out, err)
				}
				var panicErr *graph.PanicError
				if !errors.As(err, &panicErr) || len(panicErr.Stack) == 0 {
					t.Fatalf("panic error = %v", err)
				}
			} else if err != nil || out.Status != graph.StatusCompletedWithFailures || (!out.Header.HadLocalFailures || out.Header.Failure != nil) || observed.Load() == nil || !strings.Contains(string(observed.Load().Stack), "TestCallbackPanicPolicies") {
				t.Fatalf("local panic = %+v, %v", out, err)
			}
		})
	}
}

func TestIllegalTransitionIsTopLevel(t *testing.T) {
	for _, tr := range []graph.Transition[int]{
		{Action: 99},
		graph.To(1, "missing"),
		graph.Wait(1, "missing", "next"),
		{Action: graph.ActionContinue, Targets: []string{"next"}, Continuation: "stray"},
	} {
		g := graph.New[int]("start")
		node(t, g, "start", func(context.Context, graph.CallInfo, int) (graph.Transition[int], error) { return tr, nil })
		node(t, g, "next", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
			return graph.EndExecution(v), nil
		})
		edge(t, g, "start", "next")
		_, out, err := intRunner(t, g).Start(context.Background(), "bad-transition", 0, nil, graph.Options{})
		var transitionErr *graph.TransitionError
		if !errors.As(err, &transitionErr) || out.Header.Revision != 1 || out.Header.Steps != 0 || (out.Header.Failure != nil || out.Header.HadLocalFailures) {
			t.Fatalf("transition %+v => %+v, %v", tr, out, err)
		}
	}
}

func TestCloneAndMergePanics(t *testing.T) {
	g := graph.New[int]("start")
	node(t, g, "start", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndExecution(v), nil
	})
	r, err := g.Compile(graph.Config[int]{MachineID: "panic-clone", Clone: func(int) (int, error) { panic("clone") }})
	if err != nil {
		t.Fatal(err)
	}
	_, out, err := r.Start(context.Background(), "clone", 0, nil, graph.Options{})
	var panicErr *graph.PanicError
	if !errors.As(err, &panicErr) || panicErr.Callback != "clone" || out.Header.Revision != 0 {
		t.Fatalf("clone panic = %+v, %v", out, err)
	}

	cloneFork := graph.New[int]("fork")
	node(t, cloneFork, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "a", "b"), nil
	})
	for _, name := range []string{"a", "b"} {
		node(t, cloneFork, name, func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
			return graph.EndBranch[int](), nil
		})
		edge(t, cloneFork, "fork", name)
	}
	cloneCalls := 0
	cloneRunner, err := cloneFork.Compile(graph.Config[int]{MachineID: "fanout-clone", Clone: func(v int) (int, error) {
		cloneCalls++
		if cloneCalls == 3 {
			panic("fanout clone")
		}
		return v, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, out, err = cloneRunner.Start(context.Background(), "fanout-clone", 0, nil, graph.Options{})
	if !errors.As(err, &panicErr) || out.Header.Revision != 1 || out.Header.Steps != 0 {
		t.Fatalf("fanout clone panic = %+v, %v", out, err)
	}

	fork := graph.New[int]("fork")
	node(t, fork, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "a", "b"), nil
	})
	if err := fork.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(context.Context, graph.CallInfo, int, []int) (int, error) { panic("merge") }}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		node(t, fork, name, func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
			return graph.To(v, "join"), nil
		})
		edge(t, fork, "fork", name)
		edge(t, fork, name, "join")
	}
	var observed panicRecorder
	_, out, err = intRunner(t, fork).Start(context.Background(), "merge", 0, nil, graph.Options{MaxConcurrency: 1, Observer: &observed})
	if err != nil || out.Status != graph.StatusCompletedWithFailures || (!out.Header.HadLocalFailures || out.Header.Failure != nil) || observed.Load() == nil || !strings.Contains(string(observed.Load().Stack), "TestCloneAndMergePanics") {
		t.Fatalf("merge panic = %+v, %v", out, err)
	}
}

func TestWrongJoinIsTransitionError(t *testing.T) {
	g := graph.New[int]("start")
	node(t, g, "start", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.Wait(v, "input", "join"), nil
	})
	node(t, g, "other", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndBranch[int](), nil
	})
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "other", Merge: func(_ context.Context, _ graph.CallInfo, _ int, values []int) (int, error) { return 0, nil }}); err != nil {
		t.Fatal(err)
	}
	edge(t, g, "start", "join")
	if err := graph.RegisterContinuation(g, "input", func([]byte) (int, error) { return 0, nil }, func(_ context.Context, _ graph.CallInfo, state, _ int) (int, error) { return state, nil }); err != nil {
		t.Fatal(err)
	}
	_, out, err := intRunner(t, g).Start(context.Background(), "wrong-join", 0, nil, graph.Options{})
	var transitionErr *graph.TransitionError
	if !errors.As(err, &transitionErr) || out.Header.Revision != 1 {
		t.Fatalf("wrong join = %+v, %v", out, err)
	}
}

func TestContinuationPanics(t *testing.T) {
	for _, panicPhase := range []string{"decode", "apply"} {
		t.Run(panicPhase, func(t *testing.T) {
			g := graph.New[int]("wait")
			node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
				return graph.Wait(v, "input", "done"), nil
			})
			node(t, g, "done", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
				return graph.EndExecution(v), nil
			})
			edge(t, g, "wait", "done")
			if err := graph.RegisterContinuation(g, "input", func([]byte) (int, error) {
				if panicPhase == "decode" {
					panic("decoder")
				}
				return 2, nil
			}, func(_ context.Context, _ graph.CallInfo, state, value int) (int, error) {
				if panicPhase == "apply" {
					panic("apply")
				}
				return state + value, nil
			}); err != nil {
				t.Fatal(err)
			}
			r := intRunner(t, g)
			firstExecution, first, err := r.Start(context.Background(), "continuation-"+panicPhase, 1, nil, graph.Options{})
			if err != nil {
				t.Fatal(err)
			}
			var observed panicRecorder
			outExecution, out, err := r.Resume(context.Background(), mustCheckpoint(firstExecution), []graph.ResumeInput{{InvocationID: "i1", CallID: firstExecution.Pending()[0].CallID, Payload: []byte("x")}}, nil, graph.Options{Observer: &observed})
			if panicPhase == "decode" {
				var panicErr *graph.PanicError
				if !errors.As(err, &panicErr) || out.Header.Revision != first.Header.Revision || mustCheckpoint(outExecution).Invocations[0].Status != graph.InvocationWaiting {
					t.Fatalf("decode panic = %+v, %v", out, err)
				}
			} else if err != nil || out.Status != graph.StatusCompletedWithFailures || (!out.Header.HadLocalFailures || out.Header.Failure != nil) || observed.Load() == nil || len(observed.Load().Stack) == 0 {
				t.Fatalf("apply panic = %+v, %v", out, err)
			}
		})
	}
}

func TestStoreCheckpointIsAuthoritative(t *testing.T) {
	g := graph.New[int]("wait")
	node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.Wait(v, "add", "done"), nil
	})
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndExecution(v), nil
	})
	edge(t, g, "wait", "done")
	if err := graph.RegisterContinuation(g, "add", func([]byte) (int, error) { return 1, nil }, func(_ context.Context, _ graph.CallInfo, state, value int) (int, error) { return state + value, nil }); err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	store := newMemoryStore(t)
	firstExecution, _, err := r.Start(context.Background(), "authority", 4, store, graph.Options{})
	if err != nil {
		t.Fatal(err)
	}
	forged := mustCheckpoint(firstExecution)
	forged.Invocations = append([]graph.Invocation[int](nil), forged.Invocations...)
	forged.Invocations[0].State = 900
	forged.Invocations[0].Node = "bogus"
	_, out, err := r.Resume(context.Background(), forged, []graph.ResumeInput{{InvocationID: "i1", CallID: firstExecution.Pending()[0].CallID}}, store, graph.Options{})
	if err != nil || out.Final == nil || *out.Final != 5 {
		t.Fatalf("authoritative store = %+v, %v", out, err)
	}
	if _, _, err := r.Resume(context.Background(), mustCheckpoint(firstExecution), nil, store, graph.Options{}); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("stale revision = %v", err)
	}
}

func TestMalformedCheckpointRejected(t *testing.T) {
	g := graph.New[int]("wait")
	node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.Wait(v, "input", "wait"), nil
	})
	edge(t, g, "wait", "wait")
	if err := graph.RegisterContinuation(g, "input", func([]byte) (int, error) { return 0, nil }, func(_ context.Context, _ graph.CallInfo, state, _ int) (int, error) { return state, nil }); err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	firstExecution, _, err := r.Start(context.Background(), "validation", 0, nil, graph.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*graph.Checkpoint[int]){
		func(s *graph.Checkpoint[int]) { s.FormatVersion = 1 },
		func(s *graph.Checkpoint[int]) { s.FormatVersion++ },
		func(s *graph.Checkpoint[int]) { s.Invocations[0].CallID = "" },
		func(s *graph.Checkpoint[int]) { s.Invocations[0].CallID = "c9999" },
		func(s *graph.Checkpoint[int]) { s.Invocations[0].ID = "i99" },
		func(s *graph.Checkpoint[int]) { s.Invocations[0].BranchIndex = 1 },
		func(s *graph.Checkpoint[int]) {
			s.Invocations[0].Status = graph.InvocationGroup
			s.Invocations[0].ChildGroupID = "g9"
		},
		func(s *graph.Checkpoint[int]) { s.Invocations[0].Continuation = "missing" },
	} {
		bad := mustCheckpoint(firstExecution)
		bad.Invocations = append([]graph.Invocation[int](nil), bad.Invocations...)
		mutate(&bad)
		if _, _, err := r.Resume(context.Background(), bad, nil, nil, graph.Options{}); !errors.Is(err, graph.ErrInvalidCheckpoint) {
			t.Fatalf("malformed checkpoint accepted: %+v, %v", bad, err)
		}
	}
}

func TestDuplicateCallbackIDsRejected(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "a", "b"), nil
	})
	for _, name := range []string{"a", "b"} {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.EndBranch[int](), nil
		})
		edge(t, g, "fork", name)
	}
	r := intRunner(t, g)
	firstExecution, first, err := r.Start(context.Background(), "duplicate-call-id", 0, nil, graph.Options{MaxSteps: 1})
	if err != nil || len(mustCheckpoint(firstExecution).Invocations) != 3 {
		t.Fatalf("start = %+v, %v", first, err)
	}
	forged := mustCheckpoint(firstExecution)
	forged.Invocations = append([]graph.Invocation[int](nil), mustCheckpoint(firstExecution).Invocations...)
	forged.Invocations[2].CallID = forged.Invocations[1].CallID
	if _, _, err := r.Resume(context.Background(), forged, nil, nil, graph.Options{}); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("duplicate callback ID error = %v", err)
	}
}
