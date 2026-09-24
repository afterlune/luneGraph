package graph_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	graph "lune-graph"
)

func TestCallbackPanicPolicies(t *testing.T) {
	boom := errors.New("callback boom")
	for _, scope := range []graph.FailureScope{graph.FailInvocation, graph.FailExecution} {
		t.Run(string(rune('0'+scope)), func(t *testing.T) {
			g := graph.New[int]("panic")
			if err := g.AddNode(graph.NodeSpec[int]{Name: "panic", OnError: scope, Run: func(context.Context, int) (graph.Transition[int], error) {
				panic(boom)
			}}); err != nil {
				t.Fatal(err)
			}
			out, err := intRunner(t, g).Start(context.Background(), "node-panic", 1, graph.Options[int]{})
			if scope == graph.FailExecution {
				if !errors.Is(err, boom) || out.Checkpoint.Revision != 1 {
					t.Fatalf("execution panic = %+v, %v", out, err)
				}
				var panicErr *graph.PanicError
				if !errors.As(err, &panicErr) || len(panicErr.Stack) == 0 {
					t.Fatalf("panic error = %v", err)
				}
			} else if err != nil || out.Status != graph.StatusCompletedWithFailures || len(out.Checkpoint.Failures) != 1 || !strings.Contains(out.Checkpoint.Failures[0].PanicStack, "TestCallbackPanicPolicies") {
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
		node(t, g, "start", func(context.Context, int) (graph.Transition[int], error) { return tr, nil })
		node(t, g, "next", func(_ context.Context, v int) (graph.Transition[int], error) { return graph.EndExecution(v), nil })
		edge(t, g, "start", "next")
		out, err := intRunner(t, g).Start(context.Background(), "bad-transition", 0, graph.Options[int]{})
		var transitionErr *graph.TransitionError
		if !errors.As(err, &transitionErr) || out.Checkpoint.Revision != 1 || out.Checkpoint.Steps != 0 || len(out.Checkpoint.Failures) != 0 {
			t.Fatalf("transition %+v => %+v, %v", tr, out, err)
		}
	}
}

func TestCloneAndMergePanics(t *testing.T) {
	g := graph.New[int]("start")
	node(t, g, "start", func(_ context.Context, v int) (graph.Transition[int], error) { return graph.EndExecution(v), nil })
	r, err := g.Compile(graph.Config[int]{MachineID: "panic-clone", Clone: func(int) (int, error) { panic("clone") }})
	if err != nil {
		t.Fatal(err)
	}
	out, err := r.Start(context.Background(), "clone", 0, graph.Options[int]{})
	var panicErr *graph.PanicError
	if !errors.As(err, &panicErr) || panicErr.Callback != "clone" || out.Checkpoint.Revision != 0 {
		t.Fatalf("clone panic = %+v, %v", out, err)
	}

	cloneFork := graph.New[int]("fork")
	node(t, cloneFork, "fork", func(_ context.Context, v int) (graph.Transition[int], error) { return graph.To(v, "a", "b"), nil })
	for _, name := range []string{"a", "b"} {
		node(t, cloneFork, name, func(_ context.Context, v int) (graph.Transition[int], error) { return graph.EndBranch(v), nil })
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
	out, err = cloneRunner.Start(context.Background(), "fanout-clone", 0, graph.Options[int]{})
	if !errors.As(err, &panicErr) || out.Checkpoint.Revision != 1 || out.Checkpoint.Steps != 0 {
		t.Fatalf("fanout clone panic = %+v, %v", out, err)
	}

	fork := graph.New[int]("fork")
	node(t, fork, "fork", func(_ context.Context, v int) (graph.Transition[int], error) { return graph.To(v, "a", "b"), nil })
	if err := fork.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(context.Context, []int) (int, error) { panic("merge") }}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		node(t, fork, name, func(_ context.Context, v int) (graph.Transition[int], error) { return graph.To(v, "join"), nil })
		edge(t, fork, "fork", name)
		edge(t, fork, name, "join")
	}
	out, err = intRunner(t, fork).Start(context.Background(), "merge", 0, graph.Options[int]{MaxConcurrency: 1})
	if err != nil || out.Status != graph.StatusCompletedWithFailures || len(out.Checkpoint.Failures) != 1 || !strings.Contains(out.Checkpoint.Failures[0].PanicStack, "TestCloneAndMergePanics") {
		t.Fatalf("merge panic = %+v, %v", out, err)
	}
}

func TestWrongJoinIsTransitionError(t *testing.T) {
	g := graph.New[int]("start")
	node(t, g, "start", func(_ context.Context, v int) (graph.Transition[int], error) {
		return graph.Wait(v, "input", "join"), nil
	})
	node(t, g, "other", func(_ context.Context, v int) (graph.Transition[int], error) { return graph.EndBranch(v), nil })
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "other", Merge: func(_ context.Context, values []int) (int, error) { return 0, nil }}); err != nil {
		t.Fatal(err)
	}
	edge(t, g, "start", "join")
	if err := graph.RegisterContinuation(g, "input", func([]byte) (int, error) { return 0, nil }, func(_ context.Context, state, _ int) (int, error) { return state, nil }); err != nil {
		t.Fatal(err)
	}
	out, err := intRunner(t, g).Start(context.Background(), "wrong-join", 0, graph.Options[int]{})
	var transitionErr *graph.TransitionError
	if !errors.As(err, &transitionErr) || out.Checkpoint.Revision != 1 {
		t.Fatalf("wrong join = %+v, %v", out, err)
	}
}

func TestContinuationPanics(t *testing.T) {
	for _, panicPhase := range []string{"decode", "apply"} {
		t.Run(panicPhase, func(t *testing.T) {
			g := graph.New[int]("wait")
			node(t, g, "wait", func(_ context.Context, v int) (graph.Transition[int], error) {
				return graph.Wait(v, "input", "done"), nil
			})
			node(t, g, "done", func(_ context.Context, v int) (graph.Transition[int], error) { return graph.EndExecution(v), nil })
			edge(t, g, "wait", "done")
			if err := graph.RegisterContinuation(g, "input", func([]byte) (int, error) {
				if panicPhase == "decode" {
					panic("decoder")
				}
				return 2, nil
			}, func(_ context.Context, state, value int) (int, error) {
				if panicPhase == "apply" {
					panic("apply")
				}
				return state + value, nil
			}); err != nil {
				t.Fatal(err)
			}
			r := intRunner(t, g)
			first, err := r.Start(context.Background(), "continuation-"+panicPhase, 1, graph.Options[int]{})
			if err != nil {
				t.Fatal(err)
			}
			out, err := r.Resume(context.Background(), first.Checkpoint, []graph.ResumeInput{{InvocationID: "i1", Payload: []byte("x")}}, graph.Options[int]{})
			if panicPhase == "decode" {
				var panicErr *graph.PanicError
				if !errors.As(err, &panicErr) || out.Checkpoint.Revision != first.Checkpoint.Revision || out.Checkpoint.Invocations[0].Status != graph.InvocationWaiting {
					t.Fatalf("decode panic = %+v, %v", out, err)
				}
			} else if err != nil || out.Status != graph.StatusCompletedWithFailures || len(out.Checkpoint.Failures) != 1 || out.Checkpoint.Failures[0].PanicStack == "" {
				t.Fatalf("apply panic = %+v, %v", out, err)
			}
		})
	}
}

func TestStoreCheckpointIsAuthoritative(t *testing.T) {
	g := graph.New[int]("wait")
	node(t, g, "wait", func(_ context.Context, v int) (graph.Transition[int], error) {
		return graph.Wait(v, "add", "done"), nil
	})
	node(t, g, "done", func(_ context.Context, v int) (graph.Transition[int], error) { return graph.EndExecution(v), nil })
	edge(t, g, "wait", "done")
	if err := graph.RegisterContinuation(g, "add", func([]byte) (int, error) { return 1, nil }, func(_ context.Context, state, value int) (int, error) { return state + value, nil }); err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	store := newMemoryStore(t)
	first, err := r.Start(context.Background(), "authority", 4, graph.Options[int]{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	forged := first.Checkpoint
	forged.Invocations = append([]graph.Invocation[int](nil), forged.Invocations...)
	forged.Invocations[0].State = 900
	forged.Invocations[0].Node = "bogus"
	out, err := r.Resume(context.Background(), forged, []graph.ResumeInput{{InvocationID: "i1"}}, graph.Options[int]{Store: store})
	if err != nil || out.Checkpoint.Final == nil || *out.Checkpoint.Final != 5 {
		t.Fatalf("authoritative store = %+v, %v", out, err)
	}
	if _, err := r.Resume(context.Background(), first.Checkpoint, nil, graph.Options[int]{Store: store}); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("stale revision = %v", err)
	}
}

func TestMalformedCheckpointRejected(t *testing.T) {
	g := graph.New[int]("wait")
	node(t, g, "wait", func(_ context.Context, v int) (graph.Transition[int], error) {
		return graph.Wait(v, "input", "wait"), nil
	})
	edge(t, g, "wait", "wait")
	if err := graph.RegisterContinuation(g, "input", func([]byte) (int, error) { return 0, nil }, func(_ context.Context, state, _ int) (int, error) { return state, nil }); err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	first, err := r.Start(context.Background(), "validation", 0, graph.Options[int]{})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*graph.Checkpoint[int]){
		func(s *graph.Checkpoint[int]) { s.FormatVersion++ },
		func(s *graph.Checkpoint[int]) { s.Invocations[0].ID = "i99" },
		func(s *graph.Checkpoint[int]) { s.Invocations[0].BranchIndex = 1 },
		func(s *graph.Checkpoint[int]) {
			s.Invocations[0].Status = graph.InvocationGroup
			s.Invocations[0].ChildGroupID = "g9"
		},
		func(s *graph.Checkpoint[int]) { s.Invocations[0].Continuation = "missing" },
	} {
		bad := first.Checkpoint
		bad.Invocations = append([]graph.Invocation[int](nil), bad.Invocations...)
		mutate(&bad)
		if _, err := r.Resume(context.Background(), bad, nil, graph.Options[int]{}); !errors.Is(err, graph.ErrInvalidCheckpoint) {
			t.Fatalf("malformed checkpoint accepted: %+v, %v", bad, err)
		}
	}
}
