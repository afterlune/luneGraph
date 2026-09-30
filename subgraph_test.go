package graph_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestSubgraphEnterAndReturn(t *testing.T) {
	child := graph.New[int]("work")
	node(t, child, "work", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Return(state + 3), nil
	})

	parent := graph.New[int]("start")
	node(t, parent, "start", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "normalize"), nil
	})
	node(t, parent, "after", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	if err := parent.AddSubgraph("normalize", child); err != nil {
		t.Fatal(err)
	}
	edge(t, parent, "start", "normalize")
	edge(t, parent, "normalize", "after")

	out, err := intRunner(t, parent).Start(context.Background(), "subgraph", 4, graph.Options[int]{})
	if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || *out.Checkpoint.Final != 7 {
		t.Fatalf("subgraph result = %+v, %v", out, err)
	}
}

func TestSubgraphCanBeEntryAndReturnWithoutOutputEndsBranch(t *testing.T) {
	child := graph.New[int]("work")
	node(t, child, "work", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Return(state + 1), nil
	})

	parent := graph.New[int]("worker")
	if err := parent.AddSubgraph("worker", child); err != nil {
		t.Fatal(err)
	}
	out, err := intRunner(t, parent).Start(context.Background(), "subgraph-entry", 8, graph.Options[int]{})
	if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final != nil {
		t.Fatalf("subgraph entry result = %+v, %v", out, err)
	}
	if len(out.Checkpoint.Terminals) != 1 || out.Checkpoint.Terminals[0].State != 9 {
		t.Fatalf("terminals = %+v", out.Checkpoint.Terminals)
	}
}

func TestSubgraphTerminalActionsKeepTheirExecutionSemantics(t *testing.T) {
	for _, action := range []string{"branch", "execution"} {
		t.Run(action, func(t *testing.T) {
			child := graph.New[int]("finish")
			node(t, child, "finish", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
				if action == "branch" {
					return graph.EndBranch(state + 1), nil
				}
				return graph.EndExecution(state + 1), nil
			})

			root := graph.New[int]("worker")
			if err := root.AddSubgraph("worker", child); err != nil {
				t.Fatal(err)
			}
			afterCalls := 0
			node(t, root, "after", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
				afterCalls++
				return graph.EndExecution(state), nil
			})
			edge(t, root, "worker", "after")
			out, err := intRunner(t, root).Start(context.Background(), "terminal-"+action, 4, graph.Options[int]{})
			if err != nil || afterCalls != 0 {
				t.Fatalf("result = %+v, after calls=%d, err=%v", out, afterCalls, err)
			}
			if action == "branch" {
				if out.Status != graph.StatusCompleted || out.Checkpoint.Final != nil || len(out.Checkpoint.Terminals) != 1 || out.Checkpoint.Terminals[0].State != 5 {
					t.Fatalf("branch result = %+v", out)
				}
			} else if out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || *out.Checkpoint.Final != 5 {
				t.Fatalf("execution result = %+v", out)
			}
		})
	}
}

func TestNestedAndRepeatedSubgraphsHaveIndependentNames(t *testing.T) {
	shared := graph.New[int]("increment")
	node(t, shared, "increment", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Return(state + 1), nil
	})

	repeated := graph.New[int]("start")
	node(t, repeated, "start", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "first"), nil
	})
	node(t, repeated, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	for _, name := range []string{"first", "second"} {
		if err := repeated.AddSubgraph(name, shared); err != nil {
			t.Fatal(err)
		}
	}
	edge(t, repeated, "start", "first")
	edge(t, repeated, "first", "second")
	edge(t, repeated, "second", "done")
	runner := intRunner(t, repeated)

	first, err := runner.Start(context.Background(), "repeated", 0, graph.Options[int]{MaxSteps: 1})
	if err != nil || first.Status != graph.StatusBudget || first.Checkpoint.Invocations[0].Node != "first/increment" {
		t.Fatalf("first mount = %+v, %v", first, err)
	}
	second, err := runner.Resume(context.Background(), first.Checkpoint, nil, graph.Options[int]{MaxSteps: 1})
	if err != nil || second.Status != graph.StatusBudget || second.Checkpoint.Invocations[0].Node != "second/increment" {
		t.Fatalf("second mount = %+v, %v", second, err)
	}
	last, err := runner.Resume(context.Background(), second.Checkpoint, nil, graph.Options[int]{})
	if err != nil || last.Status != graph.StatusCompleted || last.Checkpoint.Final == nil || *last.Checkpoint.Final != 2 {
		t.Fatalf("repeated result = %+v, %v", last, err)
	}

	inner := graph.New[int]("increment")
	node(t, inner, "increment", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Return(state + 2), nil
	})
	outer := graph.New[int]("begin")
	node(t, outer, "begin", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "inner"), nil
	})
	node(t, outer, "finish", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Return(state + 3), nil
	})
	if err := outer.AddSubgraph("inner", inner); err != nil {
		t.Fatal(err)
	}
	edge(t, outer, "begin", "inner")
	edge(t, outer, "inner", "finish")

	root := graph.New[int]("outer")
	if err := root.AddSubgraph("outer", outer); err != nil {
		t.Fatal(err)
	}
	node(t, root, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, root, "outer", "done")
	nested, err := intRunner(t, root).Start(context.Background(), "nested-subgraph", 0, graph.Options[int]{})
	if err != nil || nested.Status != graph.StatusCompleted || nested.Checkpoint.Final == nil || *nested.Checkpoint.Final != 5 {
		t.Fatalf("nested result = %+v, %v", nested, err)
	}
}

func TestSubgraphReturnCanReachFanoutJoin(t *testing.T) {
	worker := graph.New[int]("work")
	node(t, worker, "work", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Return(state + 1), nil
	})

	root := graph.New[int]("fork")
	node(t, root, "fork", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "worker", "sibling"), nil
	})
	node(t, root, "sibling", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state+2, "joined"), nil
	})
	node(t, root, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	if err := root.AddSubgraph("worker", worker); err != nil {
		t.Fatal(err)
	}
	if err := root.AddJoin(graph.JoinSpec[int]{Name: "joined", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, states []int) (int, error) {
		if len(states) != 2 {
			return 0, errors.New("expected both branches at join")
		}
		return states[0] + states[1], nil
	}}); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"fork", "worker"}, {"fork", "sibling"}, {"worker", "joined"}, {"sibling", "joined"}, {"joined", "done"}} {
		edge(t, root, pair[0], pair[1])
	}
	out, err := intRunner(t, root).Start(context.Background(), "subgraph-join", 0, graph.Options[int]{MaxConcurrency: 2})
	if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || *out.Checkpoint.Final != 3 {
		t.Fatalf("join result = %+v, %v", out, err)
	}
}

func TestSubgraphCanContainFanoutAndJoin(t *testing.T) {
	child := graph.New[int]("fork")
	node(t, child, "fork", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "left", "right"), nil
	})
	node(t, child, "left", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state+1, "joined"), nil
	})
	node(t, child, "right", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state+2, "joined"), nil
	})
	node(t, child, "finish", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Return(state), nil
	})
	if err := child.AddJoin(graph.JoinSpec[int]{Name: "joined", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, states []int) (int, error) {
		return states[0] + states[1], nil
	}}); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"fork", "left"}, {"fork", "right"}, {"left", "joined"}, {"right", "joined"}, {"joined", "finish"}} {
		edge(t, child, pair[0], pair[1])
	}

	root := graph.New[int]("worker")
	if err := root.AddSubgraph("worker", child); err != nil {
		t.Fatal(err)
	}
	node(t, root, "after", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, root, "worker", "after")
	out, err := intRunner(t, root).Start(context.Background(), "subgraph-internal-join", 0, graph.Options[int]{MaxConcurrency: 2})
	if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || *out.Checkpoint.Final != 3 {
		t.Fatalf("subgraph join result = %+v, %v", out, err)
	}
}

func TestSubgraphContinuationRecovers(t *testing.T) {
	child := graph.New[int]("ask")
	node(t, child, "ask", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Wait(state, "add", "return"), nil
	})
	node(t, child, "return", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Return(state), nil
	})
	edge(t, child, "ask", "return")
	store := &rejectNextCASStore{Store: newMemoryStore(t)}
	var applied []graph.CallInfo
	if err := graph.RegisterContinuation(child, "add", func(payload []byte) (int, error) {
		return strconv.Atoi(string(payload))
	}, func(_ context.Context, call graph.CallInfo, state, value int) (int, error) {
		applied = append(applied, call)
		if len(applied) == 1 {
			store.reject.Store(true)
		}
		return state + value, nil
	}); err != nil {
		t.Fatal(err)
	}

	root := graph.New[int]("start")
	node(t, root, "start", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "worker"), nil
	})
	node(t, root, "after", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	if err := root.AddSubgraph("worker", child); err != nil {
		t.Fatal(err)
	}
	edge(t, root, "start", "worker")
	edge(t, root, "worker", "after")
	runner := intRunner(t, root)
	paused, err := runner.Start(context.Background(), "subgraph-wait", 4, graph.Options[int]{Store: store})
	if err != nil || paused.Status != graph.StatusWaiting {
		t.Fatalf("paused = %+v, %v", paused, err)
	}
	invocation := paused.Checkpoint.Invocations[0]
	if invocation.Node != "worker/ask" || invocation.Continuation != "worker/add" || len(invocation.Next) != 1 || invocation.Next[0] != "worker/return" {
		t.Fatalf("waiting invocation = %+v", invocation)
	}

	inputs := []graph.ResumeInput{{InvocationID: invocation.ID, Payload: []byte("6")}}
	if _, err := runner.Resume(context.Background(), paused.Checkpoint, inputs, graph.Options[int]{Store: store}); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("first resume error = %v", err)
	}
	completed, err := runner.Recover(context.Background(), "subgraph-wait", inputs, graph.Options[int]{Store: store})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Checkpoint.Final == nil || *completed.Checkpoint.Final != 10 {
		t.Fatalf("recovered = %+v, %v", completed, err)
	}
	if len(applied) != 2 || applied[0] != applied[1] || applied[0].RunID != "subgraph-wait" || applied[0].CallID == "" {
		t.Fatalf("continuation call info = %+v", applied)
	}
}

func TestSubgraphCompileRejectsInvalidComposition(t *testing.T) {
	newReturningChild := func(t *testing.T) *graph.Graph[int] {
		t.Helper()
		child := graph.New[int]("work")
		node(t, child, "work", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.Return(state), nil
		})
		return child
	}

	t.Run("multiple outputs", func(t *testing.T) {
		root := graph.New[int]("worker")
		if err := root.AddSubgraph("worker", newReturningChild(t)); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"left", "right"} {
			node(t, root, name, func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
				return graph.EndExecution(state), nil
			})
			edge(t, root, "worker", name)
		}
		if _, err := root.Compile(graph.Config[int]{MachineID: "bad-output", Clone: func(v int) (int, error) { return v, nil }}); err == nil {
			t.Fatal("compile accepted multiple subgraph outputs")
		}
	})

	t.Run("recursive reference", func(t *testing.T) {
		root := graph.New[int]("self")
		if err := root.AddSubgraph("self", root); err != nil {
			t.Fatal(err)
		}
		if _, err := root.Compile(graph.Config[int]{MachineID: "recursive", Clone: func(v int) (int, error) { return v, nil }}); err == nil {
			t.Fatal("compile accepted recursive subgraph")
		}
	})

	t.Run("qualified name collision", func(t *testing.T) {
		root := graph.New[int]("worker")
		if err := root.AddSubgraph("worker", newReturningChild(t)); err != nil {
			t.Fatal(err)
		}
		node(t, root, "worker/work", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.EndExecution(state), nil
		})
		if _, err := root.Compile(graph.Config[int]{MachineID: "collision", Clone: func(v int) (int, error) { return v, nil }}); err == nil {
			t.Fatal("compile accepted a qualified vertex collision")
		}
	})

	t.Run("qualified continuation collision", func(t *testing.T) {
		root := graph.New[int]("worker")
		child := newReturningChild(t)
		if err := root.AddSubgraph("worker", child); err != nil {
			t.Fatal(err)
		}
		decode := func(data []byte) (int, error) { return strconv.Atoi(string(data)) }
		apply := func(_ context.Context, _ graph.CallInfo, state, value int) (int, error) { return state + value, nil }
		if err := graph.RegisterContinuation(root, "worker/add", decode, apply); err != nil {
			t.Fatal(err)
		}
		if err := graph.RegisterContinuation(child, "add", decode, apply); err != nil {
			t.Fatal(err)
		}
		if _, err := root.Compile(graph.Config[int]{MachineID: "continuation-collision", Clone: func(v int) (int, error) { return v, nil }}); err == nil {
			t.Fatal("compile accepted a qualified continuation collision")
		}
	})
}

func TestReturnOutsideSubgraphIsTransitionError(t *testing.T) {
	root := graph.New[int]("return")
	node(t, root, "return", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Return(state), nil
	})
	out, err := intRunner(t, root).Start(context.Background(), "return-root", 0, graph.Options[int]{})
	var transitionErr *graph.TransitionError
	if !errors.As(err, &transitionErr) || out.Checkpoint.Completed {
		t.Fatalf("result = %+v, error = %v", out, err)
	}
}

func TestSubgraphCannotSelectQualifiedTargetFromAnotherScope(t *testing.T) {
	child := graph.New[int]("work")
	node(t, child, "work", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "worker/next"), nil
	})
	node(t, child, "next", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, child, "work", "next")

	root := graph.New[int]("worker")
	if err := root.AddSubgraph("worker", child); err != nil {
		t.Fatal(err)
	}
	out, err := intRunner(t, root).Start(context.Background(), "qualified-target", 0, graph.Options[int]{})
	var transitionErr *graph.TransitionError
	if !errors.As(err, &transitionErr) || out.Checkpoint.Completed {
		t.Fatalf("result = %+v, error = %v", out, err)
	}
}

func TestCompiledSubgraphIsIndependentOfLaterBuilderChanges(t *testing.T) {
	child := graph.New[int]("work")
	node(t, child, "work", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		if state == 1 {
			return graph.To(state, "late"), nil
		}
		return graph.Return(state), nil
	})

	root := graph.New[int]("worker")
	if err := root.AddSubgraph("worker", child); err != nil {
		t.Fatal(err)
	}
	node(t, root, "after", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, root, "worker", "after")
	runner := intRunner(t, root)

	node(t, child, "late", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, child, "work", "late")

	out, err := runner.Start(context.Background(), "compiled-subgraph", 1, graph.Options[int]{})
	var transitionErr *graph.TransitionError
	if !errors.As(err, &transitionErr) || out.Checkpoint.Completed {
		t.Fatalf("result = %+v, error = %v", out, err)
	}
}
