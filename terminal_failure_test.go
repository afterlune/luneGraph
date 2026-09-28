package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	graph "lune-graph"
)

func TestExecutionFailureIsPersisted(t *testing.T) {
	for _, scope := range []graph.FailureScope{graph.FailExecution, graph.FailGroup} {
		t.Run(fmt.Sprint(scope), func(t *testing.T) {
			boom := errors.New("node failed")
			calls := 0
			g := graph.New[int]("work")
			if err := g.AddNode(graph.NodeSpec[int]{Name: "work", OnError: scope, Run: func(context.Context, graph.CallInfo, int) (graph.Transition[int], error) {
				calls++
				return graph.Transition[int]{}, boom
			}}); err != nil {
				t.Fatal(err)
			}
			runner := intRunner(t, g)
			store := newMemoryStore(t)
			first, err := runner.Start(context.Background(), "failed", 1, graph.Options[int]{Store: store})
			if !errors.Is(err, boom) || first.Status != graph.StatusFailed || first.Checkpoint.Revision != 2 || first.Checkpoint.Steps != 1 || !first.Checkpoint.Completed || first.Checkpoint.Final != nil || len(first.Checkpoint.Invocations) != 0 || len(first.Checkpoint.Groups) != 0 || len(first.Checkpoint.Failures) != 1 || first.Checkpoint.Failures[0].Scope != graph.FailExecution {
				t.Fatalf("failed result = %+v, %v", first, err)
			}
			loaded, err := store.Load(context.Background(), "failed")
			if err != nil || loaded.Revision != 2 || !loaded.Completed {
				t.Fatalf("stored failure = %+v, %v", loaded, err)
			}
			recovered, err := runner.Recover(context.Background(), "failed", nil, graph.Options[int]{Store: store})
			if !errors.Is(err, graph.ErrRunFailed) || errors.Is(err, boom) || recovered.Status != graph.StatusFailed || recovered.Checkpoint.Revision != 2 || !strings.Contains(err.Error(), boom.Error()) || calls != 1 {
				t.Fatalf("recovered failure = %+v, %v; calls=%d", recovered, err, calls)
			}
			if _, err := runner.Resume(context.Background(), loaded, nil, graph.Options[int]{Store: store}); !errors.Is(err, graph.ErrInvalidCheckpoint) {
				t.Fatalf("resumed terminal failure = %v", err)
			}
			withInput, err := runner.Recover(context.Background(), "failed", []graph.ResumeInput{{InvocationID: "i1"}}, graph.Options[int]{Store: store})
			if !errors.Is(err, graph.ErrRunCompleted) || withInput.Status != graph.StatusFailed || calls != 1 {
				t.Fatalf("input to failed run = %+v, %v", withInput, err)
			}
		})
	}
}

func TestRejectMalformedFailedCheckpoint(t *testing.T) {
	boom := errors.New("failure")
	g := graph.New[int]("work")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "work", OnError: graph.FailExecution, Run: func(context.Context, graph.CallInfo, int) (graph.Transition[int], error) {
		return graph.Transition[int]{}, boom
	}}); err != nil {
		t.Fatal(err)
	}
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	if _, err := runner.Start(context.Background(), "malformed-failure", 0, graph.Options[int]{Store: store}); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		alter func(*graph.Checkpoint[int])
	}{
		{"not completed", func(value *graph.Checkpoint[int]) { value.Completed = false }},
		{"no failure", func(value *graph.Checkpoint[int]) { value.Failures = nil }},
		{"duplicate terminal failure", func(value *graph.Checkpoint[int]) { value.Failures = append(value.Failures, value.Failures[0]) }},
		{"final result", func(value *graph.Checkpoint[int]) { final := 1; value.Final = &final }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := &alteredLoadStore{Store: store, alter: tc.alter}
			if _, err := runner.Recover(context.Background(), "malformed-failure", nil, graph.Options[int]{Store: corrupt}); !errors.Is(err, graph.ErrInvalidCheckpoint) {
				t.Fatalf("malformed terminal = %v", err)
			}
		})
	}
}

func TestFailureOverridePersistsTerminal(t *testing.T) {
	boom := errors.New("override failure")
	g := graph.New[int]("work")
	node(t, g, "work", func(context.Context, graph.CallInfo, int) (graph.Transition[int], error) {
		return graph.Transition[int]{}, boom
	})
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	scope := graph.FailExecution
	failed, err := runner.Start(context.Background(), "override", 0, graph.Options[int]{Store: store, FailureOverride: &scope})
	if !errors.Is(err, boom) || !failed.Checkpoint.Completed || failed.Checkpoint.Failures[0].Scope != graph.FailExecution {
		t.Fatalf("overridden failure = %+v, %v", failed, err)
	}
}

func TestContinuationExecutionFailureIsPersisted(t *testing.T) {
	boom := errors.New("apply failed")
	g := graph.New[int]("wait")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "wait", OnError: graph.FailExecution, Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Wait(state, "input", "done"), nil
	}}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, g, "wait", "done")
	if err := graph.RegisterContinuation(g, "input", func([]byte) (int, error) { return 1, nil }, func(context.Context, graph.CallInfo, int, int) (int, error) {
		return 0, boom
	}); err != nil {
		t.Fatal(err)
	}
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	first, err := runner.Start(context.Background(), "apply", 0, graph.Options[int]{Store: store})
	if err != nil || first.Status != graph.StatusWaiting || first.Checkpoint.Steps != 1 {
		t.Fatalf("wait = %+v, %v", first, err)
	}
	failed, err := runner.Resume(context.Background(), first.Checkpoint, []graph.ResumeInput{{InvocationID: "i1"}}, graph.Options[int]{Store: store})
	if !errors.Is(err, boom) || failed.Status != graph.StatusFailed || failed.Checkpoint.Revision != first.Checkpoint.Revision+1 || failed.Checkpoint.Steps != first.Checkpoint.Steps || !failed.Checkpoint.Completed || len(failed.Checkpoint.Failures) != 1 || failed.Checkpoint.Failures[0].Node != "wait" {
		t.Fatalf("apply failure = %+v, %v", failed, err)
	}
	if _, err := runner.Recover(context.Background(), "apply", nil, graph.Options[int]{Store: store}); !errors.Is(err, graph.ErrRunFailed) {
		t.Fatalf("recovered continuation failure = %v", err)
	}
}

func TestJoinExecutionFailureIsPersisted(t *testing.T) {
	boom := errors.New("merge failed")
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "a", "b"), nil
	})
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", OnError: graph.FailExecution, Merge: func(context.Context, graph.CallInfo, []int) (int, error) {
		return 0, boom
	}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.To(state, "join"), nil
		})
		edge(t, g, "fork", name)
		edge(t, g, name, "join")
	}
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	failed, err := runner.Start(context.Background(), "join-failure", 0, graph.Options[int]{Store: store, MaxConcurrency: 1})
	if !errors.Is(err, boom) || failed.Status != graph.StatusFailed || failed.Checkpoint.Revision != 4 || failed.Checkpoint.Steps != 3 || !failed.Checkpoint.Completed || len(failed.Checkpoint.Failures) != 1 || failed.Checkpoint.Failures[0].Node != "join" || failed.Checkpoint.Failures[0].Scope != graph.FailExecution {
		t.Fatalf("join failure = %+v, %v", failed, err)
	}
	if _, err := runner.Recover(context.Background(), "join-failure", nil, graph.Options[int]{Store: store}); !errors.Is(err, graph.ErrRunFailed) {
		t.Fatalf("recovered join failure = %v", err)
	}
}

func TestExecutionPanicKeepsOriginalErrorAndStoredStack(t *testing.T) {
	boom := errors.New("panic cause")
	g := graph.New[int]("panic")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "panic", OnError: graph.FailExecution, Run: func(context.Context, graph.CallInfo, int) (graph.Transition[int], error) {
		panic(boom)
	}}); err != nil {
		t.Fatal(err)
	}
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	failed, err := runner.Start(context.Background(), "panic-failure", 0, graph.Options[int]{Store: store})
	var panicErr *graph.PanicError
	if !errors.Is(err, boom) || !errors.As(err, &panicErr) || failed.Checkpoint.Failures[0].PanicStack == "" {
		t.Fatalf("panic result = %+v, %v", failed, err)
	}
	recovered, err := runner.Recover(context.Background(), "panic-failure", nil, graph.Options[int]{Store: store})
	if !errors.Is(err, graph.ErrRunFailed) || errors.As(err, &panicErr) || recovered.Checkpoint.Failures[0].PanicStack == "" {
		t.Fatalf("recovered panic = %+v, %v", recovered, err)
	}
}
