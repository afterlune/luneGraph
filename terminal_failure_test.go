package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	graph "github.com/afterlune/luneGraph"
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
			firstExecution, first, err := runner.Start(context.Background(), "failed", 1, store, graph.Options{})
			if !errors.Is(err, boom) || first.Status != graph.StatusFailed || first.Header.Revision != 2 || first.Header.Steps != 1 || !first.Header.Completed || first.Final != nil || len(mustCheckpoint(firstExecution).Invocations) != 0 || len(mustCheckpoint(firstExecution).Groups) != 0 || first.Header.Failure == nil {
				t.Fatalf("failed result = %+v, %v", first, err)
			}
			loaded, err := store.Load(context.Background(), "failed")
			if err != nil || loaded.Revision != 2 || !loaded.Completed {
				t.Fatalf("stored failure = %+v, %v", loaded, err)
			}
			_, recovered, err := runner.Recover(context.Background(), "failed", nil, store, graph.Options{})
			if !errors.Is(err, graph.ErrRunFailed) || errors.Is(err, boom) || recovered.Status != graph.StatusFailed || recovered.Header.Revision != 2 || !strings.Contains(err.Error(), boom.Error()) || calls != 1 {
				t.Fatalf("recovered failure = %+v, %v; calls=%d", recovered, err, calls)
			}
			if _, _, err := runner.Resume(context.Background(), loaded, nil, store, graph.Options{}); !errors.Is(err, graph.ErrRunCompleted) {
				t.Fatalf("resumed terminal failure = %v", err)
			}
			_, withInput, err := runner.Recover(context.Background(), "failed", []graph.ResumeInput{{InvocationID: "i1"}}, store, graph.Options{})
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
	if _, _, err := runner.Start(context.Background(), "malformed-failure", 0, store, graph.Options{}); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		alter func(*graph.Checkpoint[int])
	}{
		{"not completed", func(value *graph.Checkpoint[int]) { value.Completed = false }},
		{"invalid failure ID", func(value *graph.Checkpoint[int]) { value.Failure.InvocationID = "bad" }},
		{"unknown failure node", func(value *graph.Checkpoint[int]) { value.Failure.Node = "missing" }},
		{"final result", func(value *graph.Checkpoint[int]) { final := 1; value.Final = &final }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := &alteredLoadStore{Store: store, alter: tc.alter}
			if _, _, err := runner.Recover(context.Background(), "malformed-failure", nil, corrupt, graph.Options{}); !errors.Is(err, graph.ErrInvalidCheckpoint) {
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
	_, failed, err := runner.Start(context.Background(), "override", 0, store, graph.Options{FailureOverride: &scope})
	if !errors.Is(err, boom) || !failed.Header.Completed || failed.Header.Failure == nil {
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
	firstExecution, first, err := runner.Start(context.Background(), "apply", 0, store, graph.Options{})
	if err != nil || first.Status != graph.StatusWaiting || first.Header.Steps != 1 {
		t.Fatalf("wait = %+v, %v", first, err)
	}
	_, failed, err := runner.Resume(context.Background(), mustCheckpoint(firstExecution), []graph.ResumeInput{{InvocationID: "i1", CallID: firstExecution.Pending()[0].CallID}}, store, graph.Options{})
	if !errors.Is(err, boom) || failed.Status != graph.StatusFailed || failed.Header.Revision != first.Header.Revision+1 || failed.Header.Steps != first.Header.Steps || !failed.Header.Completed || failed.Header.Failure == nil || failed.Header.Failure.Node != "wait" {
		t.Fatalf("apply failure = %+v, %v", failed, err)
	}
	if _, _, err := runner.Recover(context.Background(), "apply", nil, store, graph.Options{}); !errors.Is(err, graph.ErrRunFailed) {
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
	_, failed, err := runner.Start(context.Background(), "join-failure", 0, store, graph.Options{MaxConcurrency: 1})
	if !errors.Is(err, boom) || failed.Status != graph.StatusFailed || failed.Header.Revision != 5 || failed.Header.Steps != 3 || !failed.Header.Completed || failed.Header.Failure == nil || failed.Header.Failure.Node != "join" {
		t.Fatalf("join failure = %+v, %v", failed, err)
	}
	if _, _, err := runner.Recover(context.Background(), "join-failure", nil, store, graph.Options{}); !errors.Is(err, graph.ErrRunFailed) {
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
	_, failed, err := runner.Start(context.Background(), "panic-failure", 0, store, graph.Options{})
	var panicErr *graph.PanicError
	if !errors.Is(err, boom) || !errors.As(err, &panicErr) || failed.Header.Failure.PanicStack == "" {
		t.Fatalf("panic result = %+v, %v", failed, err)
	}
	_, recovered, err := runner.Recover(context.Background(), "panic-failure", nil, store, graph.Options{})
	if !errors.Is(err, graph.ErrRunFailed) || errors.As(err, &panicErr) || recovered.Header.Failure.PanicStack == "" {
		t.Fatalf("recovered panic = %+v, %v", recovered, err)
	}
}
