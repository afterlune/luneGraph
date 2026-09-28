package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/sqlite"
)

func TestRunnerResumesAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runs.db")
	store, err := sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	g := graph.New[int]("wait")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "wait", Run: func(_ context.Context, value int) (graph.Transition[int], error) {
		return graph.Wait(value, "add", "done"), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode(graph.NodeSpec[int]{Name: "done", Run: func(_ context.Context, value int) (graph.Transition[int], error) {
		return graph.EndExecution(value), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("wait", "done"); err != nil {
		t.Fatal(err)
	}
	if err := graph.RegisterContinuation(g, "add", func([]byte) (int, error) { return 2, nil }, func(_ context.Context, state, input int) (int, error) {
		return state + input, nil
	}); err != nil {
		t.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[int]{MachineID: "wait-v1", Clone: func(value int) (int, error) { return value, nil }})
	if err != nil {
		t.Fatal(err)
	}
	first, err := runner.Start(ctx, "waiting", 3, graph.Options[int]{Store: store})
	if err != nil || first.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", first, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	last, err := runner.Recover(ctx, "waiting", []graph.ResumeInput{{InvocationID: "i1"}}, graph.Options[int]{Store: store})
	if err != nil || last.Status != graph.StatusCompleted || last.Checkpoint.Final == nil || *last.Checkpoint.Final != 5 {
		t.Fatalf("recover after reopen = %+v, %v", last, err)
	}
	finished, err := runner.Recover(ctx, "waiting", nil, graph.Options[int]{Store: store})
	if err != nil || finished.Status != graph.StatusCompleted || finished.Checkpoint.Revision != last.Checkpoint.Revision {
		t.Fatalf("completed run after reopen = %+v, %v", finished, err)
	}

	loop := graph.New[int]("loop")
	if err := loop.AddNode(graph.NodeSpec[int]{Name: "loop", Run: func(_ context.Context, value int) (graph.Transition[int], error) {
		if value == 2 {
			return graph.EndExecution(value), nil
		}
		return graph.To(value+1, "loop"), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := loop.AddEdge("loop", "loop"); err != nil {
		t.Fatal(err)
	}
	loopRunner, err := loop.Compile(graph.Config[int]{MachineID: "loop-v1", Clone: func(value int) (int, error) { return value, nil }})
	if err != nil {
		t.Fatal(err)
	}
	budgeted, err := loopRunner.Start(ctx, "budgeted", 0, graph.Options[int]{Store: store, MaxSteps: 1})
	if err != nil || budgeted.Status != graph.StatusBudget {
		t.Fatalf("budgeted start = %+v, %v", budgeted, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := loopRunner.Recover(ctx, "budgeted", nil, graph.Options[int]{Store: store})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Checkpoint.Final == nil || *completed.Checkpoint.Final != 2 {
		t.Fatalf("budgeted recover after reopen = %+v, %v", completed, err)
	}
}
