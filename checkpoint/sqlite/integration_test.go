package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/sqlite"
)

type ackLossStore struct {
	graph.Store[int]
	report error
}

func (s *ackLossStore) CompareAndSwap(ctx context.Context, expected uint64, next graph.Checkpoint[int]) error {
	if err := s.Store.CompareAndSwap(ctx, expected, next); err != nil {
		return err
	}
	if s.report != nil {
		err := s.report
		s.report = nil
		return err
	}
	return nil
}

func TestRecoverAfterSQLiteCommitAcknowledgementIsLost(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ack-lost.db")
	store, err := sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			_ = store.Close()
		}
	})

	var firstCalls, finalCalls int
	g := graph.New[int]("first")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "first", Run: func(_ context.Context, _ graph.CallInfo, value int) (graph.Transition[int], error) {
		firstCalls++
		return graph.To(value+1, "final"), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode(graph.NodeSpec[int]{Name: "final", Run: func(_ context.Context, _ graph.CallInfo, value int) (graph.Transition[int], error) {
		finalCalls++
		return graph.EndExecution(value + 1), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("first", "final"); err != nil {
		t.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[int]{MachineID: "ack-lost-v1", Clone: func(value int) (int, error) { return value, nil }})
	if err != nil {
		t.Fatal(err)
	}
	commitErr := errors.New("SQLite commit acknowledgement lost")
	wrapped := &ackLossStore{Store: store, report: commitErr}
	returned, err := runner.Start(ctx, "ack-lost", 0, graph.Options[int]{Store: wrapped})
	if !errors.Is(err, commitErr) || returned.Checkpoint.Revision != 1 || returned.Checkpoint.Completed {
		t.Fatalf("Start after lost acknowledgement = %+v, %v", returned, err)
	}
	if firstCalls != 1 || finalCalls != 0 {
		t.Fatalf("calls before reopen: first=%d final=%d", firstCalls, finalCalls)
	}
	committed, err := store.Load(ctx, "ack-lost")
	if err != nil || committed.Revision != 2 || len(committed.Invocations) != 1 || committed.Invocations[0].Node != "final" || committed.Invocations[0].Status != graph.InvocationReady {
		t.Fatalf("checkpoint after lost acknowledgement = %+v, %v", committed, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := runner.Recover(ctx, "ack-lost", nil, graph.Options[int]{Store: store})
	if err != nil || recovered.Status != graph.StatusCompleted || recovered.Checkpoint.Revision != 3 || recovered.Checkpoint.Final == nil || *recovered.Checkpoint.Final != 2 {
		t.Fatalf("recovery after reopen = %+v, %v", recovered, err)
	}
	if firstCalls != 1 || finalCalls != 1 {
		t.Fatalf("calls after recovery: first=%d final=%d", firstCalls, finalCalls)
	}
}

func TestRunnerResumesAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runs.db")
	store, err := sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	g := graph.New[int]("wait")
	var applyCall graph.CallInfo
	if err := g.AddNode(graph.NodeSpec[int]{Name: "wait", Run: func(_ context.Context, _ graph.CallInfo, value int) (graph.Transition[int], error) {
		return graph.Wait(value, "add", "done"), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode(graph.NodeSpec[int]{Name: "done", Run: func(_ context.Context, _ graph.CallInfo, value int) (graph.Transition[int], error) {
		return graph.EndExecution(value), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("wait", "done"); err != nil {
		t.Fatal(err)
	}
	if err := graph.RegisterContinuation(g, "add", func([]byte) (int, error) { return 2, nil }, func(_ context.Context, call graph.CallInfo, state, input int) (int, error) {
		applyCall = call
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
	if applyCall.CallID != first.Checkpoint.Invocations[0].CallID || applyCall.RunID != "waiting" || applyCall.InvocationID != "i1" {
		t.Fatalf("recovered continuation call info = %+v; checkpoint call ID = %q", applyCall, first.Checkpoint.Invocations[0].CallID)
	}
	finished, err := runner.Recover(ctx, "waiting", nil, graph.Options[int]{Store: store})
	if err != nil || finished.Status != graph.StatusCompleted || finished.Checkpoint.Revision != last.Checkpoint.Revision {
		t.Fatalf("completed run after reopen = %+v, %v", finished, err)
	}

	loop := graph.New[int]("loop")
	if err := loop.AddNode(graph.NodeSpec[int]{Name: "loop", Run: func(_ context.Context, _ graph.CallInfo, value int) (graph.Transition[int], error) {
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
