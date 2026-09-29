package sqlite_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/sqlite"
)

func TestSubgraphContinuationRecoversAfterReopen(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "subgraph-runs.db")
	const (
		runID     = "subgraph-sqlite-run"
		machineID = "subgraph-sqlite-v1"
	)

	buildRunner := func(applied *[]graph.CallInfo) *graph.Runner[int] {
		t.Helper()
		child := graph.New[int]("ask")
		if err := child.AddNode(graph.NodeSpec[int]{Name: "ask", Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.Wait(state, "add", "return"), nil
		}}); err != nil {
			t.Fatal(err)
		}
		if err := child.AddNode(graph.NodeSpec[int]{Name: "return", Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.Return(state), nil
		}}); err != nil {
			t.Fatal(err)
		}
		if err := child.AddEdge("ask", "return"); err != nil {
			t.Fatal(err)
		}
		if err := graph.RegisterContinuation(child, "add", func(payload []byte) (int, error) {
			return strconv.Atoi(string(payload))
		}, func(_ context.Context, call graph.CallInfo, state, value int) (int, error) {
			*applied = append(*applied, call)
			return state + value, nil
		}); err != nil {
			t.Fatal(err)
		}

		parent := graph.New[int]("start")
		if err := parent.AddNode(graph.NodeSpec[int]{Name: "start", Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.To(state, "worker"), nil
		}}); err != nil {
			t.Fatal(err)
		}
		if err := parent.AddNode(graph.NodeSpec[int]{Name: "after", Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.EndExecution(state), nil
		}}); err != nil {
			t.Fatal(err)
		}
		if err := parent.AddSubgraph("worker", child); err != nil {
			t.Fatal(err)
		}
		if err := parent.AddEdge("start", "worker"); err != nil {
			t.Fatal(err)
		}
		if err := parent.AddEdge("worker", "after"); err != nil {
			t.Fatal(err)
		}
		runner, err := parent.Compile(graph.Config[int]{MachineID: machineID, Clone: func(state int) (int, error) { return state, nil }})
		if err != nil {
			t.Fatal(err)
		}
		return runner
	}

	var firstRunnerCalls []graph.CallInfo
	firstRunner := buildRunner(&firstRunnerCalls)
	store, err := sqlite.Open(ctx, dbPath, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	paused, err := firstRunner.Start(ctx, runID, 4, graph.Options[int]{Store: store})
	if err != nil || paused.Status != graph.StatusWaiting {
		t.Fatalf("start subgraph run = %+v, %v", paused, err)
	}
	if len(paused.Checkpoint.Invocations) != 1 {
		t.Fatalf("waiting invocations = %+v", paused.Checkpoint.Invocations)
	}
	waiting := paused.Checkpoint.Invocations[0]
	if waiting.Node != "worker/ask" || waiting.Continuation != "worker/add" || len(waiting.Next) != 1 || waiting.Next[0] != "worker/return" || waiting.CallID == "" {
		t.Fatalf("persisted subgraph position = %+v", waiting)
	}
	if len(firstRunnerCalls) != 0 {
		t.Fatalf("continuation ran before resume: %+v", firstRunnerCalls)
	}
	closeErr := store.Close()
	store = nil
	if closeErr != nil {
		t.Fatal(closeErr)
	}

	var recoveredRunnerCalls []graph.CallInfo
	recoveredRunner := buildRunner(&recoveredRunnerCalls)
	store, err = sqlite.Open(ctx, dbPath, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}

	inputs := []graph.ResumeInput{{InvocationID: waiting.ID, Payload: []byte("6")}}
	completed, err := recoveredRunner.Recover(ctx, runID, inputs, graph.Options[int]{Store: store})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Checkpoint.Final == nil || *completed.Checkpoint.Final != 10 {
		t.Fatalf("recover subgraph run = %+v, %v", completed, err)
	}
	if len(recoveredRunnerCalls) != 1 {
		t.Fatalf("recovered continuation calls = %+v", recoveredRunnerCalls)
	}
	call := recoveredRunnerCalls[0]
	if call.RunID != runID || call.InvocationID != waiting.ID || call.CallID != waiting.CallID {
		t.Fatalf("recovered continuation call = %+v, checkpoint call ID = %q", call, waiting.CallID)
	}

	stored, err := store.Load(ctx, runID)
	if err != nil || !stored.Completed || stored.Final == nil || *stored.Final != 10 || stored.Revision != completed.Checkpoint.Revision {
		t.Fatalf("stored completed run = %+v, %v", stored, err)
	}
	finished, err := recoveredRunner.Recover(ctx, runID, nil, graph.Options[int]{Store: store})
	if err != nil || finished.Status != graph.StatusCompleted || finished.Checkpoint.Revision != stored.Revision || len(recoveredRunnerCalls) != 1 {
		t.Fatalf("completed recovery replayed work: %+v, calls=%+v, err=%v", finished, recoveredRunnerCalls, err)
	}
}
