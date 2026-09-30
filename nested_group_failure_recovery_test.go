package graph_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func TestNestedFailGroupRecoveryAfterSQLiteReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const (
		runID     = "nested-group-failure"
		machineID = "nested-group-failure-v1"
	)
	dbPath := filepath.Join(t.TempDir(), "runs.db")
	boom := errors.New("nested branch failed")
	workersStarted := make(chan struct{}, 2)
	workersExited := make(chan error, 2)
	var workerStartCount atomic.Int32
	var workerExitCount atomic.Int32
	var failureCallCount atomic.Int32
	var mergeCallCount atomic.Int32
	var mergedStates [][]int

	g := graph.New[int]("root")
	node(t, g, "root", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "inner", "survivor"), nil
	})
	node(t, g, "inner", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "deep", "failer"), nil
	})
	node(t, g, "deep", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "worker-a", "worker-b"), nil
	})
	worker := func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		workerStartCount.Add(1)
		workersStarted <- struct{}{}
		<-ctx.Done()
		workerExitCount.Add(1)
		exitErr := ctx.Err()
		workersExited <- exitErr
		return graph.Transition[int]{}, exitErr
	}
	node(t, g, "worker-a", worker)
	node(t, g, "worker-b", worker)
	if err := g.AddNode(graph.NodeSpec[int]{
		Name:    "failer",
		OnError: graph.FailGroup,
		Run: func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
			failureCallCount.Add(1)
			for range 2 {
				select {
				case <-workersStarted:
				case <-ctx.Done():
					return graph.Transition[int]{}, ctx.Err()
				}
			}
			return graph.Transition[int]{}, boom
		},
	}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "survivor", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state+10, "outer-join"), nil
	})
	if err := g.AddJoin(graph.JoinSpec[int]{
		Name: "outer-join",
		From: "root",
		Merge: func(_ context.Context, _ graph.CallInfo, states []int) (int, error) {
			mergeCallCount.Add(1)
			mergedStates = append(mergedStates, append([]int(nil), states...))
			if len(states) != 1 {
				return 0, errors.New("outer join received unexpected branch states")
			}
			return states[0], nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	for _, edgePair := range [][2]string{
		{"root", "inner"},
		{"root", "survivor"},
		{"inner", "deep"},
		{"inner", "failer"},
		{"deep", "worker-a"},
		{"deep", "worker-b"},
		{"survivor", "outer-join"},
		{"outer-join", "done"},
	} {
		edge(t, g, edgePair[0], edgePair[1])
	}
	compile := func() *graph.Runner[int] {
		t.Helper()
		runner, err := g.Compile(graph.Config[int]{MachineID: machineID, Clone: func(state int) (int, error) { return state, nil }})
		if err != nil {
			t.Fatal(err)
		}
		return runner
	}
	runner := compile()

	store, err := sqlite.Open(ctx, dbPath, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	first, err := runner.Start(ctx, runID, 7, graph.Options[int]{Store: store, MaxSteps: 1, MaxConcurrency: 4})
	if err != nil || first.Status != graph.StatusBudget || len(first.Checkpoint.Groups) != 1 {
		t.Fatalf("initial fan-out = status %s, groups %d, error %v", first.Status, len(first.Checkpoint.Groups), err)
	}
	checkpointWithNestedGroup, err := runner.Recover(ctx, runID, nil, graph.Options[int]{Store: store, MaxSteps: 1, MaxConcurrency: 4})
	if err != nil || checkpointWithNestedGroup.Status != graph.StatusBudget || len(checkpointWithNestedGroup.Checkpoint.Groups) != 2 {
		t.Fatalf("nested fan-out = status %s, groups %d, error %v", checkpointWithNestedGroup.Status, len(checkpointWithNestedGroup.Checkpoint.Groups), err)
	}
	var nestedParentFound bool
	for _, invocation := range checkpointWithNestedGroup.Checkpoint.Invocations {
		if invocation.Node == "inner" && invocation.Status == graph.InvocationGroup && invocation.ChildGroupID != "" {
			nestedParentFound = true
			break
		}
	}
	if !nestedParentFound {
		t.Fatalf("checkpoint did not retain nested activation parent: %+v", checkpointWithNestedGroup.Checkpoint.Invocations)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil

	store, err = sqlite.Open(ctx, dbPath, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Load(ctx, runID)
	if err != nil || reopened.Revision != checkpointWithNestedGroup.Checkpoint.Revision || len(reopened.Groups) != 2 {
		t.Fatalf("reopened nested checkpoint = revision %d, groups %d, error %v", reopened.Revision, len(reopened.Groups), err)
	}
	var reopenedNestedParent bool
	for _, invocation := range reopened.Invocations {
		if invocation.Node == "inner" && invocation.Status == graph.InvocationGroup && invocation.ChildGroupID != "" {
			reopenedNestedParent = true
			break
		}
	}
	if !reopenedNestedParent {
		t.Fatalf("SQLite lost nested activation parent: %+v", reopened.Invocations)
	}
	runner = compile()
	result, err := runner.Recover(ctx, runID, nil, graph.Options[int]{Store: store, MaxConcurrency: 4})
	if err != nil || result.Status != graph.StatusCompletedWithFailures || result.Checkpoint.Final == nil || *result.Checkpoint.Final != 17 {
		t.Fatalf("nested group recovery = status %s, final %v, error %v", result.Status, result.Checkpoint.Final, err)
	}
	if len(result.Checkpoint.Failures) != 1 || result.Checkpoint.Failures[0].Node != "failer" || result.Checkpoint.Failures[0].Scope != graph.FailGroup || result.Checkpoint.Failures[0].Message != boom.Error() {
		t.Fatalf("nested group failure record = %+v", result.Checkpoint.Failures)
	}
	if !result.Checkpoint.Completed || len(result.Checkpoint.Groups) != 0 || len(result.Checkpoint.Invocations) != 0 {
		t.Fatalf("completed checkpoint retained execution paths: %+v", result.Checkpoint)
	}
	if mergeCallCount.Load() != 1 || len(mergedStates) != 1 || len(mergedStates[0]) != 1 || mergedStates[0][0] != 17 {
		t.Fatalf("outer join calls=%d states=%v", mergeCallCount.Load(), mergedStates)
	}
	if failureCallCount.Load() != 1 || workerStartCount.Load() != 2 || workerExitCount.Load() != 2 {
		t.Fatalf("callback counts: failer=%d workers started=%d exited=%d", failureCallCount.Load(), workerStartCount.Load(), workerExitCount.Load())
	}
	for range 2 {
		select {
		case exitErr := <-workersExited:
			if !errors.Is(exitErr, context.Canceled) {
				t.Fatalf("nested worker exited without group cancellation: %v", exitErr)
			}
		case <-ctx.Done():
			t.Fatalf("nested worker was not canceled and drained: %v", ctx.Err())
		}
	}
	stored, err := store.Load(ctx, runID)
	if err != nil || !stored.Completed || stored.Revision != result.Checkpoint.Revision || stored.Final == nil || *stored.Final != 17 || len(stored.Groups) != 0 || len(stored.Invocations) != 0 || len(stored.Failures) != 1 || stored.Failures[0].Scope != graph.FailGroup {
		t.Fatalf("stored final checkpoint = %+v, error %v", stored, err)
	}
	again, err := runner.Recover(ctx, runID, nil, graph.Options[int]{Store: store, MaxConcurrency: 4})
	if err != nil || again.Status != graph.StatusCompletedWithFailures || again.Checkpoint.Revision != stored.Revision || failureCallCount.Load() != 1 || mergeCallCount.Load() != 1 {
		t.Fatalf("completed recovery replayed callbacks: status %s, revision %d, error %v", again.Status, again.Checkpoint.Revision, err)
	}
}
