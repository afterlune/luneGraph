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

func TestStaticNodeTimeoutWithFailInvocation(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "fast", "slow"), nil
	})
	if err := g.AddNode(graph.NodeSpec[int]{
		Name:    "fast",
		Timeout: 500 * time.Millisecond,
		OnError: graph.FailInvocation,
		Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.To(state+10, "join"), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode(graph.NodeSpec[int]{
		Name:    "slow",
		Timeout: 20 * time.Millisecond,
		OnError: graph.FailInvocation,
		Run: func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
			select {
			case <-ctx.Done():
				return graph.Transition[int]{}, ctx.Err()
			case <-time.After(1 * time.Second):
				return graph.To(99, "join"), nil
			}
		},
	}); err != nil {
		t.Fatal(err)
	}

	var mergeResults []graph.BranchResult[int]
	if err := g.AddJoin(graph.JoinSpec[int]{
		Name:    "join",
		From:    "fork",
		OnError: graph.FailExecution,
		Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
			mergeResults = append([]graph.BranchResult[int](nil), branches...)
			sum := parent
			for _, b := range branches {
				if b.OK() {
					sum += b.State
				}
			}
			return sum, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})

	edge(t, g, "fork", "fast")
	edge(t, g, "fork", "slow")
	edge(t, g, "fast", "join")
	edge(t, g, "slow", "join")
	edge(t, g, "join", "done")

	runner := intRunner(t, g)
	_, res, err := runner.Start(context.Background(), "static-timeout-test", 100, nil, graph.Options{MaxConcurrency: 2})
	if err != nil || res.Status != graph.StatusCompletedWithFailures || res.Final == nil || *res.Final != 210 {
		t.Fatalf("start = %+v, %v", res, err)
	}
	if len(mergeResults) != 2 {
		t.Fatalf("expected 2 merge branch results, got %d", len(mergeResults))
	}
	if !mergeResults[0].OK() || mergeResults[0].State != 110 {
		t.Fatalf("unexpected fast branch result: %+v", mergeResults[0])
	}
	if !mergeResults[1].Failed() || mergeResults[1].Error != context.DeadlineExceeded.Error() {
		t.Fatalf("unexpected slow branch result: %+v", mergeResults[1])
	}
}

func TestDynamicBranchTimeoutOverride(t *testing.T) {
	g := graph.New[int]("dispatch")
	node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, parent int) (graph.Transition[int], error) {
		return graph.FanOut(parent,
			graph.Branch[int]{Target: "worker", State: 10, Timeout: 500 * time.Millisecond},
			graph.Branch[int]{Target: "worker", State: 20, Timeout: 20 * time.Millisecond},
		), nil
	})
	if err := g.AddNode(graph.NodeSpec[int]{
		Name:    "worker",
		OnError: graph.FailInvocation,
		Run: func(ctx context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			if state == 10 {
				return graph.CompleteBranch(state + 1), nil
			}
			select {
			case <-ctx.Done():
				return graph.Transition[int]{}, ctx.Err()
			case <-time.After(1 * time.Second):
				return graph.CompleteBranch(state + 2), nil
			}
		},
	}); err != nil {
		t.Fatal(err)
	}

	var mergeBranches []graph.BranchResult[int]
	if err := g.AddJoin(graph.JoinSpec[int]{
		Name:    "joined",
		From:    "dispatch",
		OnError: graph.FailExecution,
		Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
			mergeBranches = append([]graph.BranchResult[int](nil), branches...)
			values := graph.BranchValues(branches)
			sum := parent
			for _, v := range values {
				sum += v
			}
			return sum, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})

	edge(t, g, "dispatch", "worker")
	edge(t, g, "joined", "done")

	runner := intRunner(t, g)
	_, res, err := runner.Start(context.Background(), "dynamic-branch-timeout-test", 100, nil, graph.Options{MaxConcurrency: 2})
	if err != nil || res.Status != graph.StatusCompletedWithFailures || res.Final == nil || *res.Final != 111 {
		t.Fatalf("start = %+v, %v", res, err)
	}
	if len(mergeBranches) != 2 {
		t.Fatalf("expected 2 merge branches, got %d", len(mergeBranches))
	}
	if !mergeBranches[0].OK() || mergeBranches[0].State != 11 {
		t.Fatalf("branch 0 did not succeed: %+v", mergeBranches[0])
	}
	if !mergeBranches[1].Failed() || mergeBranches[1].Error != context.DeadlineExceeded.Error() {
		t.Fatalf("branch 1 did not timeout properly: %+v", mergeBranches[1])
	}
}

func TestNodeTimeoutEscapeDetection(t *testing.T) {
	// A node that ignores ctx and returns nil error after sleeping past its deadline
	// must be caught by the runtime and reported as DeadlineExceeded.
	g := graph.New[int]("start")
	if err := g.AddNode(graph.NodeSpec[int]{
		Name:    "start",
		Timeout: 10 * time.Millisecond,
		OnError: graph.FailExecution,
		Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			time.Sleep(30 * time.Millisecond)
			return graph.EndExecution(state + 1), nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	runner := intRunner(t, g)
	_, res, err := runner.Start(context.Background(), "escape-detection", 0, nil, graph.Options{})
	if err == nil || res.Status != graph.StatusFailed || res.Header.Failure == nil || res.Header.Failure.Message != context.DeadlineExceeded.Error() {
		t.Fatalf("expected failure from timeout escape: %+v, %v", res, err)
	}
}

func TestJoinTimeoutWithFailExecution(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "a", "b"), nil
	})
	node(t, g, "a", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state+1, "join"), nil
	})
	node(t, g, "b", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state+2, "join"), nil
	})
	if err := g.AddJoin(graph.JoinSpec[int]{
		Name:    "join",
		From:    "fork",
		Timeout: 20 * time.Millisecond,
		OnError: graph.FailExecution,
		Merge: func(ctx context.Context, _ graph.CallInfo, _ int, _ []graph.BranchResult[int]) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(1 * time.Second):
				return 99, nil
			}
		},
	}); err != nil {
		t.Fatal(err)
	}

	edge(t, g, "fork", "a")
	edge(t, g, "fork", "b")
	edge(t, g, "a", "join")
	edge(t, g, "b", "join")

	runner := intRunner(t, g)
	_, res, err := runner.Start(context.Background(), "join-timeout", 0, nil, graph.Options{MaxConcurrency: 2})
	if err == nil || res.Status != graph.StatusFailed || res.Header.Failure == nil || res.Header.Failure.Message != context.DeadlineExceeded.Error() {
		t.Fatalf("expected join failure on timeout: %+v, %v", res, err)
	}
}

type rejectTimeoutStore struct {
	graph.Store[int]
	reject atomic.Bool
}

func (s *rejectTimeoutStore) Commit(ctx context.Context, expected uint64, change graph.Change[int]) error {
	if len(change.DeleteGroups) != 0 && s.reject.Swap(false) {
		return graph.ErrConflict
	}
	return s.Store.Commit(ctx, expected, change)
}

func TestTimeoutPersistenceWithSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "timeout_test.db")
	store, err := sqlite.Open(context.Background(), dbPath, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	g := graph.New[int]("dispatch")
	node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, parent int) (graph.Transition[int], error) {
		return graph.FanOut(parent,
			graph.Branch[int]{Target: "worker", State: 10, Timeout: 500 * time.Millisecond},
			graph.Branch[int]{Target: "worker", State: 20, Timeout: 25 * time.Millisecond},
		), nil
	})
	if err := g.AddNode(graph.NodeSpec[int]{
		Name:    "worker",
		OnError: graph.FailInvocation,
		Run: func(ctx context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			if state == 10 {
				return graph.CompleteBranch(state + 5), nil
			}
			select {
			case <-ctx.Done():
				return graph.Transition[int]{}, ctx.Err()
			case <-time.After(1 * time.Second):
				return graph.CompleteBranch(state + 5), nil
			}
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddJoin(graph.JoinSpec[int]{
		Name:    "joined",
		From:    "dispatch",
		OnError: graph.FailExecution,
		Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
			values := graph.BranchValues(branches)
			return parent + values[0], nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, g, "dispatch", "worker")
	edge(t, g, "joined", "done")

	runner := intRunner(t, g)
	rejectStore := &rejectTimeoutStore{Store: store}
	rejectStore.reject.Store(true)

	_, res, err := runner.Start(context.Background(), "sqlite-timeout-run", 100, rejectStore, graph.Options{MaxConcurrency: 2})
	if !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("expected ErrConflict from rejected join commit, got: %+v, %v", res, err)
	}

	// Verify loaded checkpoint has Error and Timeout persisted
	cp, err := store.Load(context.Background(), "sqlite-timeout-run")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	var failedInv *graph.Invocation[int]
	for i := range cp.Invocations {
		if cp.Invocations[i].Status == graph.InvocationFailed {
			failedInv = &cp.Invocations[i]
			break
		}
	}
	if failedInv == nil {
		t.Fatalf("missing failed invocation in sqlite checkpoint: %+v", cp.Invocations)
	}
	var failedGroup *graph.ActivationGroup
	for i := range cp.Groups {
		if cp.Groups[i].ID == failedInv.GroupID {
			failedGroup = &cp.Groups[i]
			break
		}
	}
	if failedGroup == nil || failedInv.BranchIndex < 0 || failedInv.BranchIndex >= len(failedGroup.Branches) {
		t.Fatalf("failed invocation has no activation branch metadata: %+v / %+v", failedInv, cp.Groups)
	}
	failedBranch := failedGroup.Branches[failedInv.BranchIndex]
	if failedBranch.InvocationID != failedInv.ID || failedBranch.Error != context.DeadlineExceeded.Error() {
		t.Fatalf("persisted branch error mismatch: %+v", failedBranch)
	}
	if failedBranch.Timeout != 25*time.Millisecond {
		t.Fatalf("persisted branch timeout mismatch: got %v, want 25ms", failedBranch.Timeout)
	}

	// Replay recovery
	_, recovered, err := runner.Recover(context.Background(), "sqlite-timeout-run", nil, rejectStore, graph.Options{MaxConcurrency: 2})
	if err != nil || recovered.Status != graph.StatusCompletedWithFailures || recovered.Final == nil || *recovered.Final != 115 {
		t.Fatalf("recover failed: %+v, %v", recovered, err)
	}
}
