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

func TestSpeculativeJoinWithQuorum(t *testing.T) {
	g := graph.New[int]("dispatch")

	var slowStarted, slowCancelled atomic.Int32

	node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, parent int) (graph.Transition[int], error) {
		return graph.FanOut(parent,
			graph.Branch[int]{Target: "fast", State: 10},
			graph.Branch[int]{Target: "fast", State: 20},
			graph.Branch[int]{Target: "slow", State: 30},
			graph.Branch[int]{Target: "slow", State: 40},
			graph.Branch[int]{Target: "slow", State: 50},
		), nil
	})

	node(t, g, "fast", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		time.Sleep(10 * time.Millisecond)
		return graph.CompleteBranch(state + 1), nil
	})

	node(t, g, "slow", func(ctx context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		slowStarted.Add(1)
		select {
		case <-ctx.Done():
			slowCancelled.Add(1)
			return graph.Transition[int]{}, ctx.Err()
		case <-time.After(2 * time.Second):
			return graph.CompleteBranch(state + 1), nil
		}
	})

	var observedBranches []graph.BranchResult[int]
	if err := g.AddJoin(graph.JoinSpec[int]{
		Name:    "joined",
		From:    "dispatch",
		Quorum:  2,
		OnError: graph.FailExecution,
		Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
			observedBranches = append([]graph.BranchResult[int](nil), branches...)
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

	edge(t, g, "dispatch", "fast")
	edge(t, g, "dispatch", "slow")
	edge(t, g, "joined", "done")

	runner := intRunner(t, g)
	start := time.Now()
	_, res, err := runner.Start(context.Background(), "speculative-quorum", 100, nil, graph.Options{MaxConcurrency: 5})
	duration := time.Since(start)

	if err != nil || res.Status != graph.StatusCompleted || res.Final == nil || *res.Final != 132 {
		t.Fatalf("unexpected start result: %+v, %v", res, err)
	}

	// Should finish quickly, well under the 2-second sleep of the slow branches
	if duration > 1*time.Second {
		t.Fatalf("speculative join took too long: %v", duration)
	}

	if len(observedBranches) != 5 {
		t.Fatalf("expected 5 branches in Merge, got %d", len(observedBranches))
	}

	var okCount, pendingCount int
	for _, b := range observedBranches {
		if b.OK() {
			okCount++
			if !b.Resolved() || b.Pending() {
				t.Fatalf("expected resolved OK branch: %+v", b)
			}
		} else if b.Pending() {
			pendingCount++
			if b.Resolved() || b.OK() || b.Failed() {
				t.Fatalf("expected pending branch: %+v", b)
			}
		}
	}

	if okCount != 2 {
		t.Fatalf("expected 2 OK branches, got %d", okCount)
	}
	if pendingCount != 3 {
		t.Fatalf("expected 3 pending branches, got %d", pendingCount)
	}

	// Pruned workers must have received cancellation
	if slowStarted.Load() == 0 {
		t.Fatal("slow workers never started")
	}
	if slowCancelled.Load() != slowStarted.Load() {
		t.Fatalf("expected all %d started slow workers to be cancelled, got %d", slowStarted.Load(), slowCancelled.Load())
	}
}

func TestSpeculativeJoinWithCanMerge(t *testing.T) {
	g := graph.New[int]("dispatch")
	var slowCancelled atomic.Bool

	node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, parent int) (graph.Transition[int], error) {
		return graph.FanOut(parent,
			graph.Branch[int]{Target: "worker", State: 10},
			graph.Branch[int]{Target: "worker", State: 200}, // Target match
			graph.Branch[int]{Target: "worker", State: 30},
		), nil
	})

	node(t, g, "worker", func(ctx context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		if state == 200 {
			time.Sleep(10 * time.Millisecond)
			return graph.CompleteBranch(state), nil
		}
		select {
		case <-ctx.Done():
			slowCancelled.Store(true)
			return graph.Transition[int]{}, ctx.Err()
		case <-time.After(2 * time.Second):
			return graph.CompleteBranch(state), nil
		}
	})

	if err := g.AddJoin(graph.JoinSpec[int]{
		Name:    "joined",
		From:    "dispatch",
		OnError: graph.FailExecution,
		CanMerge: func(_ int, branches []graph.BranchResult[int]) bool {
			for _, b := range branches {
				if b.OK() && b.State == 200 {
					return true
				}
			}
			return false
		},
		Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
			for _, b := range branches {
				if b.OK() && b.State == 200 {
					return parent + b.State, nil
				}
			}
			return parent, errors.New("missing match")
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
	start := time.Now()
	_, res, err := runner.Start(context.Background(), "can-merge-test", 50, nil, graph.Options{MaxConcurrency: 3})
	duration := time.Since(start)

	if err != nil || res.Status != graph.StatusCompleted || res.Final == nil || *res.Final != 250 {
		t.Fatalf("unexpected result: %+v, %v", res, err)
	}

	if duration > 1*time.Second {
		t.Fatalf("CanMerge took too long: %v", duration)
	}
	if !slowCancelled.Load() {
		t.Fatal("expected pending workers to be cancelled")
	}
}

func TestSpeculativeJoinFallbackWhenQuorumUnmet(t *testing.T) {
	g := graph.New[int]("dispatch")

	node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, parent int) (graph.Transition[int], error) {
		return graph.FanOut(parent,
			graph.Branch[int]{Target: "worker-ok", State: 10},
			graph.Branch[int]{Target: "worker-fail", State: 20},
			graph.Branch[int]{Target: "worker-fail", State: 30},
			graph.Branch[int]{Target: "worker-ok", State: 40},
		), nil
	})

	node(t, g, "worker-ok", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.CompleteBranch(state), nil
	})

	if err := g.AddNode(graph.NodeSpec[int]{
		Name:    "worker-fail",
		OnError: graph.FailInvocation,
		Run: func(_ context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
			return graph.Transition[int]{}, errors.New("intentional branch failure")
		},
	}); err != nil {
		t.Fatal(err)
	}

	var mergeBranches []graph.BranchResult[int]
	if err := g.AddJoin(graph.JoinSpec[int]{
		Name:    "joined",
		From:    "dispatch",
		Quorum:  3, // Requires 3, but only 2 succeed
		OnError: graph.FailExecution,
		Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
			mergeBranches = append([]graph.BranchResult[int](nil), branches...)
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

	edge(t, g, "dispatch", "worker-ok")
	edge(t, g, "dispatch", "worker-fail")
	edge(t, g, "joined", "done")

	runner := intRunner(t, g)
	_, res, err := runner.Start(context.Background(), "quorum-fallback", 100, nil, graph.Options{MaxConcurrency: 4})
	if err != nil || res.Status != graph.StatusCompletedWithFailures || res.Final == nil || *res.Final != 150 {
		t.Fatalf("unexpected fallback result: %+v, %v", res, err)
	}

	if len(mergeBranches) != 4 {
		t.Fatalf("expected 4 branches in fallback merge, got %d", len(mergeBranches))
	}

	var okCount, failCount int
	for _, b := range mergeBranches {
		if b.OK() {
			okCount++
		} else if b.Failed() {
			failCount++
			if b.Error != "intentional branch failure" {
				t.Fatalf("unexpected branch error: %q", b.Error)
			}
		}
	}

	if okCount != 2 || failCount != 2 {
		t.Fatalf("expected 2 OK and 2 Failed branches, got ok=%d fail=%d", okCount, failCount)
	}
}

type rejectSpeculativeJoinStore struct {
	graph.Store[int]
	reject atomic.Bool
}

func (s *rejectSpeculativeJoinStore) Commit(ctx context.Context, expected uint64, change graph.Change[int]) error {
	if len(change.DeleteGroups) != 0 && s.reject.Swap(false) {
		return graph.ErrConflict
	}
	return s.Store.Commit(ctx, expected, change)
}

func TestSpeculativeJoinPersistenceAndRecovery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "speculative.db")
	store, err := sqlite.Open(context.Background(), dbPath, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	g := graph.New[int]("dispatch")
	node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, parent int) (graph.Transition[int], error) {
		return graph.FanOut(parent,
			graph.Branch[int]{Target: "worker", State: 10},
			graph.Branch[int]{Target: "worker", State: 20},
			graph.Branch[int]{Target: "worker", State: 30},
			graph.Branch[int]{Target: "worker", State: 40},
		), nil
	})

	node(t, g, "worker", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.CompleteBranch(state + 1), nil
	})

	if err := g.AddJoin(graph.JoinSpec[int]{
		Name:    "joined",
		From:    "dispatch",
		Quorum:  2,
		OnError: graph.FailExecution,
		Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
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
	rejectStore := &rejectSpeculativeJoinStore{Store: store}
	rejectStore.reject.Store(true)

	// Start will hit ErrConflict when DeleteGroups is committed for the speculative join
	_, res, err := runner.Start(context.Background(), "speculative-sqlite-run", 100, rejectStore, graph.Options{MaxConcurrency: 4})
	if !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("expected ErrConflict, got: %+v, %v", res, err)
	}

	// Load checkpoint from SQLite: activation group still exists with branches
	cp, err := store.Load(context.Background(), "speculative-sqlite-run")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if len(cp.Groups) != 1 {
		t.Fatalf("expected 1 active group before join commit, got %d", len(cp.Groups))
	}

	// Replay recovery: will re-trigger speculative join, execute Merge, and commit successfully
	_, recovered, err := runner.Recover(context.Background(), "speculative-sqlite-run", nil, rejectStore, graph.Options{MaxConcurrency: 4})
	if err != nil || recovered.Status != graph.StatusCompleted || recovered.Final == nil {
		t.Fatalf("recovery failed: %+v, %v", recovered, err)
	}

	// Verify all child branches and groups are cleaned up in final checkpoint
	finalCP, err := store.Load(context.Background(), "speculative-sqlite-run")
	if err != nil {
		t.Fatalf("load final failed: %v", err)
	}
	if len(finalCP.Groups) != 0 || len(finalCP.Invocations) != 0 {
		t.Fatalf("expected 0 groups and 0 invocations after completion, got groups=%d invs=%d", len(finalCP.Groups), len(finalCP.Invocations))
	}
}
