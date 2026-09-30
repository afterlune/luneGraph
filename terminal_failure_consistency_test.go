package graph_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

func TestTerminalFailureCommitOutcomes(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("node failed")
	calls := 0
	g := graph.New[int]("work")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "work", OnError: graph.FailExecution, Run: func(context.Context, graph.CallInfo, int) (graph.Transition[int], error) {
		calls++
		return graph.Transition[int]{}, boom
	}}); err != nil {
		t.Fatal(err)
	}
	runner := intRunner(t, g)
	beforeCommit := &failingStore{inner: newMemoryStore(t), failNext: true}
	first, err := runner.Start(ctx, "before-commit", 0, graph.Options[int]{Store: beforeCommit})
	if err == nil || errors.Is(err, boom) || first.Checkpoint.Revision != 1 || first.Checkpoint.Completed {
		t.Fatalf("failed commit = %+v, %v", first, err)
	}
	replayed, err := runner.Recover(ctx, "before-commit", nil, graph.Options[int]{Store: beforeCommit})
	if !errors.Is(err, boom) || !replayed.Checkpoint.Completed || replayed.Checkpoint.Revision != 2 || calls != 2 {
		t.Fatalf("replayed failure = %+v, %v; calls=%d", replayed, err, calls)
	}
	confirmed, err := runner.Recover(ctx, "before-commit", nil, graph.Options[int]{Store: beforeCommit})
	if !errors.Is(err, graph.ErrRunFailed) || confirmed.Checkpoint.Revision != 2 || calls != 2 {
		t.Fatalf("confirmed failure = %+v, %v; calls=%d", confirmed, err, calls)
	}

	lost := errors.New("commit acknowledgement lost")
	afterCommit := &ambiguousStore{Store: newMemoryStore(t), report: lost}
	uncertain, err := runner.Start(ctx, "after-commit", 0, graph.Options[int]{Store: afterCommit})
	if !errors.Is(err, lost) || uncertain.Checkpoint.Revision != 1 || uncertain.Checkpoint.Completed {
		t.Fatalf("uncertain commit = %+v, %v", uncertain, err)
	}
	recovered, err := runner.Recover(ctx, "after-commit", nil, graph.Options[int]{Store: afterCommit})
	if !errors.Is(err, graph.ErrRunFailed) || recovered.Checkpoint.Revision != 2 || calls != 3 {
		t.Fatalf("recovered committed failure = %+v, %v; calls=%d", recovered, err, calls)
	}
}

func TestCancellationDoesNotCommitTerminalFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	var calls atomic.Int32
	g := graph.New[int]("work")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "work", OnError: graph.FailExecution, Run: func(ctx context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			return graph.Transition[int]{}, ctx.Err()
		}
		return graph.EndExecution(state + 1), nil
	}}); err != nil {
		t.Fatal(err)
	}
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	type outcome struct {
		result graph.Result[int]
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := runner.Start(ctx, "cancelled-failure", 0, graph.Options[int]{Store: store})
		done <- outcome{result, err}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("node did not start")
	}
	cancel()
	select {
	case first := <-done:
		if !errors.Is(first.err, context.Canceled) || first.result.Status != graph.StatusCancelled || first.result.Checkpoint.Revision != 1 || first.result.Checkpoint.Completed {
			t.Fatalf("cancelled run = %+v, %v", first.result, first.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled run did not return")
	}
	recovered, err := runner.Recover(context.Background(), "cancelled-failure", nil, graph.Options[int]{Store: store})
	if err != nil || recovered.Status != graph.StatusCompleted || recovered.Checkpoint.Final == nil || *recovered.Checkpoint.Final != 1 || calls.Load() != 2 {
		t.Fatalf("recovered cancelled run = %+v, %v; calls=%d", recovered, err, calls.Load())
	}
}

func TestInvalidTransitionDoesNotCommitTerminalFailure(t *testing.T) {
	g := graph.New[int]("work")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "work", OnError: graph.FailExecution, Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "missing"), nil
	}}); err != nil {
		t.Fatal(err)
	}
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	first, err := runner.Start(context.Background(), "invalid-transition", 0, graph.Options[int]{Store: store})
	var transitionErr *graph.TransitionError
	if !errors.As(err, &transitionErr) || first.Checkpoint.Revision != 1 || first.Checkpoint.Completed {
		t.Fatalf("invalid transition = %+v, %v", first, err)
	}
	stored, err := store.Load(context.Background(), "invalid-transition")
	if err != nil || stored.Revision != 1 || (stored.Failure != nil || stored.HadLocalFailures) {
		t.Fatalf("stored checkpoint = %+v, %v", stored, err)
	}
}

func TestTerminalFailureCancelsSibling(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("branch failed")
	started := make(chan struct{})
	cancelled := make(chan struct{})
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "fail", "slow"), nil
	})
	if err := g.AddNode(graph.NodeSpec[int]{Name: "fail", OnError: graph.FailExecution, Run: func(context.Context, graph.CallInfo, int) (graph.Transition[int], error) {
		<-started
		return graph.Transition[int]{}, boom
	}}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "slow", func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return graph.Transition[int]{}, ctx.Err()
	})
	edge(t, g, "fork", "fail")
	edge(t, g, "fork", "slow")
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	failed, err := runner.Start(runCtx, "parallel-failure", 0, graph.Options[int]{Store: store, MaxConcurrency: 2})
	if !errors.Is(err, boom) || failed.Checkpoint.Revision != 3 || failed.Checkpoint.Steps != 2 || !failed.Checkpoint.Completed {
		t.Fatalf("parallel failure = %+v, %v", failed, err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("sibling callback was not cancelled before return")
	}
	stored, err := store.Load(ctx, "parallel-failure")
	if err != nil || stored.Revision != failed.Checkpoint.Revision || stored.Failure == nil {
		t.Fatalf("stored failure = %+v, %v", stored, err)
	}
}

func TestConcurrentTerminalFailureCommitConflict(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("node failed")
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	g := graph.New[int]("seed")
	node(t, g, "seed", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "fail"), nil
	})
	if err := g.AddNode(graph.NodeSpec[int]{Name: "fail", OnError: graph.FailExecution, Run: func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		started <- struct{}{}
		select {
		case <-release:
			return graph.Transition[int]{}, boom
		case <-ctx.Done():
			return graph.Transition[int]{}, ctx.Err()
		}
	}}); err != nil {
		t.Fatal(err)
	}
	edge(t, g, "seed", "fail")
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	if _, err := runner.Start(ctx, "conflict-failure", 0, graph.Options[int]{Store: store, MaxSteps: 1}); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result graph.Result[int]
		err    error
	}
	results := make(chan outcome, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			result, err := runner.Recover(ctx, "conflict-failure", nil, graph.Options[int]{Store: store})
			results <- outcome{result, err}
		}()
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		workers.Wait()
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("both recoveries did not start the node")
		}
	}
	close(release)
	var committed, conflicted int
	for range 2 {
		got := <-results
		switch {
		case errors.Is(got.err, boom) && got.result.Checkpoint.Completed:
			committed++
		case errors.Is(got.err, graph.ErrConflict) && !got.result.Checkpoint.Completed:
			conflicted++
		default:
			t.Fatalf("unexpected recovery = %+v, %v", got.result, got.err)
		}
	}
	if committed != 1 || conflicted != 1 {
		t.Fatalf("terminal commits: %d committed, %d conflicted", committed, conflicted)
	}
	if _, err := runner.Recover(ctx, "conflict-failure", nil, graph.Options[int]{Store: store}); !errors.Is(err, graph.ErrRunFailed) {
		t.Fatalf("stored terminal failure = %v", err)
	}
}
