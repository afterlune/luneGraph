package graph_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	graph "lune-graph"
	"lune-graph/checkpoint"
)

type countingLoadStore struct {
	graph.Store[int]
	loads int
}

func (s *countingLoadStore) Load(ctx context.Context, runID string) (graph.Checkpoint[int], error) {
	s.loads++
	return s.Store.Load(ctx, runID)
}

type alteredLoadStore struct {
	graph.Store[int]
	alter func(*graph.Checkpoint[int])
}

func (s *alteredLoadStore) Load(ctx context.Context, runID string) (graph.Checkpoint[int], error) {
	value, err := s.Store.Load(ctx, runID)
	if err == nil {
		s.alter(&value)
	}
	return value, err
}

func TestRecoverWaitingRunAndCompletedResult(t *testing.T) {
	ctx := context.Background()
	g := graph.New[int]("wait")
	calls := 0
	node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		calls++
		return graph.Wait(state, "number", "done"), nil
	})
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		calls++
		return graph.EndExecution(state), nil
	})
	edge(t, g, "wait", "done")
	if err := graph.RegisterContinuation(g, "number", func(data []byte) (int, error) {
		return strconv.Atoi(string(data))
	}, func(_ context.Context, _ graph.CallInfo, state, input int) (int, error) {
		return state + input, nil
	}); err != nil {
		t.Fatal(err)
	}
	runner := intRunner(t, g)
	store := &countingLoadStore{Store: newMemoryStore(t)}
	first, err := runner.Start(ctx, "waiting-recover", 2, graph.Options[int]{Store: store})
	if err != nil || first.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", first, err)
	}
	input := []graph.ResumeInput{{InvocationID: "i1", Payload: []byte("3")}}
	completed, err := runner.Recover(ctx, "waiting-recover", input, graph.Options[int]{Store: store})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Checkpoint.Revision != 4 || completed.Checkpoint.Final == nil || *completed.Checkpoint.Final != 5 || store.loads != 1 || calls != 2 {
		t.Fatalf("recovered = %+v, %v; loads=%d calls=%d", completed, err, store.loads, calls)
	}
	again, err := runner.Recover(ctx, "waiting-recover", nil, graph.Options[int]{Store: store})
	if err != nil || again.Status != graph.StatusCompleted || again.Checkpoint.Revision != completed.Checkpoint.Revision || again.Checkpoint.Final == nil || *again.Checkpoint.Final != 5 || calls != 2 || store.loads != 2 {
		t.Fatalf("completed reload = %+v, %v; loads=%d calls=%d", again, err, store.loads, calls)
	}
	rejected, err := runner.Recover(ctx, "waiting-recover", input, graph.Options[int]{Store: store})
	if !errors.Is(err, graph.ErrRunCompleted) || rejected.Status != graph.StatusFailed || rejected.Checkpoint.Revision != completed.Checkpoint.Revision || calls != 2 {
		t.Fatalf("input to completed run = %+v, %v; calls=%d", rejected, err, calls)
	}
}

func TestRecoverBudgetAndCompletedBranches(t *testing.T) {
	ctx := context.Background()
	g := graph.New[int]("loop")
	node(t, g, "loop", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		if state == 2 {
			return graph.EndExecution(state), nil
		}
		return graph.To(state+1, "loop"), nil
	})
	edge(t, g, "loop", "loop")
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	first, err := runner.Start(ctx, "budget-recover", 0, graph.Options[int]{Store: store, MaxSteps: 1})
	if err != nil || first.Status != graph.StatusBudget {
		t.Fatalf("budgeted = %+v, %v", first, err)
	}
	completed, err := runner.Recover(ctx, "budget-recover", nil, graph.Options[int]{Store: store})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Checkpoint.Final == nil || *completed.Checkpoint.Final != 2 {
		t.Fatalf("budget recovery = %+v, %v", completed, err)
	}

	for _, tc := range []struct {
		name   string
		node   graph.Node[int]
		status graph.Status
	}{
		{"branch", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.EndBranch(state), nil
		}, graph.StatusCompleted},
		{"failed", func(context.Context, graph.CallInfo, int) (graph.Transition[int], error) {
			return graph.Transition[int]{}, errors.New("node failed")
		}, graph.StatusCompletedWithFailures},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := graph.New[int]("only")
			node(t, g, "only", tc.node)
			runner := intRunner(t, g)
			store := newMemoryStore(t)
			first, err := runner.Start(ctx, tc.name, 7, graph.Options[int]{Store: store})
			if err != nil || first.Status != tc.status {
				t.Fatalf("start = %+v, %v", first, err)
			}
			again, err := runner.Recover(ctx, tc.name, nil, graph.Options[int]{Store: store})
			if err != nil || again.Status != tc.status || again.Checkpoint.Revision != first.Checkpoint.Revision || again.Checkpoint.Final != nil {
				t.Fatalf("terminal recovery = %+v, %v", again, err)
			}
		})
	}
}

func TestRecoverRejectsInvalidStoredRun(t *testing.T) {
	ctx := context.Background()
	g := graph.New[int]("only")
	node(t, g, "only", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	if _, err := runner.Start(ctx, "stored", 1, graph.Options[int]{Store: store}); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Recover(ctx, "stored", nil, graph.Options[int]{}); err == nil {
		t.Fatal("recovery without a store succeeded")
	}
	if _, err := runner.Recover(ctx, "missing", nil, graph.Options[int]{Store: store}); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("missing run = %v", err)
	}
	other, err := g.Compile(graph.Config[int]{MachineID: "other-machine", Clone: func(v int) (int, error) { return v, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Recover(ctx, "stored", nil, graph.Options[int]{Store: store}); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("wrong machine = %v", err)
	}
	for _, tc := range []struct {
		name  string
		alter func(*graph.Checkpoint[int])
	}{
		{"run ID", func(value *graph.Checkpoint[int]) { value.RunID = "wrong" }},
		{"ready terminal", func(value *graph.Checkpoint[int]) {
			value.Invocations = []graph.Invocation[int]{{ID: "i1", Node: "only", Status: graph.InvocationReady}}
		}},
		{"unfinished final", func(value *graph.Checkpoint[int]) { value.Completed = false }},
		{"zero steps", func(value *graph.Checkpoint[int]) { value.Steps = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := &alteredLoadStore{Store: store, alter: tc.alter}
			if _, err := runner.Recover(ctx, "stored", nil, graph.Options[int]{Store: corrupt}); !errors.Is(err, graph.ErrInvalidCheckpoint) {
				t.Fatalf("corrupt terminal = %v", err)
			}
		})
	}
}

func TestConcurrentRecoverReturnsConflict(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	g := graph.New[int]("seed")
	node(t, g, "seed", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "work"), nil
	})
	node(t, g, "work", func(ctx context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		started <- struct{}{}
		select {
		case <-release:
			return graph.EndExecution(state + 1), nil
		case <-ctx.Done():
			return graph.Transition[int]{}, ctx.Err()
		}
	})
	edge(t, g, "seed", "work")
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	if _, err := runner.Start(ctx, "contended", 0, graph.Options[int]{Store: store, MaxSteps: 1}); err != nil {
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
			result, err := runner.Recover(ctx, "contended", nil, graph.Options[int]{Store: store})
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
			t.Fatal("both recoveries did not enter the node")
		}
	}
	close(release)
	var succeeded, conflicted int
	for range 2 {
		got := <-results
		switch {
		case got.err == nil && got.result.Status == graph.StatusCompleted:
			succeeded++
		case errors.Is(got.err, graph.ErrConflict):
			conflicted++
		default:
			t.Fatalf("unexpected recovery result = %+v, %v", got.result, got.err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("recoveries: %d succeeded, %d conflicted", succeeded, conflicted)
	}
	stored, err := store.Load(ctx, "contended")
	if err != nil || stored.Revision != 3 || stored.Final == nil || *stored.Final != 1 {
		t.Fatalf("stored result = %+v, %v", stored, err)
	}
}

func TestRecoverAfterAmbiguousFinalCommit(t *testing.T) {
	ctx := context.Background()
	calls := 0
	g := graph.New[int]("only")
	node(t, g, "only", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		calls++
		return graph.EndExecution(state + 1), nil
	})
	runner := intRunner(t, g)
	boom := errors.New("commit acknowledgement lost")
	store := &ambiguousStore{Store: newMemoryStore(t), report: boom}
	first, err := runner.Start(ctx, "ambiguous-final", 0, graph.Options[int]{Store: store})
	if !errors.Is(err, boom) || first.Checkpoint.Revision != 1 {
		t.Fatalf("ambiguous commit = %+v, %v", first, err)
	}
	completed, err := runner.Recover(ctx, "ambiguous-final", nil, graph.Options[int]{Store: store})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Checkpoint.Revision != 2 || completed.Checkpoint.Final == nil || *completed.Checkpoint.Final != 1 || calls != 1 {
		t.Fatalf("recovered final = %+v, %v; calls=%d", completed, err, calls)
	}
}
