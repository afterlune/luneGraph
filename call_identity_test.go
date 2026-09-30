package graph_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type rejectNextCASStore struct {
	graph.Store[int]
	reject atomic.Bool
}

func (s *rejectNextCASStore) CompareAndSwap(ctx context.Context, expected uint64, next graph.Checkpoint[int]) error {
	if s.reject.CompareAndSwap(true, false) {
		return graph.ErrConflict
	}
	return s.Store.CompareAndSwap(ctx, expected, next)
}

func TestLoopAllocatesDistinctCallIDs(t *testing.T) {
	var calls []graph.CallInfo
	g := graph.New[int]("loop")
	node(t, g, "loop", func(_ context.Context, call graph.CallInfo, state int) (graph.Transition[int], error) {
		calls = append(calls, call)
		if state == 2 {
			return graph.EndExecution(state), nil
		}
		return graph.To(state+1, "loop"), nil
	})
	edge(t, g, "loop", "loop")
	out, err := intRunner(t, g).Start(context.Background(), "loop-run", 0, graph.Options[int]{})
	if err != nil || out.Status != graph.StatusCompleted || len(calls) != 3 {
		t.Fatalf("loop result = %+v, calls=%+v, err=%v", out, calls, err)
	}
	seen := make(map[string]bool)
	for _, call := range calls {
		if call.RunID != "loop-run" || call.InvocationID != "i1" || call.CallID == "" || seen[call.CallID] {
			t.Fatalf("invalid or reused call info: %+v", call)
		}
		seen[call.CallID] = true
	}
}

func TestNodeCallIDSurvivesRecoveryAfterRejectedCommit(t *testing.T) {
	store := &rejectNextCASStore{Store: newMemoryStore(t)}
	var calls []graph.CallInfo
	g := graph.New[int]("work")
	node(t, g, "work", func(_ context.Context, call graph.CallInfo, state int) (graph.Transition[int], error) {
		calls = append(calls, call)
		if len(calls) == 1 {
			store.reject.Store(true)
		}
		return graph.EndExecution(state + 1), nil
	})
	runner := intRunner(t, g)
	first, err := runner.Start(context.Background(), "node-replay", 0, graph.Options[int]{Store: store})
	if !errors.Is(err, graph.ErrConflict) || first.Checkpoint.Revision != 1 {
		t.Fatalf("first start = %+v, %v", first, err)
	}
	stored, err := store.Load(context.Background(), "node-replay")
	if err != nil || stored.Invocations[0].CallID != calls[0].CallID {
		t.Fatalf("stored ready call = %+v, %v", stored, err)
	}
	out, err := runner.Recover(context.Background(), "node-replay", nil, graph.Options[int]{Store: store})
	if err != nil || out.Status != graph.StatusCompleted || len(calls) != 2 {
		t.Fatalf("recovered run = %+v, calls=%+v, err=%v", out, calls, err)
	}
	if calls[0] != calls[1] {
		t.Fatalf("replayed node call info changed: first=%+v second=%+v", calls[0], calls[1])
	}
}

func TestContinuationCallIDSurvivesRecoveryAfterRejectedCommit(t *testing.T) {
	store := &rejectNextCASStore{Store: newMemoryStore(t)}
	var calls []graph.CallInfo
	g := graph.New[int]("wait")
	node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Wait(state, "add", "done"), nil
	})
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, g, "wait", "done")
	if err := graph.RegisterContinuation(g, "add", func(data []byte) (int, error) {
		return strconv.Atoi(string(data))
	}, func(_ context.Context, call graph.CallInfo, state, input int) (int, error) {
		calls = append(calls, call)
		if len(calls) == 1 {
			store.reject.Store(true)
		}
		return state + input, nil
	}); err != nil {
		t.Fatal(err)
	}
	runner := intRunner(t, g)
	first, err := runner.Start(context.Background(), "continuation-replay", 2, graph.Options[int]{Store: store})
	if err != nil || first.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", first, err)
	}
	inputs := []graph.ResumeInput{{InvocationID: "i1", Payload: []byte("3")}}
	if _, err := runner.Resume(context.Background(), first.Checkpoint, inputs, graph.Options[int]{Store: store}); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("first resume error = %v", err)
	}
	out, err := runner.Recover(context.Background(), "continuation-replay", inputs, graph.Options[int]{Store: store})
	if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || *out.Checkpoint.Final != 5 || len(calls) != 2 {
		t.Fatalf("recovered continuation = %+v, calls=%+v, err=%v", out, calls, err)
	}
	if calls[0] != calls[1] || calls[0].RunID != "continuation-replay" || calls[0].InvocationID != "i1" {
		t.Fatalf("replayed continuation call info changed: first=%+v second=%+v", calls[0], calls[1])
	}
}

func TestJoinCallIDSurvivesRecoveryAfterRejectedCommit(t *testing.T) {
	store := &rejectNextCASStore{Store: newMemoryStore(t)}
	var mergeCalls []graph.CallInfo
	var rightCalls []graph.CallInfo
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "left", "right"), nil
	})
	node(t, g, "left", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state+1, "join"), nil
	})
	node(t, g, "right", func(_ context.Context, call graph.CallInfo, state int) (graph.Transition[int], error) {
		rightCalls = append(rightCalls, call)
		return graph.To(state+2, "join"), nil
	})
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, call graph.CallInfo, states []int) (int, error) {
		mergeCalls = append(mergeCalls, call)
		if len(mergeCalls) == 1 {
			store.reject.Store(true)
		}
		return states[0] + states[1], nil
	}}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	for _, pair := range [][2]string{{"fork", "left"}, {"fork", "right"}, {"left", "join"}, {"right", "join"}, {"join", "done"}} {
		edge(t, g, pair[0], pair[1])
	}
	runner := intRunner(t, g)
	first, err := runner.Start(context.Background(), "join-replay", 0, graph.Options[int]{Store: store, MaxConcurrency: 1})
	if !errors.Is(err, graph.ErrConflict) || len(first.Checkpoint.Groups) != 1 || len(mergeCalls) != 1 {
		t.Fatalf("first start = %+v, merges=%+v, %v", first, mergeCalls, err)
	}
	if first.Checkpoint.Groups[0].CallID != mergeCalls[0].CallID {
		t.Fatalf("join call was not persisted: checkpoint=%+v merge=%+v", first.Checkpoint.Groups[0], mergeCalls[0])
	}
	out, err := runner.Recover(context.Background(), "join-replay", nil, graph.Options[int]{Store: store, MaxConcurrency: 1})
	if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || *out.Checkpoint.Final != 3 || len(mergeCalls) != 2 || len(rightCalls) != 2 {
		t.Fatalf("recovered join = %+v, merges=%+v, right calls=%+v, err=%v", out, mergeCalls, rightCalls, err)
	}
	if mergeCalls[0] != mergeCalls[1] || rightCalls[0] != rightCalls[1] {
		t.Fatalf("replayed callback identity changed: merges=%+v right=%+v", mergeCalls, rightCalls)
	}
}
