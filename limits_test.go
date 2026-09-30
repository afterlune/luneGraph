package graph_test

import (
	"context"
	"errors"
	"math"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestSchedulerCountersDoNotWrap(t *testing.T) {
	called := 0
	g := graph.New[int]("loop")
	node(t, g, "loop", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		called++
		return graph.To(state+1, "loop"), nil
	})
	edge(t, g, "loop", "loop")
	r := intRunner(t, g)
	first, err := r.Start(context.Background(), "counters", 0, graph.Options[int]{MaxSteps: 1})
	if err != nil || called != 1 {
		t.Fatalf("start = %+v, %v", first, err)
	}
	for _, mutate := range []func(*graph.Checkpoint[int]){
		func(s *graph.Checkpoint[int]) { s.Revision = math.MaxUint64 },
		func(s *graph.Checkpoint[int]) { s.Steps = math.MaxUint64 },
	} {
		limited := first.Checkpoint
		mutate(&limited)
		out, err := r.Resume(context.Background(), limited, nil, graph.Options[int]{MaxConcurrency: 2})
		if !errors.Is(err, graph.ErrExecutionLimit) || out.Status != graph.StatusFailed || out.Checkpoint.Revision != limited.Revision || out.Checkpoint.Steps != limited.Steps || called != 1 {
			t.Fatalf("counter limit = %+v, %v", out, err)
		}
	}
	badCursor := first.Checkpoint
	badCursor.ScheduleCursor = badCursor.NextID
	if _, err := r.Resume(context.Background(), badCursor, nil, graph.Options[int]{}); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("invalid cursor error = %v", err)
	}
}

func TestFanoutIDReservationDoesNotWrap(t *testing.T) {
	g := graph.New[int]("wait")
	node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Wait(state, "next", "a", "b"), nil
	})
	for _, name := range []string{"a", "b"} {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.EndBranch(state), nil
		})
		edge(t, g, "wait", name)
	}
	if err := graph.RegisterContinuation(g, "next", func([]byte) (int, error) { return 0, nil }, func(_ context.Context, _ graph.CallInfo, state, _ int) (int, error) { return state, nil }); err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	first, err := r.Start(context.Background(), "id-limit", 0, graph.Options[int]{})
	if err != nil || first.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", first, err)
	}
	limited := first.Checkpoint
	limited.NextID = math.MaxUint64 - 1
	out, err := r.Resume(context.Background(), limited, []graph.ResumeInput{{InvocationID: "i1"}}, graph.Options[int]{})
	if !errors.Is(err, graph.ErrExecutionLimit) || out.Status != graph.StatusFailed || out.Checkpoint.Revision != limited.Revision || out.Checkpoint.NextID != limited.NextID {
		t.Fatalf("ID limit = %+v, %v", out, err)
	}
}

func TestWaitCallIDReservationDoesNotWrap(t *testing.T) {
	g := graph.New[int]("seed")
	node(t, g, "seed", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "wait"), nil
	})
	node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Wait(state, "next", "done"), nil
	})
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, g, "seed", "wait")
	edge(t, g, "wait", "done")
	if err := graph.RegisterContinuation(g, "next", func([]byte) (int, error) { return 0, nil }, func(_ context.Context, _ graph.CallInfo, state, _ int) (int, error) { return state, nil }); err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	first, err := r.Start(context.Background(), "wait-id-limit", 0, graph.Options[int]{MaxSteps: 1})
	if err != nil || first.Status != graph.StatusBudget {
		t.Fatalf("start = %+v, %v", first, err)
	}
	limited := first.Checkpoint
	limited.NextID = math.MaxUint64
	out, err := r.Resume(context.Background(), limited, nil, graph.Options[int]{})
	if !errors.Is(err, graph.ErrExecutionLimit) || out.Status != graph.StatusFailed || out.Checkpoint.Revision != limited.Revision || out.Checkpoint.NextID != limited.NextID {
		t.Fatalf("wait call ID limit = %+v, %v", out, err)
	}
}

func TestNodeCallIDReservationDoesNotWrap(t *testing.T) {
	g := graph.New[int]("seed")
	node(t, g, "seed", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "next"), nil
	})
	node(t, g, "next", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "done"), nil
	})
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, g, "seed", "next")
	edge(t, g, "next", "done")
	r := intRunner(t, g)
	first, err := r.Start(context.Background(), "node-id-limit", 0, graph.Options[int]{MaxSteps: 1})
	if err != nil || first.Status != graph.StatusBudget {
		t.Fatalf("start = %+v, %v", first, err)
	}
	limited := first.Checkpoint
	limited.NextID = math.MaxUint64
	out, err := r.Resume(context.Background(), limited, nil, graph.Options[int]{})
	if !errors.Is(err, graph.ErrExecutionLimit) || out.Status != graph.StatusFailed || out.Checkpoint.Revision != limited.Revision || out.Checkpoint.NextID != limited.NextID {
		t.Fatalf("node call ID limit = %+v, %v", out, err)
	}
}

func TestResumeInputsRejectExhaustedRevisionBeforeDecode(t *testing.T) {
	g := graph.New[int]("wait")
	node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Wait(state, "next", "done"), nil
	})
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, g, "wait", "done")
	decoded := 0
	if err := graph.RegisterContinuation(g, "next", func([]byte) (int, error) {
		decoded++
		return 0, nil
	}, func(_ context.Context, _ graph.CallInfo, state, _ int) (int, error) { return state, nil }); err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	first, err := r.Start(context.Background(), "revision-limit", 0, graph.Options[int]{})
	if err != nil {
		t.Fatal(err)
	}
	limited := first.Checkpoint
	limited.Revision = math.MaxUint64
	out, err := r.Resume(context.Background(), limited, []graph.ResumeInput{{InvocationID: "i1"}}, graph.Options[int]{})
	if !errors.Is(err, graph.ErrExecutionLimit) || decoded != 0 || out.Checkpoint.Revision != limited.Revision {
		t.Fatalf("revision limit = %+v, %v", out, err)
	}
}

func TestConcurrentLaunchesReserveCommitCapacity(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, "a", "b"), nil
	})
	called := 0
	for _, name := range []string{"a", "b"} {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			called++
			return graph.EndBranch(state), nil
		})
		edge(t, g, "fork", name)
	}
	r := intRunner(t, g)
	first, err := r.Start(context.Background(), "capacity", 0, graph.Options[int]{MaxSteps: 1})
	if err != nil {
		t.Fatal(err)
	}
	limited := first.Checkpoint
	limited.Revision = math.MaxUint64 - 1
	limited.Steps = math.MaxUint64 - 1
	out, err := r.Resume(context.Background(), limited, nil, graph.Options[int]{MaxConcurrency: 2})
	if !errors.Is(err, graph.ErrExecutionLimit) || called != 1 || out.Checkpoint.Revision != math.MaxUint64 || out.Checkpoint.Steps != math.MaxUint64 {
		t.Fatalf("reserved capacity = %+v, %v, called=%d", out, err, called)
	}
}
