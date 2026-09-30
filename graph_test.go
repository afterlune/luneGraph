package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint/memory"
)

func node[S any](t *testing.T, g *graph.Graph[S], name string, run graph.Node[S]) {
	t.Helper()
	if err := g.AddNode(graph.NodeSpec[S]{Name: name, Run: run}); err != nil {
		t.Fatal(err)
	}
}

func edge[S any](t *testing.T, g *graph.Graph[S], from, to string) {
	t.Helper()
	if err := g.AddEdge(from, to); err != nil {
		t.Fatal(err)
	}
}

func intRunner(t *testing.T, g *graph.Graph[int]) *graph.Runner[int] {
	t.Helper()
	r, err := g.Compile(graph.Config[int]{MachineID: "numbers-v1", Clone: func(v int) (int, error) { return v, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCycleBudgetAndResume(t *testing.T) {
	g := graph.New[int]("loop")
	node(t, g, "loop", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		if v == 3 {
			return graph.EndExecution(v), nil
		}
		return graph.To(v+1, "loop"), nil
	})
	edge(t, g, "loop", "loop")
	r := intRunner(t, g)
	first, err := r.Start(context.Background(), "cycle", 0, graph.Options[int]{MaxSteps: 2})
	if err != nil || first.Status != graph.StatusBudget || first.Checkpoint.Steps != 2 || first.Checkpoint.Invocations[0].State != 2 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	last, err := r.Resume(context.Background(), first.Checkpoint, nil, graph.Options[int]{})
	if err != nil || last.Status != graph.StatusCompleted || last.Checkpoint.Final == nil || *last.Checkpoint.Final != 3 {
		t.Fatalf("last = %+v, %v", last, err)
	}
	if _, err := r.Resume(context.Background(), last.Checkpoint, nil, graph.Options[int]{}); err == nil {
		t.Fatal("completed checkpoint resumed")
	}
}

func TestParallelJoinAndStableMergeOrder(t *testing.T) {
	g := graph.New[int]("fork")
	started := make(chan string, 2)
	release := make(chan struct{})
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "a", "b"), nil
	})
	for _, item := range []struct {
		name  string
		delta int
	}{{"a", 1}, {"b", 2}} {
		item := item
		node(t, g, item.name, func(ctx context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
			started <- item.name
			select {
			case <-release:
			case <-ctx.Done():
				return graph.Transition[int]{}, ctx.Err()
			}
			return graph.To(v+item.delta, "join"), nil
		})
		edge(t, g, "fork", item.name)
	}
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, values []int) (int, error) {
		if len(values) != 2 || values[0] != 1 || values[1] != 2 {
			return 0, fmt.Errorf("wrong merge order: %v", values)
		}
		return values[0]*10 + values[1], nil
	}}); err != nil {
		t.Fatal(err)
	}
	edge(t, g, "a", "join")
	edge(t, g, "b", "join")
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndExecution(v), nil
	})
	edge(t, g, "join", "done")
	r := intRunner(t, g)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	complete := make(chan struct {
		result graph.Result[int]
		err    error
	}, 1)
	go func() {
		result, err := r.Start(ctx, "parallel", 0, graph.Options[int]{MaxConcurrency: 2})
		complete <- struct {
			result graph.Result[int]
			err    error
		}{result, err}
	}()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			close(release)
			t.Fatal("branches did not run concurrently")
		}
	}
	close(release)
	out := <-complete
	if out.err != nil || out.result.Status != graph.StatusCompleted || out.result.Checkpoint.Final == nil || *out.result.Checkpoint.Final != 12 {
		t.Fatalf("parallel = %+v, %v", out.result, out.err)
	}
}

func TestNestedGroups(t *testing.T) {
	g := graph.New[int]("root")
	node(t, g, "root", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "a", "b"), nil
	})
	node(t, g, "a", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "c", "d"), nil
	})
	node(t, g, "b", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v+10, "outer"), nil
	})
	node(t, g, "c", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v+1, "inner"), nil
	})
	node(t, g, "d", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v+2, "inner"), nil
	})
	node(t, g, "after_inner", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "outer"), nil
	})
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndExecution(v), nil
	})
	for _, spec := range []graph.JoinSpec[int]{
		{Name: "inner", From: "a", Merge: func(_ context.Context, _ graph.CallInfo, values []int) (int, error) {
			return values[0] + values[1], nil
		}},
		{Name: "outer", From: "root", Merge: func(_ context.Context, _ graph.CallInfo, values []int) (int, error) {
			return values[0] + values[1], nil
		}},
	} {
		if err := g.AddJoin(spec); err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range [][2]string{{"root", "a"}, {"root", "b"}, {"a", "c"}, {"a", "d"}, {"b", "outer"}, {"c", "inner"}, {"d", "inner"}, {"inner", "after_inner"}, {"after_inner", "outer"}, {"outer", "done"}} {
		edge(t, g, pair[0], pair[1])
	}
	out, err := intRunner(t, g).Start(context.Background(), "nested", 0, graph.Options[int]{MaxConcurrency: 4})
	if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || *out.Checkpoint.Final != 13 {
		t.Fatalf("nested = %+v, %v", out, err)
	}
}

func TestPauseResumeAndPayloadValidation(t *testing.T) {
	g := graph.New[int]("wait")
	node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.Wait(v, "add", "done"), nil
	})
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndExecution(v), nil
	})
	edge(t, g, "wait", "done")
	if err := graph.RegisterContinuation(g, "add", func(payload []byte) (int, error) { return strconv.Atoi(string(payload)) }, func(_ context.Context, _ graph.CallInfo, state, value int) (int, error) { return state + value, nil }); err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	first, err := r.Start(context.Background(), "pause", 4, graph.Options[int]{})
	if err != nil || first.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", first, err)
	}
	id := first.Checkpoint.Invocations[0].ID
	bad, err := r.Resume(context.Background(), first.Checkpoint, []graph.ResumeInput{{InvocationID: id, Payload: []byte("bad")}}, graph.Options[int]{})
	if err == nil || bad.Checkpoint.Revision != first.Checkpoint.Revision || bad.Checkpoint.Invocations[0].Status != graph.InvocationWaiting {
		t.Fatalf("bad resume = %+v, %v", bad, err)
	}
	last, err := r.Resume(context.Background(), first.Checkpoint, []graph.ResumeInput{{InvocationID: id, Payload: []byte("5")}}, graph.Options[int]{})
	if err != nil || last.Status != graph.StatusCompleted || last.Checkpoint.Final == nil || *last.Checkpoint.Final != 9 {
		t.Fatalf("resume = %+v, %v", last, err)
	}
}

func TestFailureScopesAndCloneRollback(t *testing.T) {
	boom := errors.New("boom")
	build := func(scope graph.FailureScope) *graph.Runner[int] {
		g := graph.New[int]("fork")
		node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
			return graph.To(v, "good", "bad"), nil
		})
		node(t, g, "good", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
			return graph.To(v+5, "join"), nil
		})
		if err := g.AddNode(graph.NodeSpec[int]{Name: "bad", OnError: scope, Run: func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
			return graph.Transition[int]{}, boom
		}}); err != nil {
			t.Fatal(err)
		}
		if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, values []int) (int, error) { return values[0], nil }}); err != nil {
			t.Fatal(err)
		}
		node(t, g, "done", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
			return graph.EndExecution(v), nil
		})
		for _, pair := range [][2]string{{"fork", "good"}, {"fork", "bad"}, {"good", "join"}, {"join", "done"}} {
			edge(t, g, pair[0], pair[1])
		}
		return intRunner(t, g)
	}
	local, err := build(graph.FailInvocation).Start(context.Background(), "local", 0, graph.Options[int]{MaxConcurrency: 1})
	if err != nil || local.Status != graph.StatusCompletedWithFailures || local.Checkpoint.Final == nil || *local.Checkpoint.Final != 5 || len(local.Checkpoint.Failures) != 1 {
		t.Fatalf("local = %+v, %v", local, err)
	}
	group, err := build(graph.FailGroup).Start(context.Background(), "group", 0, graph.Options[int]{MaxConcurrency: 1})
	if err != nil || group.Status != graph.StatusCompletedWithFailures || group.Checkpoint.Final != nil || len(group.Checkpoint.Failures) != 1 {
		t.Fatalf("group = %+v, %v", group, err)
	}
	execution, err := build(graph.FailExecution).Start(context.Background(), "execution", 0, graph.Options[int]{MaxConcurrency: 1})
	if !errors.Is(err, boom) || execution.Status != graph.StatusFailed {
		t.Fatalf("execution = %+v, %v", execution, err)
	}

	type state struct{ Values map[string]int }
	mutable := graph.New[state]("mutate")
	node(t, mutable, "mutate", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
		s.Values["n"] = 99
		return graph.Transition[state]{}, boom
	})
	r, err := mutable.Compile(graph.Config[state]{MachineID: "mutable-v1", Clone: func(s state) (state, error) {
		copy := state{Values: make(map[string]int)}
		for k, v := range s.Values {
			copy.Values[k] = v
		}
		return copy, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	initial := state{Values: map[string]int{"n": 1}}
	rolled, err := r.Start(context.Background(), "mutable", initial, graph.Options[state]{})
	if err != nil || rolled.Checkpoint.Invocations[0].State.Values["n"] != 1 || initial.Values["n"] != 1 {
		t.Fatalf("rollback = %+v, %v", rolled, err)
	}
}

type failingStore struct {
	mu       sync.Mutex
	inner    *memory.Store[int]
	failNext bool
}

func copyIntCheckpoint(s graph.Checkpoint[int]) graph.Checkpoint[int] {
	out, err := s.Clone(func(v int) (int, error) { return v, nil })
	if err != nil {
		panic(err)
	}
	return out
}

func newMemoryStore(t *testing.T) *memory.Store[int] {
	t.Helper()
	store, err := memory.New(func(v int) (int, error) { return v, nil })
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func (m *failingStore) Create(ctx context.Context, s graph.Checkpoint[int]) error {
	return m.inner.Create(ctx, s)
}

func (m *failingStore) Load(ctx context.Context, id string) (graph.Checkpoint[int], error) {
	return m.inner.Load(ctx, id)
}

func (m *failingStore) CompareAndSwap(ctx context.Context, expected uint64, next graph.Checkpoint[int]) error {
	m.mu.Lock()
	if m.failNext {
		m.failNext = false
		m.mu.Unlock()
		return errors.New("save failed")
	}
	m.mu.Unlock()
	return m.inner.CompareAndSwap(ctx, expected, next)
}

func TestStoreFailureReturnsLastCommittedCheckpoint(t *testing.T) {
	g := graph.New[int]("first")
	called := 0
	node(t, g, "first", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		called++
		return graph.To(v+1, "second"), nil
	})
	node(t, g, "second", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndExecution(v + 1), nil
	})
	edge(t, g, "first", "second")
	r := intRunner(t, g)
	store := &failingStore{inner: newMemoryStore(t), failNext: true}
	failed, err := r.Start(context.Background(), "durable", 0, graph.Options[int]{Store: store})
	if err == nil || failed.Status != graph.StatusFailed || failed.Checkpoint.Revision != 1 || failed.Checkpoint.Steps != 0 || failed.Checkpoint.Invocations[0].Node != "first" {
		t.Fatalf("failed save = %+v, %v", failed, err)
	}
	last, err := r.Resume(context.Background(), failed.Checkpoint, nil, graph.Options[int]{Store: store})
	if err != nil || last.Status != graph.StatusCompleted || last.Checkpoint.Final == nil || *last.Checkpoint.Final != 2 || called != 2 {
		t.Fatalf("resume = %+v, %v, called=%d", last, err, called)
	}
	if _, err := r.Resume(context.Background(), failed.Checkpoint, nil, graph.Options[int]{Store: store}); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("stale checkpoint error = %v", err)
	}
}
