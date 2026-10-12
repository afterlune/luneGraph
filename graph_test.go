package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	firstExecution, first, err := r.Start(context.Background(), "cycle", 0, nil, graph.Options{MaxSteps: 2})
	if err != nil || first.Status != graph.StatusBudget || first.Header.Steps != 2 || mustCheckpoint(firstExecution).Invocations[0].State != 2 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	lastExecution, last, err := r.Resume(context.Background(), mustCheckpoint(firstExecution), nil, nil, graph.Options{})
	if err != nil || last.Status != graph.StatusCompleted || last.Final == nil || *last.Final != 3 {
		t.Fatalf("last = %+v, %v", last, err)
	}
	if _, _, err := r.Resume(context.Background(), mustCheckpoint(lastExecution), nil, nil, graph.Options{}); err == nil {
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
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, _ int, branches []graph.BranchResult[int]) (int, error) {
		values := graph.BranchValues(branches)
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
		_, result, err := r.Start(ctx, "parallel", 0, nil, graph.Options{MaxConcurrency: 2})
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
	if out.err != nil || out.result.Status != graph.StatusCompleted || out.result.Final == nil || *out.result.Final != 12 {
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
		{Name: "inner", From: "a", Merge: func(_ context.Context, _ graph.CallInfo, _ int, branches []graph.BranchResult[int]) (int, error) {
			values := graph.BranchValues(branches)
			return values[0] + values[1], nil
		}},
		{Name: "outer", From: "root", Merge: func(_ context.Context, _ graph.CallInfo, _ int, branches []graph.BranchResult[int]) (int, error) {
			values := graph.BranchValues(branches)
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
	_, out, err := intRunner(t, g).Start(context.Background(), "nested", 0, nil, graph.Options{MaxConcurrency: 4})
	if err != nil || out.Status != graph.StatusCompleted || out.Final == nil || *out.Final != 13 {
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
	firstExecution, first, err := r.Start(context.Background(), "pause", 4, nil, graph.Options{})
	if err != nil || first.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", first, err)
	}
	id := mustCheckpoint(firstExecution).Invocations[0].ID
	badExecution, bad, err := r.Resume(context.Background(), mustCheckpoint(firstExecution), []graph.ResumeInput{{InvocationID: id, CallID: firstExecution.Pending()[0].CallID, Payload: []byte("bad")}}, nil, graph.Options{})
	if err == nil || bad.Header.Revision != first.Header.Revision || mustCheckpoint(badExecution).Invocations[0].Status != graph.InvocationWaiting {
		t.Fatalf("bad resume = %+v, %v", bad, err)
	}
	_, last, err := r.Resume(context.Background(), mustCheckpoint(firstExecution), []graph.ResumeInput{{InvocationID: id, CallID: firstExecution.Pending()[0].CallID, Payload: []byte("5")}}, nil, graph.Options{})
	if err != nil || last.Status != graph.StatusCompleted || last.Final == nil || *last.Final != 9 {
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
		if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, _ int, branches []graph.BranchResult[int]) (int, error) {
			values := graph.BranchValues(branches)
			return values[0], nil
		}}); err != nil {
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
	_, local, err := build(graph.FailInvocation).Start(context.Background(), "local", 0, nil, graph.Options{MaxConcurrency: 1})
	if err != nil || local.Status != graph.StatusCompletedWithFailures || local.Final == nil || *local.Final != 5 || (!local.Header.HadLocalFailures || local.Header.Failure != nil) {
		t.Fatalf("local = %+v, %v", local, err)
	}
	_, group, err := build(graph.FailGroup).Start(context.Background(), "group", 0, nil, graph.Options{MaxConcurrency: 1})
	if err != nil || group.Status != graph.StatusCompletedWithFailures || group.Final != nil || (!group.Header.HadLocalFailures || group.Header.Failure != nil) {
		t.Fatalf("group = %+v, %v", group, err)
	}
	_, execution, err := build(graph.FailExecution).Start(context.Background(), "execution", 0, nil, graph.Options{MaxConcurrency: 1})
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
	rolledExecution, rolled, err := r.Start(context.Background(), "mutable", initial, nil, graph.Options{})
	if err != nil || !rolled.Header.Completed || !rolled.Header.HadLocalFailures || rolled.Header.Failure != nil || len(mustCheckpoint(rolledExecution).Invocations) != 0 || initial.Values["n"] != 1 {
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

func (m *failingStore) LoadHeader(ctx context.Context, id string) (graph.Header, error) {
	return m.inner.LoadHeader(ctx, id)
}

func (m *failingStore) Commit(ctx context.Context, expected uint64, change graph.Change[int]) error {
	m.mu.Lock()
	if m.failNext {
		m.failNext = false
		m.mu.Unlock()
		return errors.New("save failed")
	}
	m.mu.Unlock()
	return m.inner.Commit(ctx, expected, change)
}

func (m *failingStore) Delete(ctx context.Context, id string) error {
	return m.inner.Delete(ctx, id)
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
	failedExecution, failed, err := r.Start(context.Background(), "durable", 0, store, graph.Options{})
	if err == nil || failed.Status != graph.StatusFailed || failed.Header.Revision != 1 || failed.Header.Steps != 0 || mustCheckpoint(failedExecution).Invocations[0].Node != "first" {
		t.Fatalf("failed save = %+v, %v", failed, err)
	}
	_, last, err := r.Resume(context.Background(), mustCheckpoint(failedExecution), nil, store, graph.Options{})
	if err != nil || last.Status != graph.StatusCompleted || last.Final == nil || *last.Final != 2 || called != 2 {
		t.Fatalf("resume = %+v, %v, called=%d", last, err, called)
	}
	if _, _, err := r.Resume(context.Background(), mustCheckpoint(failedExecution), nil, store, graph.Options{}); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("stale checkpoint error = %v", err)
	}
}

func TestValueClone(t *testing.T) {
	cloned, err := graph.ValueClone(42)
	if err != nil || cloned != 42 {
		t.Fatalf("ValueClone failed: %v, %v", cloned, err)
	}
}

func TestJSONClone(t *testing.T) {
	type ComplexState struct {
		Name   string         `json:"name"`
		Scores map[string]int `json:"scores"`
		Items  []string       `json:"items"`
	}
	orig := ComplexState{
		Name:   "test",
		Scores: map[string]int{"a": 1},
		Items:  []string{"x", "y"},
	}
	cloned, err := graph.JSONClone(orig)
	if err != nil {
		t.Fatalf("JSONClone error: %v", err)
	}
	cloned.Scores["a"] = 99
	cloned.Items[0] = "mutated"
	if orig.Scores["a"] != 1 || orig.Items[0] != "x" {
		t.Fatalf("JSONClone did not deep copy: orig=%+v, cloned=%+v", orig, cloned)
	}
}

func TestRegisterJSONContinuation(t *testing.T) {
	type ResumePayload struct {
		Delta int `json:"delta"`
	}
	g := graph.New[int]("start")
	node(t, g, "start", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.Wait(v, "increment", "end"), nil
	})
	node(t, g, "end", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndExecution(v), nil
	})
	edge(t, g, "start", "end")
	if err := graph.RegisterJSONContinuation(g, "increment", func(_ context.Context, _ graph.CallInfo, s int, p ResumePayload) (int, error) {
		return s + p.Delta, nil
	}); err != nil {
		t.Fatalf("RegisterJSONContinuation error: %v", err)
	}
	runner, err := g.Compile(graph.Config[int]{MachineID: "json-resume-v1", Clone: graph.ValueClone[int]})
	if err != nil {
		t.Fatalf("compile error: %v", err)
	}
	resExecution, res, err := runner.Start(context.Background(), "json-run", 10, nil, graph.Options{})
	if err != nil || res.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", res, err)
	}
	res, err = resExecution.Advance(context.Background(), []graph.ResumeInput{
		{InvocationID: mustCheckpoint(resExecution).Invocations[0].ID, CallID: mustCheckpoint(resExecution).Invocations[0].CallID, Payload: []byte(`{"delta": 5}`)},
	}, graph.Options{})
	if err != nil || res.Status != graph.StatusCompleted || res.Final == nil || *res.Final != 15 {
		t.Fatalf("resume = %+v, %v", res, err)
	}
}

func TestRunnerFork(t *testing.T) {
	g := graph.New[int]("start")
	startCalls := 0
	stepCalls := 0
	applyCalls := 0
	finishCalls := 0
	node(t, g, "start", func(_ context.Context, call graph.CallInfo, state int) (graph.Transition[int], error) {
		startCalls++
		return graph.To(state+1, "step1"), nil
	})
	node(t, g, "step1", func(_ context.Context, call graph.CallInfo, state int) (graph.Transition[int], error) {
		stepCalls++
		return graph.Wait(state, "input", "finish"), nil
	})
	node(t, g, "finish", func(_ context.Context, call graph.CallInfo, state int) (graph.Transition[int], error) {
		finishCalls++
		return graph.EndExecution(state), nil
	})
	edge(t, g, "start", "step1")
	edge(t, g, "step1", "finish")
	if err := graph.RegisterJSONContinuation(g, "input", func(_ context.Context, call graph.CallInfo, state, delta int) (int, error) {
		applyCalls++
		return state + delta, nil
	}); err != nil {
		t.Fatal(err)
	}
	store := newMemoryStore(t)
	r := intRunner(t, g)
	resExecution, res, err := r.Start(context.Background(), "run-original", 10, store, graph.Options{MaxSteps: 1})
	if err != nil || res.Status != graph.StatusBudget {
		t.Fatalf("start = %+v, %v", res, err)
	}
	source := mustCheckpoint(resExecution)
	if startCalls != 1 || stepCalls != 0 {
		t.Fatalf("initial budget call counts = start %d, step %d", startCalls, stepCalls)
	}

	// Fork several ready recovery points without running their next callbacks.
	forked, err := r.Fork(context.Background(), "run-forked", source, store)
	if err != nil || forked.RunID != "run-forked" || forked.Revision != 1 {
		t.Fatalf("fork = %+v, %v", forked, err)
	}
	if len(forked.Invocations) != 1 || forked.Invocations[0].Node != "step1" || forked.Invocations[0].State != 11 || forked.Invocations[0].CallID != source.Invocations[0].CallID {
		t.Fatalf("fork did not preserve active position: %+v", forked)
	}
	secondFork, err := r.Fork(context.Background(), "run-forked-2", source, store)
	if err != nil || secondFork.RunID != "run-forked-2" || secondFork.Revision != 1 {
		t.Fatalf("second fork = %+v, %v", secondFork, err)
	}
	if startCalls != 1 || stepCalls != 0 || applyCalls != 0 || finishCalls != 0 {
		t.Fatalf("Fork executed callbacks: start %d, step %d, apply %d, finish %d", startCalls, stepCalls, applyCalls, finishCalls)
	}
	loadedFork, err := store.Load(context.Background(), "run-forked")
	if err != nil || loadedFork.Revision != 1 || loadedFork.Invocations[0].Node != "step1" {
		t.Fatalf("stored fork = %+v, %v", loadedFork, err)
	}
	loadedSecond, err := store.Load(context.Background(), "run-forked-2")
	if err != nil || loadedSecond.Revision != 1 || loadedSecond.Invocations[0].Node != "step1" {
		t.Fatalf("stored second fork = %+v, %v", loadedSecond, err)
	}

	// Only explicit Recover calls begin executing each copied position.
	forkedExecution, forkedResult, err := r.Recover(context.Background(), "run-forked", nil, store, graph.Options{})
	if err != nil || forkedResult.Status != graph.StatusWaiting || forkedResult.Header.RunID != "run-forked" {
		t.Fatalf("recover fork = %+v, %v", forkedResult, err)
	}
	originalExecution, originalResult, err := r.Recover(context.Background(), "run-original", nil, store, graph.Options{})
	if err != nil || originalResult.Status != graph.StatusWaiting || stepCalls != 2 {
		t.Fatalf("recover original = %+v, step calls %d, %v", originalResult, stepCalls, err)
	}
	// Advance original with +100
	_, origFinal, err := r.Resume(context.Background(), mustCheckpoint(originalExecution), []graph.ResumeInput{
		{InvocationID: mustCheckpoint(originalExecution).Invocations[0].ID, CallID: mustCheckpoint(originalExecution).Invocations[0].CallID, Payload: []byte(`100`)},
	}, store, graph.Options{})
	if err != nil || origFinal.Status != graph.StatusCompleted || *origFinal.Final != 111 {
		t.Fatalf("orig final = %+v, %v", origFinal, err)
	}
	// Advance forked with +500
	_, forkFinal, err := r.Resume(context.Background(), mustCheckpoint(forkedExecution), []graph.ResumeInput{
		{InvocationID: mustCheckpoint(forkedExecution).Invocations[0].ID, CallID: mustCheckpoint(forkedExecution).Invocations[0].CallID, Payload: []byte(`500`)},
	}, store, graph.Options{})
	if err != nil || forkFinal.Status != graph.StatusCompleted || *forkFinal.Final != 511 {
		t.Fatalf("fork final = %+v, %v", forkFinal, err)
	}
	if applyCalls != 2 || finishCalls != 2 {
		t.Fatalf("explicit execution call counts = apply %d, finish %d", applyCalls, finishCalls)
	}

	// Verify both exist independently in store
	origLoaded, err := store.Load(context.Background(), "run-original")
	if err != nil || *origLoaded.Final != 111 {
		t.Fatalf("load orig = %+v, %v", origLoaded, err)
	}
	forkLoaded, err := store.Load(context.Background(), "run-forked")
	if err != nil || *forkLoaded.Final != 511 {
		t.Fatalf("load fork = %+v, %v", forkLoaded, err)
	}

	// Forking completed run fails with ErrRunCompleted
	if _, err := r.Fork(context.Background(), "run-fork-completed", origLoaded, store); !errors.Is(err, graph.ErrRunCompleted) {
		t.Fatalf("fork completed = %v, want ErrRunCompleted", err)
	}
}

func TestRunnerForkEdgeCases(t *testing.T) {
	g := graph.New[int]("start")
	node(t, g, "start", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndExecution(v), nil
	})
	r := intRunner(t, g)
	store := newMemoryStore(t)

	var nilRunner *graph.Runner[int]
	ctx := context.Background()
	cp := graph.Checkpoint[int]{
		FormatVersion: graph.CheckpointFormatVersion,
		MachineID:     "numbers-v1",
		RunID:         "run-cp",
		Revision:      1,
		NextID:        3,
		Invocations: []graph.Invocation[int]{
			{ID: "i1", CallID: "c2", Node: "start", Status: graph.InvocationReady},
		},
	}

	// 1. nil runner
	if _, err := nilRunner.Fork(ctx, "run-1", cp, nil); err == nil {
		t.Fatal("expected error on nil runner")
	}

	// 2. nil ctx
	if _, err := r.Fork(nil, "run-1", cp, nil); err == nil {
		t.Fatal("expected error on nil ctx")
	}

	// 3. invalid runID
	if _, err := r.Fork(ctx, "", cp, nil); err == nil {
		t.Fatal("expected error on empty runID")
	}
	if _, err := r.Fork(ctx, "  spaces  ", cp, nil); err == nil {
		t.Fatal("expected error on runID with spaces")
	}
	if _, err := r.Fork(ctx, cp.RunID, cp, nil); err == nil {
		t.Fatal("expected error when fork run ID matches the source")
	}

	// 4. invalid checkpoint (mismatched machine ID)
	invalidCp := cp
	invalidCp.MachineID = "wrong-machine"
	if _, err := r.Fork(ctx, "run-1", invalidCp, nil); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("expected ErrInvalidCheckpoint, got: %v", err)
	}

	// 5. Store conflict (runID already exists)
	if err := store.Create(ctx, cp); err != nil {
		t.Fatal(err)
	}
	existingRun := cp
	existingRun.RunID = "run-cp-fork"
	if err := store.Create(ctx, existingRun); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Fork(ctx, "run-cp-fork", cp, store); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("expected ErrConflict, got: %v", err)
	}

	// 6. Canceled context
	cancCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.Fork(cancCtx, "run-canc", cp, store); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if _, err := store.Load(ctx, "run-canc"); err == nil {
		t.Fatalf("cancelled Fork created a run: %v", err)
	}
}

func TestJSONCloneError(t *testing.T) {
	type unmarshalable struct {
		Ch chan int
	}
	val := unmarshalable{Ch: make(chan int)}
	if _, err := graph.JSONClone(val); err == nil {
		t.Fatal("expected error cloning unmarshalable type")
	}
}

func TestExportMermaid(t *testing.T) {
	child := graph.New[int]("sub_entry")
	node(t, child, "sub_entry", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.Return(v), nil
	})

	g := graph.New[int]("start")
	node(t, g, "start", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "worker"), nil
	})
	if err := g.AddSubgraph("worker", child); err != nil {
		t.Fatal(err)
	}
	node(t, g, "finish", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndExecution(v), nil
	})
	edge(t, g, "start", "worker")
	edge(t, g, "worker", "finish")

	builderDiagram, diagramErr := g.ExportMermaid()
	if diagramErr != nil {
		t.Fatal(diagramErr)
	}
	if !strings.Contains(builderDiagram, `(["start (entry)"])`) || !strings.Contains(builderDiagram, `["worker"]`) || !strings.Contains(builderDiagram, " --> ") {
		t.Fatal(builderDiagram)
	}

	runner := intRunner(t, g)
	runnerDiagram := runner.ExportMermaid()
	if !strings.Contains(runnerDiagram, `(["start (entry)"])`) || !strings.Contains(runnerDiagram, `["worker/sub_entry"]`) || !strings.Contains(runnerDiagram, "-.->|return|") {
		t.Fatal(runnerDiagram)
	}
}
