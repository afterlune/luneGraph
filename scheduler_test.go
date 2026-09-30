package graph_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

func TestReadyBranchesRotateAcrossStoredResumes(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "loop", "once"), nil
	})
	loopCalls, onceCalls := 0, 0
	node(t, g, "loop", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		loopCalls++
		return graph.To(v+1, "loop"), nil
	})
	node(t, g, "once", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		onceCalls++
		return graph.EndBranch[int](), nil
	})
	for _, pair := range [][2]string{{"fork", "loop"}, {"fork", "once"}, {"loop", "loop"}} {
		edge(t, g, pair[0], pair[1])
	}
	r := intRunner(t, g)
	store := newMemoryStore(t)
	opts := graph.Options[int]{Store: store, MaxConcurrency: 1, MaxSteps: 1}
	first, err := r.Start(context.Background(), "fair", 0, opts)
	if err != nil || first.Status != graph.StatusBudget || first.Checkpoint.ScheduleCursor != 1 {
		t.Fatalf("fork = %+v, %v", first, err)
	}
	second, err := r.Resume(context.Background(), first.Checkpoint, nil, opts)
	if err != nil || second.Status != graph.StatusBudget || loopCalls != 1 || onceCalls != 0 || second.Checkpoint.ScheduleCursor != 4 {
		t.Fatalf("first branch = %+v, %v", second, err)
	}
	loaded, err := store.Load(context.Background(), "fair")
	if err != nil || loaded.ScheduleCursor != 4 {
		t.Fatalf("stored cursor = %+v, %v", loaded, err)
	}
	third, err := r.Resume(context.Background(), loaded, nil, opts)
	if err != nil || third.Status != graph.StatusBudget || loopCalls != 1 || onceCalls != 1 || third.Checkpoint.ScheduleCursor != 6 {
		t.Fatalf("rotated branch = %+v, %v", third, err)
	}
}

func TestFirstReceivedEndExecutionWins(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "slow", "winner"), nil
	})
	slowStarted := make(chan struct{})
	slowExited := make(chan struct{})
	node(t, g, "slow", func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		close(slowStarted)
		<-ctx.Done()
		close(slowExited)
		return graph.EndExecution(99), nil
	})
	winnerStarted := make(chan struct{})
	releaseWinner := make(chan struct{})
	node(t, g, "winner", func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		close(winnerStarted)
		select {
		case <-releaseWinner:
			return graph.EndExecution(7), nil
		case <-ctx.Done():
			return graph.Transition[int]{}, ctx.Err()
		}
	})
	edge(t, g, "fork", "slow")
	edge(t, g, "fork", "winner")
	r := intRunner(t, g)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type response struct {
		result graph.Result[int]
		err    error
	}
	done := make(chan response, 1)
	go func() {
		result, err := r.Start(ctx, "winner", 0, graph.Options[int]{MaxConcurrency: 2})
		done <- response{result, err}
	}()
	for _, started := range []chan struct{}{slowStarted, winnerStarted} {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("both branches did not start")
		}
	}
	close(releaseWinner)
	select {
	case out := <-done:
		if out.err != nil || out.result.Status != graph.StatusCompleted || out.result.Checkpoint.Final == nil || *out.result.Checkpoint.Final != 7 {
			t.Fatalf("first completion = %+v, %v", out.result, out.err)
		}
	case <-ctx.Done():
		t.Fatal("winner did not complete")
	}
	select {
	case <-slowExited:
	default:
		t.Fatal("Start returned before cancelled sibling exited")
	}
}

func TestCancellationWaitsForRunningNode(t *testing.T) {
	g := graph.New[int]("work")
	started := make(chan struct{})
	observedCancel := make(chan struct{})
	release := make(chan struct{})
	node(t, g, "work", func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		close(started)
		<-ctx.Done()
		close(observedCancel)
		<-release
		return graph.Transition[int]{}, ctx.Err()
	})
	r := intRunner(t, g)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type response struct {
		result graph.Result[int]
		err    error
	}
	done := make(chan response, 1)
	go func() {
		result, err := r.Start(ctx, "cancel-wait", 0, graph.Options[int]{})
		done <- response{result, err}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("worker did not start")
	}
	cancel()
	select {
	case <-observedCancel:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not observe cancellation")
	}
	select {
	case out := <-done:
		t.Fatalf("returned before worker exit: %+v, %v", out.result, out.err)
	default:
	}
	close(release)
	select {
	case out := <-done:
		if !errors.Is(out.err, context.Canceled) || out.result.Status != graph.StatusCancelled || out.result.Checkpoint.Steps != 0 {
			t.Fatalf("cancel = %+v, %v", out.result, out.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled run did not return after worker exit")
	}
}

func TestResumeBatchCommitsExecutionFailureAfterInputPrefix(t *testing.T) {
	boom := errors.New("second input failed")
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "a", "b"), nil
	})
	node(t, g, "a", func(_ context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		return graph.Wait(1, "input", "join"), nil
	})
	if err := g.AddNode(graph.NodeSpec[int]{Name: "b", OnError: graph.FailExecution, Run: func(_ context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		return graph.Wait(2, "input", "join"), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, values []int) (int, error) {
		return values[0], nil
	}}); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"fork", "a"}, {"fork", "b"}, {"a", "join"}, {"b", "join"}} {
		edge(t, g, pair[0], pair[1])
	}
	applyCalls := 0
	if err := graph.RegisterContinuation(g, "input", func(payload []byte) (int, error) {
		return strconv.Atoi(string(payload))
	}, func(_ context.Context, _ graph.CallInfo, state, value int) (int, error) {
		applyCalls++
		if state == 2 {
			return 0, boom
		}
		return state + value, nil
	}); err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	store := newMemoryStore(t)
	opts := graph.Options[int]{Store: store, MaxConcurrency: 2}
	first, err := r.Start(context.Background(), "batch", 0, opts)
	if err != nil || first.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", first, err)
	}
	ids := map[string]string{}
	for _, inv := range first.Checkpoint.Invocations {
		if inv.Status == graph.InvocationWaiting {
			ids[inv.Node] = inv.ID
		}
	}
	bad := []graph.ResumeInput{{InvocationID: ids["a"], Payload: []byte("3")}, {InvocationID: ids["b"], Payload: []byte("bad")}}
	rejected, err := r.Resume(context.Background(), first.Checkpoint, bad, opts)
	if err == nil || rejected.Checkpoint.Revision != first.Checkpoint.Revision || applyCalls != 0 {
		t.Fatalf("predecode rejection = %+v, %v", rejected, err)
	}
	inputs := []graph.ResumeInput{{InvocationID: ids["a"], Payload: []byte("3")}, {InvocationID: ids["b"], Payload: []byte("4")}}
	partial, err := r.Resume(context.Background(), first.Checkpoint, inputs, opts)
	if !errors.Is(err, boom) || partial.Checkpoint.Revision != first.Checkpoint.Revision+2 || !partial.Checkpoint.Completed || partial.Checkpoint.Failure == nil || applyCalls != 2 {
		t.Fatalf("partial resume = %+v, %v", partial, err)
	}
	stored, err := store.Load(context.Background(), "batch")
	if err != nil || stored.Revision != partial.Checkpoint.Revision || !stored.Completed || len(stored.Invocations) != 0 {
		t.Fatalf("stored failed run = %+v, %v", stored, err)
	}
}

func findInvocation[S any](s graph.Checkpoint[S], id string) (int, *graph.Invocation[S]) {
	for i := range s.Invocations {
		if s.Invocations[i].ID == id {
			return i, &s.Invocations[i]
		}
	}
	return -1, nil
}
