package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint/memory"
)

func TestStaleExecutionHandleChecksHeaderBeforeCallbacks(t *testing.T) {
	var calls int
	g := graph.New[int]("loop")
	node(t, g, "loop", func(_ context.Context, _ graph.CallInfo, value int) (graph.Transition[int], error) {
		calls++
		if value == 3 {
			return graph.EndExecution(value), nil
		}
		return graph.To(value+1, "loop"), nil
	})
	edge(t, g, "loop", "loop")
	runner := intRunner(t, g)
	store, err := memory.New[int](graph.ValueClone[int])
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	stale, first, err := runner.Start(ctx, "stale-handle", 0, store, graph.Options{MaxSteps: 1, MaxConcurrency: 1})
	if err != nil || first.Status != graph.StatusBudget || first.Header.Revision != 2 || calls != 1 {
		t.Fatalf("start = %+v, %v, calls=%d", first, err, calls)
	}
	_, external, err := runner.Recover(ctx, "stale-handle", nil, store, graph.Options{MaxSteps: 1, MaxConcurrency: 1})
	if err != nil || external.Status != graph.StatusBudget || external.Header.Revision != 3 || calls != 2 {
		t.Fatalf("external advance = %+v, %v, calls=%d", external, err, calls)
	}
	out, err := stale.Advance(ctx, nil, graph.Options{MaxSteps: 1, MaxConcurrency: 1})
	if !errors.Is(err, graph.ErrConflict) || out.Header.Revision != first.Header.Revision || calls != 2 {
		t.Fatalf("stale advance = %+v, %v, calls=%d; callback must not run", out, err, calls)
	}
}

func TestPartialResumeAcrossParallelInvocations(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "a", "b"), nil
	})
	for _, name := range []string{"a", "b"} {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
			return graph.Wait(v, "add", "join"), nil
		})
		edge(t, g, "fork", name)
	}
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, _ int, branches []graph.BranchResult[int]) (int, error) {
		values := graph.BranchValues(branches)
		return values[0] + values[1], nil
	}}); err != nil {
		t.Fatal(err)
	}
	edge(t, g, "a", "join")
	edge(t, g, "b", "join")
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndExecution(v), nil
	})
	edge(t, g, "join", "done")
	if err := graph.RegisterContinuation(g, "add", func(b []byte) (int, error) { return strconv.Atoi(string(b)) }, func(_ context.Context, _ graph.CallInfo, state, value int) (int, error) { return state + value, nil }); err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	firstExecution, first, err := r.Start(context.Background(), "partial", 0, nil, graph.Options{MaxConcurrency: 2})
	if err != nil || first.Status != graph.StatusWaiting {
		t.Fatalf("first = %+v, %v", first, err)
	}
	ids := make(map[string]string)
	callIDs := map[string]string{}
	for _, inv := range mustCheckpoint(firstExecution).Invocations {
		if inv.Status == graph.InvocationWaiting {
			ids[inv.Node] = inv.ID
			callIDs[inv.Node] = inv.CallID
		}
	}
	if len(ids) != 2 {
		t.Fatalf("waiting invocations = %v", ids)
	}
	corrupt := mustCheckpoint(firstExecution)
	corrupt.Groups = append([]graph.ActivationGroup(nil), corrupt.Groups...)
	corrupt.Groups[0].Branches = []graph.ActivationBranch{{InvocationID: "missing"}, {InvocationID: ids["b"]}}
	if _, _, err := r.Resume(context.Background(), corrupt, nil, nil, graph.Options{}); err == nil {
		t.Fatal("invalid group checkpoint accepted")
	}
	middleExecution, middle, err := r.Resume(context.Background(), mustCheckpoint(firstExecution), []graph.ResumeInput{{InvocationID: ids["a"], CallID: callIDs["a"], Payload: []byte("3")}}, nil, graph.Options{})
	if err != nil || middle.Status != graph.StatusWaiting {
		t.Fatalf("middle = %+v, %v", middle, err)
	}
	_, last, err := r.Resume(context.Background(), mustCheckpoint(middleExecution), []graph.ResumeInput{{InvocationID: ids["b"], CallID: callIDs["b"], Payload: []byte("4")}}, nil, graph.Options{})
	if err != nil || last.Status != graph.StatusCompleted || last.Final == nil || *last.Final != 7 {
		t.Fatalf("last = %+v, %v", last, err)
	}
}

func TestGroupFailureCancelsSibling(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "bad", "waiting"), nil
	})
	started := make(chan struct{})
	cancelled := make(chan struct{})
	if err := g.AddNode(graph.NodeSpec[int]{Name: "bad", OnError: graph.FailGroup, Run: func(_ context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		<-started
		return graph.Transition[int]{}, errors.New("group failed")
	}}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "waiting", func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return graph.Transition[int]{}, ctx.Err()
	})
	edge(t, g, "fork", "bad")
	edge(t, g, "fork", "waiting")
	r := intRunner(t, g)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, out, err := r.Start(ctx, "group-failure", 0, nil, graph.Options{MaxConcurrency: 2})
	if err != nil || out.Status != graph.StatusCompletedWithFailures || (!out.Header.HadLocalFailures || out.Header.Failure != nil) {
		t.Fatalf("group failure = %+v, %v", out, err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("running sibling was not cancelled and joined")
	}
}

type notifyingStore struct {
	*memory.Store[int]
	committed chan graph.Checkpoint[int]
}

func (n *notifyingStore) Commit(ctx context.Context, expected uint64, change graph.Change[int]) error {
	next := change.Checkpoint()
	if err := n.Store.Commit(ctx, expected, change); err != nil {
		return err
	}
	if next.Steps == 2 {
		select {
		case n.committed <- copyIntCheckpoint(next):
		default:
		}
	}
	return nil
}

func TestCancelledRunKeepsInFlightInvocationPending(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.To(v, "fast", "slow"), nil
	})
	node(t, g, "fast", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndBranch[int](), nil
	})
	node(t, g, "slow", func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		<-ctx.Done()
		return graph.Transition[int]{}, ctx.Err()
	})
	edge(t, g, "fork", "fast")
	edge(t, g, "fork", "slow")
	r := intRunner(t, g)
	store := &notifyingStore{Store: newMemoryStore(t), committed: make(chan graph.Checkpoint[int], 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type output struct {
		result graph.Result[int]
		err    error
	}
	done := make(chan output, 1)
	go func() {
		_, result, err := r.Start(ctx, "cancel", 0, store, graph.Options{MaxConcurrency: 2})
		done <- output{result, err}
	}()
	select {
	case <-store.committed:
		cancel()
	case <-ctx.Done():
		t.Fatal("fast branch did not commit")
	}
	out := <-done
	if !errors.Is(out.err, context.Canceled) || out.result.Status != graph.StatusCancelled {
		t.Fatalf("cancel = %+v, %v", out.result, out.err)
	}
	stored, err := store.Load(context.Background(), "cancel")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Steps != 2 {
		t.Fatalf("accepted branch outcome missing: %+v", stored)
	}
	var pending bool
	for _, inv := range stored.Invocations {
		if inv.Node == "slow" && inv.Status == graph.InvocationReady {
			pending = true
		}
	}
	if !pending {
		t.Fatalf("in-flight branch was not persisted as pending: %+v", stored.Invocations)
	}
	if stored.Revision != out.result.Header.Revision {
		t.Fatalf("returned revision %d, stored revision %d", out.result.Header.Revision, stored.Revision)
	}
}

func TestEndBranchCompletesWithoutResult(t *testing.T) {
	g := graph.New[int]("fork")
	recovering := false
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		if recovering {
			t.Error("completed recovery executed a callback")
		}
		return graph.To(v, "first", "second"), nil
	})
	node(t, g, "first", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndBranch[int](), nil
	})
	node(t, g, "second", func(_ context.Context, _ graph.CallInfo, v int) (graph.Transition[int], error) {
		return graph.EndBranch[int](), nil
	})
	edge(t, g, "fork", "first")
	edge(t, g, "fork", "second")
	runner := intRunner(t, g)
	opts := graph.Options{MaxConcurrency: 2}
	optsStore := newMemoryStore(t)
	outExecution, out, err := runner.Start(context.Background(), "ended-branches", 0, optsStore, opts)
	if err != nil || out.Status != graph.StatusCompleted || len(mustCheckpoint(outExecution).Invocations) != 0 {
		t.Fatalf("terminals = %+v, %v", out, err)
	}
	if out.Final != nil {
		t.Fatal("unjoined branches produced a single final state")
	}
	if len(mustCheckpoint(outExecution).Groups) != 0 {
		t.Fatal(fmt.Sprint("unsettled group: ", mustCheckpoint(outExecution).Groups))
	}
	recovering = true
	recoveredExecution, recovered, err := runner.Recover(context.Background(), out.Header.RunID, nil, optsStore, opts)
	if err != nil || recovered.Status != graph.StatusCompleted || recovered.Header.Revision != out.Header.Revision || recovered.Final != nil || recovered.Header.Failure != nil || len(mustCheckpoint(recoveredExecution).Invocations) != 0 || len(mustCheckpoint(recoveredExecution).Groups) != 0 {
		t.Fatalf("natural completion recovery = %+v, %v", recovered, err)
	}
}
