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
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, values []int) (int, error) {
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
	first, err := r.Start(context.Background(), "partial", 0, graph.Options[int]{MaxConcurrency: 2})
	if err != nil || first.Status != graph.StatusWaiting {
		t.Fatalf("first = %+v, %v", first, err)
	}
	ids := make(map[string]string)
	for _, inv := range first.Checkpoint.Invocations {
		if inv.Status == graph.InvocationWaiting {
			ids[inv.Node] = inv.ID
		}
	}
	if len(ids) != 2 {
		t.Fatalf("waiting invocations = %v", ids)
	}
	corrupt := first.Checkpoint
	corrupt.Groups = append([]graph.ActivationGroup(nil), corrupt.Groups...)
	corrupt.Groups[0].Children = []string{"missing", ids["b"]}
	if _, err := r.Resume(context.Background(), corrupt, nil, graph.Options[int]{}); err == nil {
		t.Fatal("invalid group checkpoint accepted")
	}
	middle, err := r.Resume(context.Background(), first.Checkpoint, []graph.ResumeInput{{InvocationID: ids["a"], Payload: []byte("3")}}, graph.Options[int]{})
	if err != nil || middle.Status != graph.StatusWaiting {
		t.Fatalf("middle = %+v, %v", middle, err)
	}
	last, err := r.Resume(context.Background(), middle.Checkpoint, []graph.ResumeInput{{InvocationID: ids["b"], Payload: []byte("4")}}, graph.Options[int]{})
	if err != nil || last.Status != graph.StatusCompleted || last.Checkpoint.Final == nil || *last.Checkpoint.Final != 7 {
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
	out, err := r.Start(ctx, "group-failure", 0, graph.Options[int]{MaxConcurrency: 2})
	if err != nil || out.Status != graph.StatusCompletedWithFailures || (!out.Checkpoint.HadLocalFailures || out.Checkpoint.Failure != nil) {
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

func (n *notifyingStore) CompareAndSwap(ctx context.Context, expected uint64, next graph.Checkpoint[int]) error {
	if err := n.Store.CompareAndSwap(ctx, expected, next); err != nil {
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
		result, err := r.Start(ctx, "cancel", 0, graph.Options[int]{Store: store, MaxConcurrency: 2})
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
	if stored.Revision != out.result.Checkpoint.Revision {
		t.Fatalf("returned revision %d, stored revision %d", out.result.Checkpoint.Revision, stored.Revision)
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
	opts := graph.Options[int]{MaxConcurrency: 2, Store: newMemoryStore(t)}
	out, err := runner.Start(context.Background(), "ended-branches", 0, opts)
	if err != nil || out.Status != graph.StatusCompleted || len(out.Checkpoint.Invocations) != 0 {
		t.Fatalf("terminals = %+v, %v", out, err)
	}
	if out.Checkpoint.Final != nil {
		t.Fatal("unjoined branches produced a single final state")
	}
	if len(out.Checkpoint.Groups) != 0 {
		t.Fatal(fmt.Sprint("unsettled group: ", out.Checkpoint.Groups))
	}
	recovering = true
	recovered, err := runner.Recover(context.Background(), out.Checkpoint.RunID, nil, opts)
	if err != nil || recovered.Status != graph.StatusCompleted || recovered.Checkpoint.Revision != out.Checkpoint.Revision || recovered.Checkpoint.Final != nil || recovered.Checkpoint.Failure != nil || len(recovered.Checkpoint.Invocations) != 0 || len(recovered.Checkpoint.Groups) != 0 {
		t.Fatalf("natural completion recovery = %+v, %v", recovered, err)
	}
}
