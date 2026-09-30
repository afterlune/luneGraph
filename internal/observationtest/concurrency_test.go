package observationtest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

func TestSharedObserverRunnerAndStore(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "a", "b"), nil
	})
	for _, name := range []string{"a", "b"} {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.To(s+1, "join"), nil
		})
		edge(t, g, "fork", name)
	}
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, values []int) (int, error) {
		return values[0] + values[1], nil
	}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		edge(t, g, name, "join")
	}
	r, log, store := compile(t, g), &recorder{}, memoryStore(t)
	var workers sync.WaitGroup
	for i := range 16 {
		workers.Go(func() {
			result, err := r.Start(context.Background(), fmt.Sprintf("shared-%d", i), i, graph.Options[int]{Store: store, Observer: log, MaxConcurrency: 2})
			if err != nil || result.Status != graph.StatusCompleted || len(result.Checkpoint.Terminals) != 1 || result.Checkpoint.Terminals[0].State != 2*(i+1) {
				t.Errorf("run %d: %+v, %v", i, result, err)
			}
		})
	}
	workers.Wait()
	byCall := map[uint64][]graph.Event{}
	for _, e := range log.snapshot() {
		byCall[e.OperationID] = append(byCall[e.OperationID], e)
	}
	if len(byCall) != 16 {
		t.Fatalf("call count = %d", len(byCall))
	}
	for _, events := range byCall {
		checkPairs(t, events)
		joins := finished(events, graph.OperationJoin)
		if len(joins) != 1 || joins[0].CallID == "" || joins[0].Node != "join" {
			t.Fatalf("join identity = %+v", joins)
		}
		if events[len(events)-1].Operation != graph.OperationStart {
			t.Fatal("public finish preceded worker finish")
		}
	}
}

func TestCancellationAndDiscardedCallbackFinish(t *testing.T) {
	for _, endExecution := range []bool{false, true} {
		t.Run(fmt.Sprint(endExecution), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			started := make(chan struct{})
			g := graph.New[int]("fork")
			node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.To(s, "slow", "winner"), nil
			})
			node(t, g, "slow", func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				close(started)
				<-ctx.Done()
				// A successful result is still discarded after cancellation.
				return graph.EndExecution(99), nil
			})
			node(t, g, "winner", func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				select {
				case <-started:
				case <-ctx.Done():
					return graph.Transition[int]{}, ctx.Err()
				}
				if !endExecution {
					cancel()
				}
				return graph.EndExecution(7), nil
			})
			edge(t, g, "fork", "slow")
			edge(t, g, "fork", "winner")
			log := &recorder{}
			result, err := compile(t, g).Start(ctx, "drain", 0, graph.Options[int]{Observer: log, MaxConcurrency: 2})
			if endExecution {
				if err != nil || result.Status != graph.StatusCompleted || *result.Checkpoint.Final != 7 {
					t.Fatalf("winner = %+v, %v", result, err)
				}
			} else if result.Status != graph.StatusCancelled || err == nil || result.Checkpoint.Completed {
				t.Fatalf("cancel = %+v, %v", result, err)
			}
			events := log.snapshot()
			checkCall(t, events, graph.OperationStart, result, err)
			nodes := finished(events, graph.OperationNode)
			if len(nodes) != 3 {
				t.Fatalf("discarded callback not observed: %+v", nodes)
			}
			for _, e := range nodes {
				if e.Node == "slow" && (e.Err != nil || e.Action != graph.ActionEndExecution) {
					t.Fatalf("slow callback = %+v", e)
				}
			}
		})
	}
}

func TestNodeObserverDeliveryIsConcurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	arrivals := make(chan struct{}, 2)
	release := make(chan struct{})
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "a", "b"), nil
	})
	for _, name := range []string{"a", "b"} {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.EndBranch(s), nil
		})
		edge(t, g, "fork", name)
	}
	observer := graph.ObserverFunc(func(ctx context.Context, e graph.Event) {
		if e.Operation == graph.OperationNode && e.Phase == graph.PhaseStarted && e.Node != "fork" {
			arrivals <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	done := make(chan error, 1)
	r := compile(t, g)
	go func() {
		_, err := r.Start(ctx, "delivery", 0, graph.Options[int]{Observer: observer, MaxConcurrency: 2})
		done <- err
	}()
	for range 2 {
		select {
		case <-arrivals:
		case <-ctx.Done():
			t.Fatal("observer delivery was serialized")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
