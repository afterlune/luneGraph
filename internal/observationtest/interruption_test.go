package observationtest

import (
	"context"
	"errors"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

func TestInterruptionDrainsWorkersBeforePublicFinish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	exited := make(chan struct{})
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "slow", "stop"), nil
	})
	node(t, g, "slow", func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		close(started)
		<-ctx.Done()
		close(exited)
		return graph.EndExecution(99), nil
	})
	node(t, g, "stop", func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		select {
		case <-started:
		case <-ctx.Done():
			return graph.Transition[int]{}, ctx.Err()
		}
		return graph.EndExecution(7), graph.Interrupt(nil)
	})
	edge(t, g, "fork", "slow")
	edge(t, g, "fork", "stop")
	log := &recorder{}
	store := memoryStore(t)
	result, err := compile(t, g).Start(ctx, "interruption", 0, graph.Options[int]{Store: store, Observer: log, MaxConcurrency: 2})
	if result.Status != graph.StatusInterrupted || !errors.Is(err, graph.ErrInterrupted) || result.Checkpoint.Completed || result.Checkpoint.Revision != 2 || result.Checkpoint.Steps != 1 || result.Checkpoint.Final != nil {
		t.Fatalf("result=%+v %v", result, err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("returned before worker exited")
	}
	events := log.snapshot()
	checkCall(t, events, graph.OperationStart, result, err)
	nodes := finished(events, graph.OperationNode)
	if len(nodes) != 3 {
		t.Fatalf("callback finishes=%+v", nodes)
	}
	for _, e := range nodes {
		if e.Node == "stop" && !errors.Is(e.Err, graph.ErrInterrupted) {
			t.Fatalf("missing signal: %+v", e)
		}
		if e.Node == "slow" && (e.Err != nil || e.Action != graph.ActionEndExecution) {
			t.Fatalf("discarded finish=%+v", e)
		}
	}
	if events[len(events)-1].Operation != graph.OperationStart || events[len(events)-1].Status != graph.StatusInterrupted {
		t.Fatalf("public finish=%+v", events[len(events)-1])
	}
	saved, loadErr := store.Load(ctx, "interruption")
	if loadErr != nil || saved.Revision != 2 || len(finished(events, graph.OperationCompareAndSwap)) != 1 {
		t.Fatalf("saved=%+v %v", saved, loadErr)
	}
}
