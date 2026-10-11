package executor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/internal/limit"
)

func TestJoinlessGroupSettlementNeedsNoCallbackPermit(t *testing.T) {
	limiter, err := graph.NewLimiter(2)
	if err != nil {
		t.Fatal(err)
	}
	holderCtx, cancelHolder := context.WithCancel(context.Background())
	held := make(chan struct{})
	holderDone := make(chan error, 1)
	holderGraph := graph.New[int]("hold")
	if err := holderGraph.AddNode(graph.NodeSpec[int]{Name: "hold", Run: func(ctx context.Context, _ graph.CallInfo, value int) (graph.Transition[int], error) {
		close(held)
		<-ctx.Done()
		return graph.Transition[int]{}, ctx.Err()
	}}); err != nil {
		t.Fatal(err)
	}
	holder, err := holderGraph.Compile(graph.Config[int]{MachineID: "no-merge-holder", Clone: graph.ValueClone[int]})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _, err := holder.Start(holderCtx, "holder", 0, nil, graph.Options{MaxConcurrency: 1, Limiter: limiter})
		holderDone <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-held:
	case <-ctx.Done():
		cancelHolder()
		t.Fatal("holder did not acquire its permit")
	}
	defer func() {
		cancelHolder()
		if err := <-holderDone; !errors.Is(err, context.Canceled) {
			t.Errorf("holder result: %v", err)
		}
	}()
	g := graph.New[int]("root")
	add := func(name string, run graph.Node[int]) {
		t.Helper()
		if err := g.AddNode(graph.NodeSpec[int]{Name: name, Run: run}); err != nil {
			t.Fatal(err)
		}
	}
	add("root", func(_ context.Context, _ graph.CallInfo, value int) (graph.Transition[int], error) {
		return graph.To(value, "a", "b"), nil
	})
	for _, name := range []string{"a", "b"} {
		add(name, func(_ context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
			return graph.EndBranch[int](), nil
		})
	}
	for _, name := range []string{"a", "b"} {
		if err := g.AddEdge("root", name); err != nil {
			t.Fatal(err)
		}
	}
	runner, err := g.Compile(graph.Config[int]{MachineID: "no-merge-limit", Clone: graph.ValueClone[int]})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct {
		out graph.Result[int]
		err error
	}, 1)
	go func() {
		_, out, err := runner.Start(ctx, "no-merge", 0, nil, graph.Options{MaxConcurrency: 2, Limiter: limiter})
		done <- struct {
			out graph.Result[int]
			err error
		}{out, err}
	}()
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal("holder stopped before the run")
	}
	select {
	case got := <-done:
		if got.err != nil || got.out.Status != graph.StatusCompleted || !got.out.Header.Completed || got.out.Final != nil {
			t.Fatalf("result=%+v err=%v", got.out, got.err)
		}
	case <-ctx.Done():
		t.Fatal("joinless group waited for a callback permit")
	}
	if !limit.TryAcquire(limiter) {
		t.Fatal("holder consumed more than one permit")
	}
	if limit.TryAcquire(limiter) {
		limit.Release(limiter)
		limit.Release(limiter)
		t.Fatal("holder exited before the completed run")
	}
	limit.Release(limiter)
}
