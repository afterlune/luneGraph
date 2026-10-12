package observationtest

import (
	"context"
	"sync"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/internal/limit"
)

// A synchronous Merge must not wait for work that its own scheduler has yet
// to dispatch. The deadline is only a fuse; cancellation below is explicit.
func TestLimiterIndependentBranchProgressDuringJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	store := memoryStore(t)
	limiter, err := graph.NewLimiter(4)
	if err != nil {
		t.Fatal(err)
	}
	f := &failureFixture{log: &recorder{}, limiter: limiter}
	joinEntered, deepFinished, allowDeep := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseDeep := closeOnce(allowDeep)
	markDeepFinished := closeOnce(deepFinished)
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); releaseDeep(); workers.Wait() })
	observer := graph.ObserverFunc(func(ctx context.Context, event graph.Event) {
		f.log.Observe(ctx, event)
		if event.Node == "deep" && event.Phase == graph.PhaseFinished {
			markDeepFinished()
		}
	})
	g := graph.New[int]("root")
	node(t, g, "root", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "deep", "nested"), nil
	})
	node(t, g, "deep", func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		select {
		case <-allowDeep:
			return graph.To(s, "descendant"), nil
		case <-ctx.Done():
			return graph.Transition[int]{}, ctx.Err()
		}
	})
	node(t, g, "nested", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "a", "b"), nil
	})
	descendantStarted := make(chan struct{})
	node(t, g, "descendant", func(_ context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		close(descendantStarted)
		return graph.EndBranch[int](), nil
	})
	for _, name := range []string{"a", "b"} {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.To(s, "join"), nil
		})
	}
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "nested", Merge: func(ctx context.Context, _ graph.CallInfo, _ int, _ []graph.BranchResult[int]) (int, error) {
		close(joinEntered)
		select {
		case <-descendantStarted:
			return 0, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}}); err != nil {
		t.Fatal(err)
	}
	for _, e := range [][2]string{{"root", "deep"}, {"root", "nested"}, {"deep", "descendant"}, {"nested", "a"}, {"nested", "b"}, {"a", "join"}, {"b", "join"}} {
		edge(t, g, e[0], e[1])
	}
	runner := compile(t, g)
	done := make(chan struct{})
	var result graph.Result[int]
	var runErr error
	workers.Add(1)
	go func() {
		defer workers.Done()
		_, result, runErr = runner.Start(ctx, "dependency", 0, store, graph.Options{Limiter: limiter, MaxConcurrency: 4, Observer: observer})
		close(done)
	}()
	f.wait(t, ctx, joinEntered, "join entered; deep not dispatched beyond its gate")
	releaseDeep()
	f.wait(t, ctx, deepFinished, "deep finished; result waiting for synchronous join")
	f.wait(t, ctx, descendantStarted, "independent successor dispatched during Merge")
	f.wait(t, ctx, done, "asynchronous join completion")
	if runErr != nil || result.Status != graph.StatusCompleted || !result.Header.Completed {
		t.Fatalf("result=%+v err=%v", result, runErr)
	}
	saved, err := store.Load(ctx, "dependency")
	if err != nil || !saved.Completed {
		t.Fatalf("saved=%+v err=%v", saved, err)
	}
	checkCall(t, f.log.snapshot(), graph.OperationStart, result, runErr)
	drainCtx, stopDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopDrain()

	acquired := 0
	defer func() {
		for acquired > 0 {
			limit.Release(limiter)
			acquired--
		}
	}()
	for range 4 {
		if err := limit.Acquire(drainCtx, limiter); err != nil {
			t.Fatal("permit leaked:", err)
		}
		acquired++
	}
}
