package observationtest

import (
	"context"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

type casPanicStore struct{ graph.Store[int] }

func (s casPanicStore) CompareAndSwap(ctx context.Context, before uint64, cp graph.Checkpoint[int]) error {
	if cp.Revision == 3 {
		panic("CAS panic")
	}
	return s.Store.CompareAndSwap(ctx, before, cp)
}

func TestStorePanicDrainsWorkersAndReleasesBudgetWithoutFakeResolution(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "a", "b"), nil
	})
	bStarted := make(chan struct{})
	node(t, g, "a", func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		select {
		case <-bStarted:
		case <-ctx.Done():
		}
		return graph.EndBranch[int](), nil
	})
	node(t, g, "b", func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		close(bStarted)
		<-ctx.Done()
		return graph.EndBranch[int](), ctx.Err()
	})
	for _, name := range []string{"a", "b"} {
		edge(t, g, "fork", name)
	}
	r, log := compile(t, g), &recorder{}
	l, _ := graph.NewLimiter(2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	func() {
		defer func() {
			if got := recover(); got != "CAS panic" {
				t.Fatalf("panic=%v", got)
			}
		}()
		_, _ = r.Start(ctx, "panic", 1, graph.Options[int]{Store: casPanicStore{memoryStore(t)}, Observer: log, Limiter: l, MaxConcurrency: 2})
	}()
	seen := resolved(log.snapshot(), graph.OperationNode)
	if len(seen) != 1 || seen[0].Node != "fork" || seen[0].Outcome != graph.OutcomeCommitted {
		t.Fatalf("fabricated panic resolution=%+v", seen)
	}
	if len(finished(log.snapshot(), graph.OperationNode)) != 3 || len(finished(log.snapshot(), graph.OperationStart)) != 0 {
		t.Fatal("panic did not drain, or public completion was fabricated")
	}
	if _, err := singleRunner(t).Start(ctx, "reuse", 1, graph.Options[int]{Limiter: l}); err != nil {
		t.Fatal("panic leaked budget:", err)
	}
}
