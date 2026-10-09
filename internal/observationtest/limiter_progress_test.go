package observationtest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/internal/limit"
)

type gatedCommitStore struct {
	graph.Store[int]
	write func(context.Context, uint64, graph.Checkpoint[int]) error
}

func (s gatedCommitStore) CompareAndSwap(ctx context.Context, before uint64, cp graph.Checkpoint[int]) error {
	return s.write(ctx, before, cp)
}

func TestLimiterProcessesReturnedResultsWhileBudgetExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l, _ := graph.NewLimiter(3)
	// Test-owned permits represent callbacks in other executions. A CAS gate
	// fills the remaining slots after our returning workers release theirs,
	// ensuring the scheduler must process queued results without admission.
	if err := limit.Acquire(ctx, l); err != nil {
		t.Fatal(err)
	}
	var held atomic.Int32
	held.Store(1)
	releaseHeld := func() {
		for range held.Swap(0) {
			limit.Release(l)
		}
	}
	defer releaseHeld()
	aStarted := make(chan struct{})
	releaseNodes := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseNodes) }) }
	defer release()
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "a", "b", "c", "d"), nil
	})
	node(t, g, "a", func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		close(aStarted)
		select {
		case <-releaseNodes:
		case <-ctx.Done():
		}
		return graph.EndBranch[int](), nil
	})
	node(t, g, "b", func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		select {
		case <-aStarted:
		case <-ctx.Done():
		}
		return graph.EndBranch[int](), nil
	})
	node(t, g, "c", func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		select {
		case <-releaseNodes:
		case <-ctx.Done():
		}
		return graph.EndBranch[int](), nil
	})
	node(t, g, "d", func(_ context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		return graph.EndBranch[int](), nil
	})
	for _, name := range []string{"a", "b", "c", "d"} {
		edge(t, g, "fork", name)
	}
	r, memory := compile(t, g), memoryStore(t)
	seed, err := r.Start(ctx, "progress", 0, graph.Options[int]{Store: memory, MaxSteps: 1})
	if err != nil {
		t.Fatal(err)
	}
	aCommitted := make(chan struct{})
	var commitOnce sync.Once
	hooked := false
	store := gatedCommitStore{Store: memory, write: func(ctx context.Context, before uint64, cp graph.Checkpoint[int]) error {
		if !hooked {
			hooked = true
			release()
			for range 2 {
				if err := limit.Acquire(ctx, l); err != nil {
					return err
				}
				held.Add(1)
			}
		}
		if err := memory.CompareAndSwap(ctx, before, cp); err != nil {
			return err
		}
		for _, inv := range cp.Invocations {
			if inv.Node == "a" && inv.Status == graph.InvocationEnded {
				commitOnce.Do(func() { close(aCommitted) })
			}
		}
		return nil
	}}
	log := &recorder{}
	type answer struct {
		result graph.Result[int]
		err    error
	}
	done := make(chan answer, 1)
	go func() {
		out, err := r.Resume(ctx, seed.Checkpoint, nil, graph.Options[int]{Store: store, Limiter: l, MaxConcurrency: 4, Observer: log})
		done <- answer{out, err}
	}()
	select {
	case <-aCommitted:
		if held.Load() != 3 {
			t.Fatal("fixture did not exhaust capacity")
		}
		releaseHeld()
	case <-ctx.Done():
		// The public call drains before reporting failure; then fixture-owned
		// permits can be released without racing the gated writer.
		got := <-done
		t.Fatalf("queued callback result blocked behind capacity: %v", got.err)
	}
	got := <-done
	if got.err != nil || !got.result.Checkpoint.Completed || got.result.Checkpoint.Steps != 5 {
		t.Fatalf("progress=%+v %v", got.result, got.err)
	}
	checkCall(t, log.snapshot(), graph.OperationResume, got.result, got.err)
}
