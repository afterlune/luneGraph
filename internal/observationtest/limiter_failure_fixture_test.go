package observationtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/internal/limit"
)

// failureFixture leaves a healthy outer branch joined, with an inner group
// containing a blocked descendant group and a node or join that fails its group.
type failureFixture struct {
	runner           *graph.Runner[int]
	limiter          *graph.Limiter
	store            graph.Store[int]
	log              *recorder
	observer         graph.Observer
	seed             graph.Checkpoint[int]
	ctx              context.Context
	cancel           context.CancelFunc
	cloneReached     chan struct{}
	blockedCancelled chan struct{}
	allowClone       func()
	releaseBlocked   func()
	stopHolder       func()
	holderDone       chan error
	holderCtx        context.Context
	held             atomic.Bool
	joins            atomic.Int32
	target           sync.WaitGroup
}

func closeOnce(ch chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

func waitFailureSignal(t *testing.T, ctx context.Context, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("%s: %v", name, ctx.Err())
	}
}

func newFailureFixture(t *testing.T, origin string, store graph.Store[int]) *failureFixture {
	t.Helper()
	f := &failureFixture{store: store, log: &recorder{}, cloneReached: make(chan struct{}), blockedCancelled: make(chan struct{}), holderDone: make(chan error, 1)}
	f.ctx, f.cancel = context.WithTimeout(context.Background(), 5*time.Second)
	var err error
	f.limiter, err = graph.NewLimiter(4)
	if err != nil {
		t.Fatal(err)
	}
	allowClone, releaseBlocked := make(chan struct{}), make(chan struct{})
	f.allowClone, f.releaseBlocked = closeOnce(allowClone), closeOnce(releaseBlocked)
	holderRelease := make(chan struct{})
	f.stopHolder = closeOnce(holderRelease)
	var cancelHolder context.CancelFunc
	f.holderCtx, cancelHolder = context.WithCancel(context.Background())
	holderLaunched := false
	// Register cleanup before launching either execution or entering a gate.
	t.Cleanup(func() {
		f.cancel()
		f.allowClone()
		f.releaseBlocked()
		f.target.Wait()
		f.releaseHeld()
		cancelHolder()
		f.stopHolder()
		if !holderLaunched {
			return
		}
		select {
		case <-f.holderDone:
		case <-time.After(5 * time.Second):
			t.Error("independent holder did not drain")
		}
	})
	blockedStarted := make(chan struct{})
	aCommitted := make(chan struct{})
	markACommitted := closeOnce(aCommitted)
	f.observer = graph.ObserverFunc(func(ctx context.Context, event graph.Event) {
		f.log.Observe(ctx, event)
		if event.Node == "a" && event.Phase == graph.PhaseResolved && event.Outcome == graph.OutcomeCommitted {
			markACommitted()
		}
	})
	var blockedCalls, blockedCancellations atomic.Int32
	boom := errors.New("nested failure")
	g := graph.New[int]("root")
	node(t, g, "root", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "survivor", "inner"), nil
	})
	node(t, g, "survivor", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s+10, "outerjoin"), nil
	})
	trigger := "failer"
	if origin == "join" {
		trigger = "nested"
	}
	node(t, g, "inner", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "deep", trigger), nil
	})
	node(t, g, "deep", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "blocked-a", "blocked-b"), nil
	})
	blocked := func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		call := blockedCalls.Add(1)
		if call == 2 {
			close(blockedStarted)
		}
		<-ctx.Done()
		if call <= 2 {
			if blockedCancellations.Add(1) == 2 {
				close(f.blockedCancelled)
			}
			<-releaseBlocked
		}
		// A callback that ignores its cancelled result must still be discarded.
		return graph.EndExecution(99), nil
	}
	for _, name := range []string{"blocked-a", "blocked-b"} {
		node(t, g, name, blocked)
		edge(t, g, "deep", name)
	}
	if origin == "node" {
		if err := g.AddNode(graph.NodeSpec[int]{Name: "failer", OnError: graph.FailGroup, Run: func(ctx context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
			select {
			case <-blockedStarted:
			case <-ctx.Done():
				return graph.Transition[int]{}, ctx.Err()
			}
			return graph.Transition[int]{}, boom
		}}); err != nil {
			t.Fatal(err)
		}
	} else {
		node(t, g, "nested", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.To(s, "a", "b"), nil
		})
		for _, name := range []string{"a", "b"} {
			node(t, g, name, func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				if name == "b" {
					// Arrival order is not scheduling order. Make a's commit the
					// explicit prerequisite for b to trigger the nested join.
					select {
					case <-aCommitted:
					case <-ctx.Done():
						return graph.Transition[int]{}, ctx.Err()
					}
				}
				return graph.To(s, "innerjoin"), nil
			})
			edge(t, g, "nested", name)
		}
		if err := g.AddJoin(graph.JoinSpec[int]{Name: "innerjoin", From: "nested", OnError: graph.FailGroup, Merge: func(ctx context.Context, _ graph.CallInfo, _ []int) (int, error) {
			select {
			case <-blockedStarted:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			return 0, boom
		}}); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"a", "b"} {
			edge(t, g, name, "innerjoin")
		}
	}
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "outerjoin", From: "root", Merge: func(_ context.Context, _ graph.CallInfo, values []int) (int, error) {
		f.joins.Add(1)
		if len(values) != 1 || values[0] != 17 {
			return 0, fmt.Errorf("failed branch reached outer join: %v", values)
		}
		return values[0], nil
	}}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.EndExecution(s), nil
	})
	for _, e := range [][2]string{{"root", "survivor"}, {"root", "inner"}, {"survivor", "outerjoin"}, {"inner", "deep"}, {"inner", trigger}, {"outerjoin", "done"}} {
		edge(t, g, e[0], e[1])
	}
	var armed, gated atomic.Bool
	f.runner, err = g.Compile(graph.Config[int]{MachineID: "observation-v1", Clone: func(s int) (int, error) {
		if s == 17 && armed.Load() && gated.CompareAndSwap(false, true) {
			// The holder owns one slot and two descendants retain their slots
			// after cancellation. Take the last slot before outer join admission.
			if err := limit.Acquire(f.ctx, f.limiter); err != nil {
				return 0, err
			}
			f.held.Store(true)
			close(f.cloneReached)
			<-allowClone
		}
		return s, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	seed, err := f.runner.Start(f.ctx, "target", 7, graph.Options[int]{Store: store, MaxSteps: 1, MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		seed, err = f.runner.Recover(f.ctx, "target", nil, graph.Options[int]{Store: store, MaxSteps: 1, MaxConcurrency: 1})
		if err != nil {
			t.Fatal(err)
		}
	}
	if seed.Checkpoint.Steps != 3 || len(seed.Checkpoint.Groups) != 2 {
		t.Fatalf("seed=%+v", seed)
	}
	f.seed = seed.Checkpoint
	armed.Store(true)
	holderStarted := make(chan struct{})
	holderGraph := graph.New[int]("holder")
	node(t, holderGraph, "holder", func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		close(holderStarted)
		select {
		case <-holderRelease:
			return graph.EndExecution(s), nil
		case <-ctx.Done():
			return graph.Transition[int]{}, ctx.Err()
		}
	})
	holder := compile(t, holderGraph)
	holderLaunched = true
	go func() {
		_, err := holder.Start(f.holderCtx, "independent", 0, graph.Options[int]{Store: store, Limiter: f.limiter})
		f.holderDone <- err
		close(f.holderDone)
	}()
	waitFailureSignal(t, f.ctx, holderStarted, "holder did not start")
	return f
}

func (f *failureFixture) goRun(fn func()) {
	f.target.Add(1)
	go func() { defer f.target.Done(); fn() }()
}

func (f *failureFixture) releaseHeld() {
	if f.held.Swap(false) {
		limit.Release(f.limiter)
	}
}
