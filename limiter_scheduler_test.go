package graph_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

func TestLimiterCloneFailureReleasesPermit(t *testing.T) {
	l, _ := graph.NewLimiter(1)
	var copies int
	g := limiterGraph(t, "node", func(_ context.Context, _ graph.CallInfo, s int) (int, error) {
		t.Error("callback ran after clone failed")
		return s, nil
	})
	r, err := g.Compile(graph.Config[int]{MachineID: "clone-limit", Clone: func(s int) (int, error) {
		copies++
		if copies == 2 {
			return 0, errors.New("clone failed")
		}
		return s, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, err := r.Start(ctx, "clone", 1, graph.Options[int]{Limiter: l})
	if err == nil || out.Checkpoint.Revision != 1 || out.Checkpoint.Steps != 0 || out.Checkpoint.Completed {
		t.Fatalf("clone=%+v %v", out, err)
	}
	healthy := limiterFixture(t, "node", func(_ context.Context, _ graph.CallInfo, s int) (int, error) { return s, nil })
	if _, err := healthy.Start(ctx, "reuse", 1, graph.Options[int]{Limiter: l}); err != nil {
		t.Fatal("permit leaked:", err)
	}
}

func TestLimiterWaitingJoinCancellationDoesNotFailExecution(t *testing.T) {
	l, _ := graph.NewLimiter(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	holderCtx, release := context.WithCancel(context.Background())
	defer release()
	holderStarted := make(chan struct{})
	holderDone := make(chan error, 1)
	holder := limiterFixture(t, "node", func(ctx context.Context, _ graph.CallInfo, s int) (int, error) {
		close(holderStarted)
		<-ctx.Done()
		return s, ctx.Err()
	})
	var merged atomic.Bool
	g := limiterGraph(t, "join", func(_ context.Context, _ graph.CallInfo, s int) (int, error) { merged.Store(true); return s, nil })
	// Branch states are 1; input cloning during join receives committed branch
	// values. Initial and scheduling clones are counted to gate the first join
	// input clone, after both node callbacks have released their permits.
	var clones int
	r, err := g.Compile(graph.Config[int]{MachineID: "join-limit", Clone: func(s int) (int, error) {
		clones++
		if clones == 7 {
			go func() {
				_, err := holder.Start(holderCtx, "holder", 0, graph.Options[int]{Limiter: l})
				holderDone <- err
			}()
			<-holderStarted
			cancel()
		}
		return s, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := r.Start(ctx, "join", 1, graph.Options[int]{Limiter: l, MaxConcurrency: 1})
	if !errors.Is(err, context.Canceled) || out.Status != graph.StatusCancelled || merged.Load() || out.Checkpoint.Completed || out.Checkpoint.Failure != nil || out.Checkpoint.Revision != 3 || out.Checkpoint.Steps != 2 {
		t.Fatalf("join=%+v %v merged=%v clones=%d", out, err, merged.Load(), clones)
	}
	release()
	if !errors.Is(<-holderDone, context.Canceled) {
		t.Fatal("holder cancellation")
	}
}

func TestSharedLimiterWithDifferentStateTypes(t *testing.T) {
	l, _ := graph.NewLimiter(1)
	r := limiterFixture(t, "node", func(_ context.Context, _ graph.CallInfo, s int) (int, error) { return s, nil })
	g := graph.New[string]("work")
	if err := g.AddNode(graph.NodeSpec[string]{Name: "work", Run: func(_ context.Context, _ graph.CallInfo, s string) (graph.Transition[string], error) {
		return graph.EndExecution(s + "!"), nil
	}}); err != nil {
		t.Fatal(err)
	}
	strings, err := g.Compile(graph.Config[string]{MachineID: "string-limit", Clone: graph.ValueClone[string]})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := r.Start(ctx, "integer", 1, graph.Options[int]{Limiter: l}); err != nil {
		t.Fatal(err)
	}
	out, err := strings.Start(ctx, "string", "hello", graph.Options[string]{Limiter: l})
	if err != nil || out.Checkpoint.Final == nil || *out.Checkpoint.Final != "hello!" {
		t.Fatalf("different state=%+v %v", out, err)
	}
}
