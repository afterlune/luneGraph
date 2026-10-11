package capacitytest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

const singleRunTickNode = "tick"

func singleRunCheckpoint(t testing.TB, waiters int) (*graph.Runner[state], graph.Checkpoint[state], string) {
	t.Helper()
	if waiters < 1 {
		t.Fatal("waiter count must be positive")
	}
	ctx := context.Background()
	waiterNames := make([]string, waiters)
	targets := make([]string, 0, waiters+1)
	for i := range waiters {
		waiterNames[i] = fmt.Sprintf("wait-%04d", i)
		targets = append(targets, waiterNames[i])
	}
	targets = append(targets, singleRunTickNode)

	g := graph.New[state]("root")
	addNode(t, g, "root", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
		return graph.To(s, targets...), nil
	})
	addNode(t, g, "done", func(_ context.Context, _ graph.CallInfo, _ state) (graph.Transition[state], error) {
		return graph.EndBranch[state](), nil
	})
	for _, name := range waiterNames {
		addNode(t, g, name, func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
			return graph.Wait(s, "advance", "done"), nil
		})
		addEdge(t, g, "root", name)
		addEdge(t, g, name, "done")
	}
	addNode(t, g, singleRunTickNode, func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
		if err := checkIdentity(call, s); err != nil {
			return graph.Transition[state]{}, err
		}
		s.Total++
		s.Values["steps"]++
		return graph.To(s, singleRunTickNode), nil
	})
	addEdge(t, g, "root", singleRunTickNode)
	addEdge(t, g, singleRunTickNode, singleRunTickNode)
	continuation(t, g)
	runner := compile(t, g)

	seed, err := runner.Start(ctx, "single-run", initial("single-run"), graph.Options[state]{MaxSteps: 1, MaxConcurrency: 1})
	if err != nil || seed.Status != graph.StatusBudget || seed.Checkpoint.Steps != 1 {
		t.Fatalf("fan-out seed=%+v err=%v", seed, err)
	}
	paused, err := runner.Resume(ctx, seed.Checkpoint, nil, graph.Options[state]{MaxSteps: waiters, MaxConcurrency: 1})
	if err != nil || paused.Status != graph.StatusBudget || paused.Checkpoint.Steps != uint64(waiters+1) {
		t.Fatalf("waiting population=%+v err=%v", paused, err)
	}

	position, err := paused.Checkpoint.Clone(clone)
	if err != nil {
		t.Fatal(err)
	}
	// Create requires revision 1. This active position was produced by the
	// public Runner and uses the same independent-run numbering convention as Fork.
	position.Revision = 1
	assertSingleRunCheckpoint(t, position, waiters, waiters+1, 0)

	tickID := ""
	for _, inv := range position.Invocations {
		if inv.Node == singleRunTickNode {
			tickID = inv.ID
		}
	}
	if tickID == "" {
		t.Fatal("seed position has no ready tick invocation")
	}
	return runner, position, tickID
}

func assertSingleRunCheckpoint(t testing.TB, cp graph.Checkpoint[state], waiters int, steps, ticks int) {
	t.Helper()
	if cp.FormatVersion != graph.CheckpointFormatVersion || cp.RunID != "single-run" || cp.MachineID != "capacity-v1" || cp.Completed || cp.Failure != nil || cp.Revision == 0 || cp.Steps != uint64(steps) || len(cp.Groups) != 1 || len(cp.Invocations) != waiters+2 {
		t.Fatalf("invalid single-Run header or topology: revision=%d steps=%d groups=%d invocations=%d", cp.Revision, cp.Steps, len(cp.Groups), len(cp.Invocations))
	}
	group := cp.Groups[0]
	if group.JoinNode != "" || len(group.Children) != waiters+1 || group.ParentID == "" {
		t.Fatalf("unexpected activation group: %+v", group)
	}
	waiting := 0
	parents := 0
	readyTicks := 0
	for _, inv := range cp.Invocations {
		if inv.Status == graph.InvocationGroup && inv.ID == group.ParentID && inv.ChildGroupID == group.ID {
			parents++
		}
		if inv.Status == graph.InvocationWaiting {
			if !strings.HasPrefix(inv.Node, "wait-") || inv.CallID == "" || inv.GroupID != group.ID || inv.Continuation != "advance" || len(inv.Next) != 1 || inv.Next[0] != "done" || inv.State.Owner != "single-run" {
				t.Fatalf("invalid waiting invocation: %+v", inv)
			}
			waiting++
		}
		if inv.Node == singleRunTickNode {
			if inv.Status != graph.InvocationReady || inv.CallID == "" || inv.State.Owner != "single-run" || inv.State.Total != ticks || inv.State.Values["steps"] != ticks {
				t.Fatalf("invalid ready tick: %+v want ticks=%d", inv, ticks)
			}
			readyTicks++
		}
	}
	if waiting != waiters || parents != 1 || readyTicks != 1 {
		t.Fatalf("waiting=%d/%d parents=%d readyTicks=%d", waiting, waiters, parents, readyTicks)
	}
}

func TestSingleRunCheckpointFixture(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			const waiters = 32
			runner, position, tickID := singleRunCheckpoint(t, waiters)
			store := openStore(t, kind)
			ctx := context.Background()
			if err := store.Create(ctx, position); err != nil {
				t.Fatal(err)
			}
			loaded, err := store.Load(ctx, position.RunID)
			if err != nil {
				t.Fatal(err)
			}
			assertSingleRunCheckpoint(t, loaded, waiters, waiters+1, 0)

			out, err := runner.Recover(ctx, position.RunID, nil, graph.Options[state]{Store: store, MaxSteps: 1, MaxConcurrency: 1})
			if err != nil || out.Status != graph.StatusBudget {
				t.Fatalf("one-step recovery=%+v err=%v", out, err)
			}
			assertSingleRunCheckpoint(t, out.Checkpoint, waiters, waiters+2, 1)
			foundTick := false
			for _, inv := range out.Checkpoint.Invocations {
				if inv.ID == tickID && inv.Status == graph.InvocationReady {
					foundTick = true
				}
			}
			if !foundTick {
				t.Fatalf("ready tick %q was not preserved", tickID)
			}
			saved, err := store.Load(ctx, position.RunID)
			if err != nil || saved.Revision != out.Checkpoint.Revision || saved.Steps != out.Checkpoint.Steps {
				t.Fatalf("recovered position was not committed: %+v err=%v", saved, err)
			}
		})
	}
}

func BenchmarkSingleRunCheckpoint(b *testing.B) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, waiters := range []int{32, 256, 2048} {
			for _, mode := range []string{"load", "recover"} {
				b.Run(fmt.Sprintf("storage=%s/waiters=%d/mode=%s", kind, waiters, mode), func(b *testing.B) {
					runner, position, tickID := singleRunCheckpoint(b, waiters)
					store := openStore(b, kind)
					ctx := context.Background()
					if err := store.Create(ctx, position); err != nil {
						b.Fatal(err)
					}
					options := graph.Options[state]{Store: store, MaxSteps: 1, MaxConcurrency: 1}
					var progress int
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var current graph.Checkpoint[state]
						var err error
						if mode == "load" {
							current, err = store.Load(ctx, position.RunID)
							progress = 0
							if err == nil {
								if inv, ok := singleRunTick(current, tickID); ok {
									progress = inv.State.Total
								}
							}
						} else {
							var out graph.Result[state]
							out, err = runner.Recover(ctx, position.RunID, nil, options)
							current = out.Checkpoint
							progress = i + 1
							if err == nil && (out.Status != graph.StatusBudget || current.Revision != uint64(i+2) || current.Steps != uint64(waiters+1+i+1)) {
								b.Fatalf("recover %d: status=%s revision=%d steps=%d", i, out.Status, current.Revision, current.Steps)
							}
						}
						if err != nil {
							b.Fatal(err)
						}
						if current.RunID != position.RunID || current.Revision == 0 || current.Steps != uint64(waiters+1+progress) || len(current.Invocations) != waiters+2 || len(current.Groups) != 1 {
							b.Fatalf("invalid %s position at op %d: revision=%d steps=%d invocations=%d groups=%d", mode, i, current.Revision, current.Steps, len(current.Invocations), len(current.Groups))
						}
						inv, ok := singleRunTick(current, tickID)
						if !ok || inv.Status != graph.InvocationReady || inv.State.Total != progress {
							b.Fatalf("tick position at op %d: %+v found=%t progress=%d", i, inv, ok, progress)
						}
					}
					b.StopTimer()
					final, err := store.Load(ctx, position.RunID)
					if err != nil {
						b.Fatal(err)
					}
					assertSingleRunCheckpoint(b, final, waiters, waiters+1+progress, progress)
				})
			}
		}
	}

}

func singleRunTick(cp graph.Checkpoint[state], id string) (graph.Invocation[state], bool) {
	index := sort.Search(len(cp.Invocations), func(i int) bool { return cp.Invocations[i].ID >= id })
	if index == len(cp.Invocations) || cp.Invocations[index].ID != id {
		return graph.Invocation[state]{}, false
	}
	return cp.Invocations[index], true
}
