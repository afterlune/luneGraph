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

func singleRunCheckpoint(t testing.TB, waiters int, clones ...graph.Clone[state]) (*graph.Runner[state], graph.Checkpoint[state], string) {
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
	copyState := graph.Clone[state](clone)
	if len(clones) > 0 && clones[0] != nil {
		copyState = clones[0]
	}
	runner, err := g.Compile(graph.Config[state]{MachineID: "capacity-v1", Clone: copyState})
	if err != nil {
		t.Fatal(err)
	}
	seedExecution, seed, err := runner.Start(ctx, "single-run", initial("single-run"), nil, graph.Options{MaxSteps: 1, MaxConcurrency: 1})
	if err != nil || seed.Status != graph.StatusBudget || seed.Header.Steps != 1 {
		t.Fatalf("fan-out seed=%+v err=%v", seed, err)
	}
	pausedExecution, paused, err := runner.Resume(ctx, mustCheckpoint(seedExecution), nil, nil, graph.Options{MaxSteps: waiters, MaxConcurrency: 1})
	if err != nil || paused.Status != graph.StatusBudget || paused.Header.Steps != uint64(waiters+1) {
		t.Fatalf("waiting population=%+v err=%v", paused, err)
	}

	position, err := mustCheckpoint(pausedExecution).Clone(clone)
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
			outExecution, out, err := runner.Recover(ctx, position.RunID, nil, store, graph.Options{MaxSteps: 1, MaxConcurrency: 1})
			if err != nil || out.Status != graph.StatusBudget {
				t.Fatalf("one-step recovery=%+v err=%v", out, err)
			}
			assertSingleRunCheckpoint(t, mustCheckpoint(outExecution), waiters, waiters+2, 1)
			foundTick := false
			for _, inv := range mustCheckpoint(outExecution).Invocations {
				if inv.ID == tickID && inv.Status == graph.InvocationReady {
					foundTick = true
				}
			}
			if !foundTick {
				t.Fatalf("ready tick %q was not preserved", tickID)
			}
			saved, err := store.Load(ctx, position.RunID)
			if err != nil || saved.Revision != out.Header.Revision || saved.Steps != out.Header.Steps {
				t.Fatalf("recovered position was not committed: %+v err=%v", saved, err)
			}
		})
	}
}

type countingSingleRunStore struct {
	graph.Store[state]
	loads       int
	headerLoads int
}

func (s *countingSingleRunStore) Load(ctx context.Context, id string) (graph.Checkpoint[state], error) {
	s.loads++
	return s.Store.Load(ctx, id)
}

func (s *countingSingleRunStore) LoadHeader(ctx context.Context, id string) (graph.Header, error) {
	s.headerLoads++
	return s.Store.LoadHeader(ctx, id)
}

func TestWarmAdvanceAvoidsFullLoadAndUnchangedStateClone(t *testing.T) {
	const waiters = 32
	clones := 0
	runner, position, tickID := singleRunCheckpoint(t, waiters, func(s state) (state, error) {
		clones++
		return clone(s)
	})
	clones = 0
	base := openStore(t, "memory")
	store := &countingSingleRunStore{Store: base}
	ctx := context.Background()
	if err := store.Create(ctx, position); err != nil {
		t.Fatal(err)
	}
	opts := graph.Options{MaxSteps: 1, MaxConcurrency: 1}
	execution, out, err := runner.Recover(ctx, position.RunID, nil, store, opts)
	if err != nil || out.Status != graph.StatusBudget || store.loads != 1 || store.headerLoads != 0 {
		t.Fatalf("cold recovery=%+v err=%v full-loads=%d header-loads=%d", out, err, store.loads, store.headerLoads)
	}
	clones = 0
	for i := 0; i < 3; i++ {
		out, err = execution.Advance(ctx, nil, opts)
		if err != nil || out.Status != graph.StatusBudget || out.Header.Steps != uint64(waiters+3+i) {
			t.Fatalf("warm advance %d=%+v err=%v", i, out, err)
		}
	}
	if store.loads != 1 || store.headerLoads != 3 || clones != 3 {
		t.Fatalf("warm advances reloaded or cloned unrelated states: full-loads=%d header-loads=%d clones=%d", store.loads, store.headerLoads, clones)
	}
	pending := execution.Pending()
	if len(pending) != waiters {
		t.Fatalf("pending continuations=%d want=%d", len(pending), waiters)
	}
	for i, item := range pending {
		if (i > 0 && pending[i-1].InvocationID >= item.InvocationID) || item.CallID == "" || item.Continuation != "advance" {
			t.Fatalf("pending[%d]=%+v", i, item)
		}
	}
	snapshot, err := execution.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	assertSingleRunCheckpoint(t, snapshot, waiters, waiters+5, 4)
	if clones != waiters+5 {
		t.Fatalf("explicit checkpoint clones=%d want %d", clones, waiters+5)
	}
	for i := range snapshot.Invocations {
		if snapshot.Invocations[i].ID == tickID {
			snapshot.Invocations[i].State.Values["steps"] = -1
		}
	}
	independent, err := execution.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	tick, ok := singleRunTick(independent, tickID)
	if !ok || tick.State.Values["steps"] != 4 {
		t.Fatalf("checkpoint mutation escaped into live execution: %+v", tick)
	}
}

func BenchmarkSingleRunCheckpoint(b *testing.B) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, waiters := range []int{32, 256, 2048} {
			for _, mode := range []string{"load", "cold-recover", "warm-advance", "checkpoint"} {
				b.Run(fmt.Sprintf("storage=%s/waiters=%d/mode=%s", kind, waiters, mode), func(b *testing.B) {
					runner, position, tickID := singleRunCheckpoint(b, waiters)
					store := openStore(b, kind)
					ctx := context.Background()
					if err := store.Create(ctx, position); err != nil {
						b.Fatal(err)
					}
					options := graph.Options{MaxSteps: 1, MaxConcurrency: 1}
					var execution *graph.Execution[state]
					progress := 0
					if mode == "warm-advance" || mode == "checkpoint" {
						var out graph.Result[state]
						var err error
						execution, out, err = runner.Recover(ctx, position.RunID, nil, store, options)
						if err != nil || out.Status != graph.StatusBudget || out.Header.Revision != 2 || out.Header.Steps != uint64(waiters+2) {
							b.Fatalf("warm seed=%+v err=%v", out, err)
						}
						progress = 1
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var current graph.Checkpoint[state]
						var out graph.Result[state]
						var err error
						switch mode {
						case "load":
							current, err = store.Load(ctx, position.RunID)
							progress = 0
							if err == nil {
								if inv, ok := singleRunTick(current, tickID); ok {
									progress = inv.State.Total
								}
							}
						case "cold-recover":
							_, out, err = runner.Recover(ctx, position.RunID, nil, store, options)
							progress = i + 1
						case "warm-advance":
							out, err = execution.Advance(ctx, nil, options)
							progress = i + 2
						case "checkpoint":
							current, err = execution.Checkpoint()
							progress = 1
						}
						if err != nil {
							b.Fatal(err)
						}
						if mode == "cold-recover" || mode == "warm-advance" {
							if out.Status != graph.StatusBudget || out.Header.Revision != uint64(progress+1) || out.Header.Steps != uint64(waiters+1+progress) {
								b.Fatalf("%s %d: status=%s revision=%d steps=%d", mode, i, out.Status, out.Header.Revision, out.Header.Steps)
							}
						} else if err := validateSingleRunBenchmarkPosition(current, position, waiters, tickID, progress); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					final, err := store.Load(ctx, position.RunID)
					if err != nil {
						b.Fatal(err)
					}
					wantProgress := 0
					if mode == "cold-recover" {
						wantProgress = b.N
					} else if mode == "warm-advance" {
						wantProgress = b.N + 1
					} else if mode == "checkpoint" {
						wantProgress = 1
					}
					assertSingleRunCheckpoint(b, final, waiters, waiters+1+wantProgress, wantProgress)
				})
			}
		}
	}
}

func validateSingleRunBenchmarkPosition(cp, seed graph.Checkpoint[state], waiters int, tickID string, progress int) error {
	if cp.RunID != seed.RunID || cp.Revision == 0 || cp.Steps != uint64(waiters+1+progress) || len(cp.Invocations) != waiters+2 || len(cp.Groups) != 1 {
		return fmt.Errorf("invalid position: revision=%d steps=%d invocations=%d groups=%d", cp.Revision, cp.Steps, len(cp.Invocations), len(cp.Groups))
	}
	inv, ok := singleRunTick(cp, tickID)
	if !ok || inv.Status != graph.InvocationReady || inv.State.Total != progress {
		return fmt.Errorf("invalid tick: %+v found=%t progress=%d", inv, ok, progress)
	}
	return nil
}

func singleRunTick(cp graph.Checkpoint[state], id string) (graph.Invocation[state], bool) {
	index := sort.Search(len(cp.Invocations), func(i int) bool { return cp.Invocations[i].ID >= id })
	if index == len(cp.Invocations) || cp.Invocations[index].ID != id {
		return graph.Invocation[state]{}, false
	}
	return cp.Invocations[index], true
}
