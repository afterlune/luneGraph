package graph_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/memory"
	"lune-graph/checkpoint/sqlite"
)

type durableBenchmarkState struct {
	Values map[string]int
}

func BenchmarkDurableSequentialExecution(b *testing.B) {
	for _, persistence := range []string{"memory", "sqlite"} {
		for _, stepsPerCall := range []int{1, 16} {
			b.Run("storage="+persistence+"/steps="+strconv.Itoa(stepsPerCall), func(b *testing.B) {
				ctx := context.Background()
				store := durableBenchmarkStore(b, persistence)
				runner := durableSequentialRunner(b)
				const runID = "durable-sequential"
				seed, err := runner.Start(ctx, runID, newDurableBenchmarkState(), graph.Options[durableBenchmarkState]{Store: store, MaxSteps: 1})
				if err != nil || seed.Status != graph.StatusBudget || seed.Checkpoint.Revision != 2 || seed.Checkpoint.Steps != 1 {
					b.Fatalf("seed run = status %s, revision %d, steps %d, error %v", seed.Status, seed.Checkpoint.Revision, seed.Checkpoint.Steps, err)
				}

				options := graph.Options[durableBenchmarkState]{Store: store, MaxSteps: stepsPerCall}
				b.ReportAllocs()
				b.ResetTimer()
				var latest graph.Result[durableBenchmarkState]
				for i := 0; i < b.N; i++ {
					latest, err = runner.Recover(ctx, runID, nil, options)
					expectedProgress := uint64(1 + (i+1)*stepsPerCall)
					if err != nil || latest.Status != graph.StatusBudget || latest.Checkpoint.Steps != expectedProgress || latest.Checkpoint.Revision != expectedProgress+1 || len(latest.Checkpoint.Invocations) != 1 || latest.Checkpoint.Invocations[0].Status != graph.InvocationReady || len(latest.Checkpoint.Invocations[0].State.Values) != 16 || latest.Checkpoint.Invocations[0].State.Values["value-0"] != int(expectedProgress) {
						b.Fatalf("recover %d = status %s, revision %d, steps %d, error %v", i, latest.Status, latest.Checkpoint.Revision, latest.Checkpoint.Steps, err)
					}
				}
				b.StopTimer()
				assertDurableBenchmarkStored(b, store, latest.Checkpoint, 1)
			})
		}
	}
}

func BenchmarkDurableFanoutJoin(b *testing.B) {
	for _, persistence := range []string{"memory", "sqlite"} {
		for _, width := range []int{2, 8, 32} {
			for _, concurrency := range []struct {
				name  string
				value int
			}{{"serial", 1}, {"default", 0}} {
				b.Run("storage="+persistence+"/width="+strconv.Itoa(width)+"/concurrency="+concurrency.name, func(b *testing.B) {
					ctx := context.Background()
					store := durableBenchmarkStore(b, persistence)
					runner := durableFanoutRunner(b, width)
					const runID = "durable-fanout"
					seed, err := runner.Start(ctx, runID, newDurableBenchmarkState(), graph.Options[durableBenchmarkState]{Store: store, MaxSteps: 1, MaxConcurrency: concurrency.value})
					if err != nil || seed.Status != graph.StatusBudget || seed.Checkpoint.Revision != 2 || seed.Checkpoint.Steps != 1 || len(seed.Checkpoint.Groups) != 1 || len(seed.Checkpoint.Invocations) != width+1 {
						b.Fatalf("seed run = status %s, revision %d, steps %d, groups %d, invocations %d, error %v", seed.Status, seed.Checkpoint.Revision, seed.Checkpoint.Steps, len(seed.Checkpoint.Groups), len(seed.Checkpoint.Invocations), err)
					}
					assertDurableFanoutCheckpoint(b, seed.Checkpoint, width, 0)

					stepsPerCall := width + 2
					options := graph.Options[durableBenchmarkState]{Store: store, MaxSteps: stepsPerCall, MaxConcurrency: concurrency.value}
					b.ReportAllocs()
					b.ResetTimer()
					var latest graph.Result[durableBenchmarkState]
					for i := 0; i < b.N; i++ {
						latest, err = runner.Recover(ctx, runID, nil, options)
						cycles := i + 1
						expectedProgress := uint64(1 + cycles*stepsPerCall)
						if err != nil || latest.Status != graph.StatusBudget || latest.Checkpoint.Steps != expectedProgress || latest.Checkpoint.Revision != expectedProgress+1 {
							b.Fatalf("recover %d = status %s, revision %d, steps %d, groups %d, invocations %d, error %v", i, latest.Status, latest.Checkpoint.Revision, latest.Checkpoint.Steps, len(latest.Checkpoint.Groups), len(latest.Checkpoint.Invocations), err)
						}
						assertDurableFanoutCheckpoint(b, latest.Checkpoint, width, cycles)
					}
					b.StopTimer()
					assertDurableBenchmarkStored(b, store, latest.Checkpoint, width+1)
				})
			}
		}
	}
}

func durableBenchmarkStore(b *testing.B, persistence string) graph.Store[durableBenchmarkState] {
	b.Helper()
	switch persistence {
	case "memory":
		store, err := memory.New[durableBenchmarkState](cloneDurableBenchmarkState)
		if err != nil {
			b.Fatal(err)
		}
		return store
	case "sqlite":
		store, err := sqlite.Open(context.Background(), filepath.Join(b.TempDir(), "runs.db"), checkpoint.JSON[durableBenchmarkState]{})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if err := store.Close(); err != nil {
				b.Errorf("close SQLite benchmark store: %v", err)
			}
		})
		return store
	default:
		b.Fatalf("unknown benchmark persistence %q", persistence)
		return nil
	}
}

func durableSequentialRunner(b *testing.B) *graph.Runner[durableBenchmarkState] {
	b.Helper()
	g := graph.New[durableBenchmarkState]("step")
	if err := g.AddNode(graph.NodeSpec[durableBenchmarkState]{Name: "step", Run: func(_ context.Context, _ graph.CallInfo, state durableBenchmarkState) (graph.Transition[durableBenchmarkState], error) {
		state.Values["value-0"]++
		return graph.To(state, "step"), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddEdge("step", "step"); err != nil {
		b.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[durableBenchmarkState]{MachineID: "durable-sequential-v1", Clone: cloneDurableBenchmarkState})
	if err != nil {
		b.Fatal(err)
	}
	return runner
}

func durableFanoutRunner(b *testing.B, width int) *graph.Runner[durableBenchmarkState] {
	b.Helper()
	g := graph.New[durableBenchmarkState]("fork")
	targets := make([]string, width)
	if err := g.AddNode(graph.NodeSpec[durableBenchmarkState]{Name: "fork", Run: func(_ context.Context, _ graph.CallInfo, state durableBenchmarkState) (graph.Transition[durableBenchmarkState], error) {
		return graph.To(state, targets...), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddJoin(graph.JoinSpec[durableBenchmarkState]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, states []durableBenchmarkState) (durableBenchmarkState, error) {
		merged, err := cloneDurableBenchmarkState(states[0])
		if err != nil {
			return durableBenchmarkState{}, err
		}
		merged.Values["joins"]++
		return merged, nil
	}}); err != nil {
		b.Fatal(err)
	}
	for i := range width {
		name := "branch-" + strconv.Itoa(i)
		targets[i] = name
		if err := g.AddNode(graph.NodeSpec[durableBenchmarkState]{Name: name, Run: func(_ context.Context, _ graph.CallInfo, state durableBenchmarkState) (graph.Transition[durableBenchmarkState], error) {
			state.Values["value-0"]++
			return graph.To(state, "join"), nil
		}}); err != nil {
			b.Fatal(err)
		}
		if err := g.AddEdge("fork", name); err != nil {
			b.Fatal(err)
		}
		if err := g.AddEdge(name, "join"); err != nil {
			b.Fatal(err)
		}
	}
	if err := g.AddNode(graph.NodeSpec[durableBenchmarkState]{Name: "repeat", Run: func(_ context.Context, _ graph.CallInfo, state durableBenchmarkState) (graph.Transition[durableBenchmarkState], error) {
		return graph.To(state, "fork"), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddEdge("join", "repeat"); err != nil {
		b.Fatal(err)
	}
	if err := g.AddEdge("repeat", "fork"); err != nil {
		b.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[durableBenchmarkState]{MachineID: "durable-fanout-v1", Clone: cloneDurableBenchmarkState})
	if err != nil {
		b.Fatal(err)
	}
	return runner
}

func newDurableBenchmarkState() durableBenchmarkState {
	values := make(map[string]int, 16)
	for i := range 15 {
		values["value-"+strconv.Itoa(i)] = i
	}
	values["joins"] = 0
	return durableBenchmarkState{Values: values}
}

func cloneDurableBenchmarkState(state durableBenchmarkState) (durableBenchmarkState, error) {
	values := make(map[string]int, len(state.Values))
	for key, value := range state.Values {
		values[key] = value
	}
	return durableBenchmarkState{Values: values}, nil
}

func assertDurableFanoutCheckpoint(b *testing.B, checkpoint graph.Checkpoint[durableBenchmarkState], width, cycles int) {
	b.Helper()
	if len(checkpoint.Groups) != 1 {
		b.Fatalf("fanout checkpoint has %d groups; want 1", len(checkpoint.Groups))
	}
	if len(checkpoint.Invocations) != width+1 || len(checkpoint.Groups[0].Children) != width {
		b.Fatalf("fanout checkpoint has %d invocations and %d children; want %d and %d", len(checkpoint.Invocations), len(checkpoint.Groups[0].Children), width+1, width)
	}
	group := checkpoint.Groups[0]
	children := make(map[string]struct{}, width)
	for _, id := range group.Children {
		children[id] = struct{}{}
	}
	parentFound := false
	childrenFound := 0
	for _, invocation := range checkpoint.Invocations {
		if invocation.Status == graph.InvocationGroup {
			if parentFound || invocation.ID != group.ParentID || invocation.ChildGroupID != group.ID || len(invocation.State.Values) != 16 || invocation.State.Values["value-0"] != cycles || invocation.State.Values["joins"] != cycles {
				b.Fatalf("invalid fanout parent invocation: %+v", invocation)
			}
			parentFound = true
			continue
		}
		if _, ok := children[invocation.ID]; !ok || invocation.Status != graph.InvocationReady || invocation.GroupID != group.ID || len(invocation.State.Values) != 16 || invocation.State.Values["value-0"] != cycles || invocation.State.Values["joins"] != cycles {
			b.Fatalf("invalid fanout child invocation: %+v", invocation)
		}
		delete(children, invocation.ID)
		childrenFound++
	}
	if !parentFound || childrenFound != width || len(children) != 0 {
		b.Fatalf("fanout checkpoint has parent=%t, %d ready children, and %d unmatched group children; want parent and %d children", parentFound, childrenFound, len(children), width)
	}
}

func assertDurableBenchmarkStored(b *testing.B, store graph.Store[durableBenchmarkState], expected graph.Checkpoint[durableBenchmarkState], invocationCount int) {
	b.Helper()
	stored, err := store.Load(context.Background(), expected.RunID)
	if err != nil || stored.Revision != expected.Revision || stored.Steps != expected.Steps || stored.Completed || len(stored.Groups) != len(expected.Groups) || len(stored.Invocations) != invocationCount {
		b.Fatalf("stored checkpoint = revision %d, steps %d, groups %d, invocations %d, error %v", stored.Revision, stored.Steps, len(stored.Groups), len(stored.Invocations), err)
	}
	for i := range expected.Invocations {
		if stored.Invocations[i].ID != expected.Invocations[i].ID || stored.Invocations[i].Node != expected.Invocations[i].Node || stored.Invocations[i].Status != expected.Invocations[i].Status || len(stored.Invocations[i].State.Values) != len(expected.Invocations[i].State.Values) {
			b.Fatalf("stored invocation %d differs from result: got %+v, want %+v", i, stored.Invocations[i], expected.Invocations[i])
		}
		for key, value := range expected.Invocations[i].State.Values {
			if storedValue, ok := stored.Invocations[i].State.Values[key]; !ok || storedValue != value {
				b.Fatalf("stored invocation %d state %q = %d, want %d", i, key, stored.Invocations[i].State.Values[key], value)
			}
		}
	}
}
