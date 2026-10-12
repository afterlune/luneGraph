package graph_test

import (
	"context"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type dynamicBenchState struct{ Value int }

func BenchmarkDynamicFanOut(b *testing.B) {
	for _, width := range []int{0, 1, 2, 8, 32, 128} {
		for _, concurrency := range []struct {
			name  string
			value int
		}{{"serial", 1}, {"default", 0}} {
			b.Run("width="+strconv.Itoa(width)+"/concurrency="+concurrency.name, func(b *testing.B) {
				runner := dynamicFanOutBenchRunner(b, width)
				options := graph.Options{MaxSteps: width + 3, MaxConcurrency: concurrency.value}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					_, result, err := runner.Start(context.Background(), "dynamic-bench", dynamicBenchState{}, nil, options)
					if err != nil || result.Status != graph.StatusCompleted || result.Final == nil || result.Final.Value != width*(width+1)/2 {
						b.Fatalf("dynamic result = %+v, %v", result, err)
					}
				}
			})
		}
	}
}

func dynamicFanOutBenchRunner(b *testing.B, width int) *graph.Runner[dynamicBenchState] {
	b.Helper()
	g := graph.New[dynamicBenchState]("dispatch")
	if err := g.AddNode(graph.NodeSpec[dynamicBenchState]{Name: "dispatch", Run: func(_ context.Context, _ graph.CallInfo, parent dynamicBenchState) (graph.Transition[dynamicBenchState], error) {
		branches := make([]graph.Branch[dynamicBenchState], width)
		for i := range branches {
			branches[i] = graph.Branch[dynamicBenchState]{Target: "worker", State: dynamicBenchState{Value: i + 1}}
		}
		return graph.FanOut(parent, branches...), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddNode(graph.NodeSpec[dynamicBenchState]{Name: "worker", Run: func(_ context.Context, _ graph.CallInfo, s dynamicBenchState) (graph.Transition[dynamicBenchState], error) {
		return graph.CompleteBranch(s), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddJoin(graph.JoinSpec[dynamicBenchState]{Name: "joined", From: "dispatch", Merge: func(_ context.Context, _ graph.CallInfo, parent dynamicBenchState, branches []dynamicBenchState) (dynamicBenchState, error) {
		for _, branch := range branches {
			parent.Value += branch.Value
		}
		return parent, nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddNode(graph.NodeSpec[dynamicBenchState]{Name: "done", Run: func(_ context.Context, _ graph.CallInfo, s dynamicBenchState) (graph.Transition[dynamicBenchState], error) {
		return graph.EndExecution(s), nil
	}}); err != nil {
		b.Fatal(err)
	}
	for _, target := range []string{"worker"} {
		if err := g.AddEdge("dispatch", target); err != nil {
			b.Fatal(err)
		}
	}
	if err := g.AddEdge("joined", "done"); err != nil {
		b.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[dynamicBenchState]{MachineID: "dynamic-fanout-benchmark-v1", Clone: graph.ValueClone[dynamicBenchState]})
	if err != nil {
		b.Fatal(err)
	}
	return runner
}
