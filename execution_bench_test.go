package graph_test

import (
	"context"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/memory"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func BenchmarkSequentialExecution(b *testing.B) {
	for _, steps := range []int{1, 16, 128} {
		b.Run("steps="+strconv.Itoa(steps), func(b *testing.B) {
			runner := benchmarkSequentialRunner(b, steps)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := runner.Start(context.Background(), "sequential", 0, graph.Options[int]{MaxSteps: steps + 1})
				if err != nil {
					b.Fatal(err)
				}
				if result.Status != graph.StatusCompleted || result.Checkpoint.Final == nil || *result.Checkpoint.Final != steps || result.Checkpoint.Steps != uint64(steps) {
					b.Fatalf("unexpected result: status=%s final=%v steps=%d", result.Status, result.Checkpoint.Final, result.Checkpoint.Steps)
				}
			}
		})
	}
}

func BenchmarkFanoutJoin(b *testing.B) {
	for _, width := range []int{2, 8, 32} {
		for _, concurrency := range []struct {
			name  string
			value int
		}{{"serial", 1}, {"default", 0}} {
			b.Run("width="+strconv.Itoa(width)+"/concurrency="+concurrency.name, func(b *testing.B) {
				runner := benchmarkFanoutRunner(b, width)
				options := graph.Options[int]{MaxSteps: width + 8, MaxConcurrency: concurrency.value}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					result, err := runner.Start(context.Background(), "fanout", 0, options)
					if err != nil {
						b.Fatal(err)
					}
					if result.Status != graph.StatusCompleted || result.Checkpoint.Final == nil || *result.Checkpoint.Final != width {
						b.Fatalf("unexpected result: status=%s final=%v", result.Status, result.Checkpoint.Final)
					}
				}
			})
		}
	}
}

func BenchmarkConcurrentRuns(b *testing.B) {
	for _, persistence := range []string{"none", "memory", "sqlite"} {
		b.Run(persistence, func(b *testing.B) {
			runner := benchmarkSingleNodeRunner(b)
			var store graph.Store[int]
			var closeStore func() error
			switch persistence {
			case "memory":
				memoryStore, err := memory.New[int](func(value int) (int, error) { return value, nil })
				if err != nil {
					b.Fatal(err)
				}
				store = memoryStore
			case "sqlite":
				sqliteStore, err := sqlite.Open(context.Background(), filepath.Join(b.TempDir(), "runs.db"), checkpoint.JSON[int]{})
				if err != nil {
					b.Fatal(err)
				}
				store = sqliteStore
				closeStore = sqliteStore.Close
			}
			if closeStore != nil {
				defer func() {
					if err := closeStore(); err != nil {
						b.Error(err)
					}
				}()
			}

			var runNumber atomic.Uint64
			var firstFailure atomic.Pointer[benchmarkFailure]
			options := graph.Options[int]{Store: store}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if firstFailure.Load() != nil {
						return
					}
					runID := strconv.FormatUint(runNumber.Add(1), 10)
					result, err := runner.Start(context.Background(), runID, 0, options)
					if err != nil {
						firstFailure.CompareAndSwap(nil, &benchmarkFailure{err: err})
						return
					}
					if result.Status != graph.StatusCompleted || result.Checkpoint.Final == nil || *result.Checkpoint.Final != 1 {
						firstFailure.CompareAndSwap(nil, &benchmarkFailure{message: "unexpected completed run result"})
						return
					}
				}
			})
			b.StopTimer()
			if failure := firstFailure.Load(); failure != nil {
				if failure.err != nil {
					b.Fatal(failure.err)
				}
				b.Fatal(failure.message)
			}
		})
	}
}

type benchmarkFailure struct {
	err     error
	message string
}

func benchmarkSequentialRunner(b *testing.B, steps int) *graph.Runner[int] {
	b.Helper()
	g := graph.New[int]("step")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "step", Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		next := state + 1
		if next == steps {
			return graph.EndExecution(next), nil
		}
		return graph.To(next, "step"), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddEdge("step", "step"); err != nil {
		b.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[int]{MachineID: "benchmark-sequential-v1", Clone: func(value int) (int, error) { return value, nil }})
	if err != nil {
		b.Fatal(err)
	}
	return runner
}

func benchmarkFanoutRunner(b *testing.B, width int) *graph.Runner[int] {
	b.Helper()
	g := graph.New[int]("fork")
	targets := make([]string, width)
	if err := g.AddNode(graph.NodeSpec[int]{Name: "fork", Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.To(state, targets...), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, states []int) (int, error) {
		merged := 0
		for _, state := range states {
			merged += state
		}
		return merged, nil
	}}); err != nil {
		b.Fatal(err)
	}
	for i := range width {
		name := "branch-" + strconv.Itoa(i)
		targets[i] = name
		if err := g.AddNode(graph.NodeSpec[int]{Name: name, Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.To(state+1, "join"), nil
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
	if err := g.AddNode(graph.NodeSpec[int]{Name: "done", Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddEdge("join", "done"); err != nil {
		b.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[int]{MachineID: "benchmark-fanout-v1", Clone: func(value int) (int, error) { return value, nil }})
	if err != nil {
		b.Fatal(err)
	}
	return runner
}

func benchmarkSingleNodeRunner(b *testing.B) *graph.Runner[int] {
	b.Helper()
	g := graph.New[int]("done")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "done", Run: func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state + 1), nil
	}}); err != nil {
		b.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[int]{MachineID: "benchmark-concurrent-v1", Clone: func(value int) (int, error) { return value, nil }})
	if err != nil {
		b.Fatal(err)
	}
	return runner
}
