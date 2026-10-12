package observationtest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
	"github.com/afterlune/luneGraph/observe"
)

func BenchmarkObservation(b *testing.B) {
	for _, workload := range []string{"sequential", "fanout", "memory", "sqlite"} {
		for _, mode := range []string{"disabled", "noop", "slog"} {
			b.Run(workload+"/observer="+mode, func(b *testing.B) {
				ctx := context.Background()
				opts := graph.Options{MaxSteps: 64}
				switch mode {
				case "noop":
					opts.Observer = graph.ObserverFunc(func(context.Context, graph.Event) {})
				case "slog":
					opts.Observer = observe.NewSlog(slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})))
				}
				durable := workload == "memory" || workload == "sqlite"
				limit := 16
				var store graph.Store[int]
				if durable {
					limit = 0
					opts.MaxSteps = 16
				}
				r := counterRunner(b, limit)
				if workload == "fanout" {
					r = fanoutRunner(b, 32)
				}
				switch workload {
				case "memory":
					store = memoryStore(b)
				case "sqlite":
					store, err := sqlite.Open(ctx, filepath.Join(b.TempDir(), "runs.db"), checkpoint.JSON[int]{})
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() {
						if err := store.Close(); err != nil {
							b.Error(err)
						}
					})
				}
				if durable {
					_, seed, err := r.Start(ctx, "bench", 0, store, graph.Options{MaxSteps: 1})
					if err != nil || seed.Status != graph.StatusBudget {
						b.Fatalf("seed = %+v, %v", seed, err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				var result graph.Result[int]
				var resultExecution *graph.Execution[int]
				for i := range b.N {
					var err error
					if durable {
						resultExecution, result, err = r.Recover(ctx, "bench", nil, store, opts)
					} else {
						resultExecution, result, err = r.Start(ctx, "bench", 0, nil, opts)
					}
					if err != nil {
						b.Fatal(err)
					}
					if durable {
						progress := 1 + 16*(i+1)
						if result.Status != graph.StatusBudget || result.Header.Steps != uint64(progress) || result.Header.Revision != uint64(progress+1) || len(mustCheckpoint(resultExecution).Invocations) != 1 || mustCheckpoint(resultExecution).Invocations[0].State != progress {
							b.Fatalf("recovery = %+v", result)
						}
					} else if result.Status != graph.StatusCompleted {
						b.Fatalf("result = %+v", result)
					} else if workload == "fanout" {
						if len(mustCheckpoint(resultExecution).Invocations) != 0 || result.Final != nil {
							b.Fatal("invalid merge")
						}
					} else if result.Final == nil || *result.Final != 16 {
						b.Fatal("invalid final state")
					}
				}
				b.StopTimer()
				if durable {
					saved, err := store.Load(ctx, "bench")
					if err != nil || saved.Revision != result.Header.Revision || saved.Steps != result.Header.Steps || len(saved.Invocations) != 1 || saved.Invocations[0].State != mustCheckpoint(resultExecution).Invocations[0].State {
						b.Fatalf("stored checkpoint = %+v, %v", saved, err)
					}
				}
			})
		}
	}
}

func counterRunner(t testing.TB, limit int) *graph.Runner[int] {
	g := graph.New[int]("step")
	node(t, g, "step", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		if limit > 0 && s+1 == limit {
			return graph.EndExecution(s + 1), nil
		}
		return graph.To(s+1, "step"), nil
	})
	edge(t, g, "step", "step")
	return compile(t, g)
}

func fanoutRunner(t testing.TB, width int) *graph.Runner[int] {
	g := graph.New[int]("fork")
	targets := make([]string, width)
	for i := range width {
		targets[i] = strconv.Itoa(i)
	}
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.ToTargets(s, targets), nil
	})
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, _ int, branches []graph.BranchResult[int]) (int, error) {
		values := graph.BranchValues(branches)
		var sum int
		for _, v := range values {
			sum += v
		}
		if len(values) != 32 || sum != 32 {
			return 0, fmt.Errorf("invalid fan-out merge: %d values, sum=%d", len(values), sum)
		}
		return sum, nil
	}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range targets {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.To(s+1, "join"), nil
		})
		edge(t, g, "fork", name)
		edge(t, g, name, "join")
	}
	return compile(t, g)
}
