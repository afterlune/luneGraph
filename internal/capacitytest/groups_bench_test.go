package capacitytest

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func nestedPopulation(t testing.TB, count int) *graph.Runner[state] {
	g := graph.New[state]("root")
	targets := make([]string, count)
	for i := range count {
		targets[i] = fmt.Sprintf("fork%d", i)
	}
	addNode(t, g, "root", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
		return graph.To(s, targets...), nil
	})
	for i, fork := range targets {
		join := fmt.Sprintf("join%d", i)
		leaves := make([]string, 8)
		for j := range 8 {
			leaves[j] = fmt.Sprintf("leaf%d_%d", i, j)
		}
		addNode(t, g, fork, func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
			s.Values["group"] = i
			return graph.To(s, leaves...), nil
		})
		addEdge(t, g, "root", fork)
		for j, leaf := range leaves {
			addNode(t, g, leaf, func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
				if err := checkIdentity(call, s); err != nil {
					return graph.Transition[state]{}, err
				}
				s.Values["branch"] = j
				s.Total = 1
				return graph.To(s, join), nil
			})
			addEdge(t, g, fork, leaf)
		}
		if err := g.AddJoin(graph.JoinSpec[state]{Name: join, From: fork, Merge: func(_ context.Context, _ graph.CallInfo, values []state) (state, error) {
			if len(values) != 8 {
				return state{}, fmt.Errorf("inner count %d", len(values))
			}
			out := values[0]
			out.Total = 0
			for j, s := range values {
				if s.Values["group"] != i || s.Values["branch"] != j {
					return state{}, fmt.Errorf("inner order mismatch")
				}
				out.Total += s.Total
			}
			return out, nil
		}}); err != nil {
			t.Fatal(err)
		}
		for _, leaf := range leaves {
			addEdge(t, g, leaf, join)
		}
	}
	if err := g.AddJoin(graph.JoinSpec[state]{Name: "all", From: "root", Merge: func(_ context.Context, _ graph.CallInfo, values []state) (state, error) {
		if len(values) != count {
			return state{}, fmt.Errorf("outer count %d", len(values))
		}
		out := values[0]
		out.Total = 0
		for i, s := range values {
			if s.Values["group"] != i || s.Total != 8 {
				return state{}, fmt.Errorf("outer order mismatch")
			}
			out.Total += s.Total
		}
		return out, nil
	}}); err != nil {
		t.Fatal(err)
	}
	for i := range count {
		addEdge(t, g, fmt.Sprintf("join%d", i), "all")
	}
	addNode(t, g, "done", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
		return graph.EndExecution(s), nil
	})
	addEdge(t, g, "all", "done")
	return compile(t, g)
}

func BenchmarkCapacityGroups(b *testing.B) {
	for _, count := range []int{8, 32, 128} {
		for _, concurrency := range []int{1, 8} {
			b.Run(fmt.Sprintf("groups=%d/concurrency=%d", count, concurrency), func(b *testing.B) {
				r := nestedPopulation(b, count)
				seed, err := r.Start(context.Background(), "groups", initial("groups"), graph.Options[state]{MaxSteps: count + 1, MaxConcurrency: 1})
				if err != nil || len(seed.Checkpoint.Groups) != count+1 {
					b.Fatalf("seed: %v groups=%d", err, len(seed.Checkpoint.Groups))
				}
				original := fmt.Sprintf("%#v", seed.Checkpoint)
				values := make([]map[string]int, len(seed.Checkpoint.Invocations))
				for i, inv := range seed.Checkpoint.Invocations {
					copied, _ := clone(inv.State)
					values[i] = copied.Values
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					out, err := r.Resume(context.Background(), seed.Checkpoint, nil, graph.Options[state]{MaxSteps: count*8 + 1, MaxConcurrency: concurrency})
					if err != nil || out.Checkpoint.Final == nil || out.Checkpoint.Final.Total != count*8 || out.Checkpoint.Steps != uint64(2+count*9) {
						b.Fatalf("result: %+v %v", out, err)
					}
				}
				b.StopTimer()
				if original != fmt.Sprintf("%#v", seed.Checkpoint) {
					b.Fatal("seed structure mutated")
				}
				for i, inv := range seed.Checkpoint.Invocations {
					if !reflect.DeepEqual(inv.State.Values, values[i]) {
						b.Fatal("seed state mutated")
					}
				}
				b.ReportMetric(float64(count+1), "groups/op")
				b.ReportMetric(float64(count*8), "branches/op")
			})
		}
	}
}
