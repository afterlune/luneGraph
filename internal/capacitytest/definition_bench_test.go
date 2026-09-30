package capacitytest

import (
	"context"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func BenchmarkCapacityCompile(b *testing.B) {
	for _, size := range []int{128, 1024, 8192} {
		b.Run("nodes="+strconv.Itoa(size), func(b *testing.B) {
			g := chainGraph(b, size)
			r := compile(b, g)
			out, err := r.Start(context.Background(), "chain", initial("chain"), graph.Options[state]{MaxSteps: size + 1})
			if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || out.Checkpoint.Final.Total != size || out.Checkpoint.Final.Values["steps"] != size || out.Checkpoint.Steps != uint64(size) {
				b.Fatalf("invalid chain: %+v, %v", out, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r, err := g.Compile(graph.Config[state]{MachineID: "capacity-v1", Clone: clone})
				if err != nil || r == nil {
					b.Fatalf("Compile: %v", err)
				}
			}
			b.ReportMetric(float64(size), "nodes/op")
		})
	}
}
