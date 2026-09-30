package capacitytest

import (
	"context"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestNestedPopulationAcrossRecoveryBudgets(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, concurrency := range []int{1, 8} {
			t.Run(kind+"/concurrency="+strconv.Itoa(concurrency), func(t *testing.T) {
				r, store := nestedPopulation(t, 8), openStore(t, kind)
				out, err := r.Start(context.Background(), "nested", initial("nested"), graph.Options[state]{Store: store, MaxSteps: 9, MaxConcurrency: 1})
				if err != nil || len(out.Checkpoint.Groups) != 9 {
					t.Fatalf("seed: %+v %v", out, err)
				}
				previous := out.Checkpoint.Steps
				for attempt := 0; attempt < 20 && !out.Checkpoint.Completed; attempt++ {
					out, err = r.Recover(context.Background(), "nested", nil, graph.Options[state]{Store: store, MaxSteps: 7, MaxConcurrency: concurrency})
					if err != nil || out.Checkpoint.Steps <= previous || out.Checkpoint.Steps > previous+7 || out.Checkpoint.Revision != out.Checkpoint.Steps+1 {
						t.Fatalf("recovery: %+v %v", out, err)
					}
					previous = out.Checkpoint.Steps
				}
				if out.Checkpoint.Final == nil || out.Checkpoint.Final.Total != 64 || out.Checkpoint.Steps != 74 || len(out.Checkpoint.Groups) != 0 || (out.Checkpoint.Failure != nil || out.Checkpoint.HadLocalFailures) {
					t.Fatalf("final: %+v", out)
				}
			})
		}
	}
}
