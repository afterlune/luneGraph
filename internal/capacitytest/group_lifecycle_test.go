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
				outExecution, out, err := r.Start(context.Background(), "nested", initial("nested"), store, graph.Options{MaxSteps: 9, MaxConcurrency: 1})
				if err != nil || len(mustCheckpoint(outExecution).Groups) != 9 {
					t.Fatalf("seed: %+v %v", out, err)
				}
				previous := out.Header.Steps
				for attempt := 0; attempt < 20 && !out.Header.Completed; attempt++ {
					outExecution, out, err = r.Recover(context.Background(), "nested", nil, store, graph.Options{MaxSteps: 7, MaxConcurrency: concurrency})
					if err != nil || out.Header.Steps <= previous || out.Header.Steps > previous+7 || out.Header.Revision < out.Header.Steps+1 {
						t.Fatalf("recovery: %+v %v", out, err)
					}
					previous = out.Header.Steps
				}
				if out.Final == nil || out.Final.Total != 64 || out.Header.Steps != 74 || len(mustCheckpoint(outExecution).Groups) != 0 || (out.Header.Failure != nil || out.Header.HadLocalFailures) {
					t.Fatalf("final: %+v", out)
				}
			})
		}
	}
}
