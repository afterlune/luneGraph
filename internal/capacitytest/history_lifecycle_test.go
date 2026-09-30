package capacitytest

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestHistoryAcrossBudgetWaitAndRecovery(t *testing.T) {
	for _, profile := range []struct {
		kind   string
		rounds int
	}{{"none", 128}, {"memory", 128}, {"sqlite", 32}} {
		for _, mode := range []string{"terminal", "failure", "mixed"} {
			for _, concurrency := range []int{1, 8} {
				t.Run(fmt.Sprintf("store=%s/mode=%s/concurrency=%d", profile.kind, mode, concurrency), func(t *testing.T) {
					r, store := historyRunner(t, mode), openStore(t, profile.kind)
					tracker := &callbackTracker{seen: map[string]bool{}}
					opts := graph.Options[state]{Store: store, MaxSteps: historyStepsPerRound, MaxConcurrency: concurrency, Observer: tracker}
					cp := seedHistory(t, r, opts, mode, profile.rounds)
					// Five nodes and one merge per round, plus one apply after
					// each earlier wait. Call IDs must never repeat on new work.
					wantCalls := profile.rounds*6 + (profile.rounds-1)/4
					if tracker.err != nil || tracker.active != 0 || tracker.peak > concurrency || len(tracker.seen) != wantCalls {
						t.Fatalf("callback lifecycle: seen=%d want=%d active=%d peak=%d err=%v", len(tracker.seen), wantCalls, tracker.active, tracker.peak, tracker.err)
					}
					// With no input, a waiting run must retain its execution position
					// and pending callback ID without invoking any callback.
					var out graph.Result[state]
					var err error
					if store == nil {
						out, err = r.Resume(context.Background(), cp, nil, opts)
					} else {
						out, err = r.Recover(context.Background(), cp.RunID, nil, opts)
					}
					if err != nil || out.Status != graph.StatusWaiting || !reflect.DeepEqual(cp, out.Checkpoint) || len(tracker.seen) != wantCalls {
						t.Fatalf("waiting recovery changed position or callbacks: %v", err)
					}
				})
			}
		}
	}
}
