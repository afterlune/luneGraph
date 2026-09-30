package capacitytest

import (
	"context"
	"fmt"
	"sync"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type callbackTracker struct {
	mu           sync.Mutex
	active, peak int
	seen         map[string]bool
	err          error
}

func (c *callbackTracker) Observe(_ context.Context, e graph.Event) {
	if e.Operation != graph.OperationNode && e.Operation != graph.OperationJoin && e.Operation != graph.OperationApply {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e.Phase == graph.PhaseStarted {
		if e.CallID == "" || c.seen[e.CallID] {
			c.err = fmt.Errorf("missing or repeated callback ID: %+v", e)
		}
		c.seen[e.CallID] = true
	}
	if e.Operation == graph.OperationNode {
		if e.Phase == graph.PhaseStarted {
			c.active++
		} else {
			c.active--
		}
		if c.active > c.peak {
			c.peak = c.active
		}
	}
}

func TestWideFanoutStateIsolation(t *testing.T) {
	r := fanoutRunner(t, 128, 1, 0)
	tracker := &callbackTracker{seen: map[string]bool{}}
	out, err := r.Start(context.Background(), "wide", initial("wide"), graph.Options[state]{MaxSteps: 130, MaxConcurrency: 8, Observer: tracker})
	if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || out.Checkpoint.Final.Total != 128 || out.Checkpoint.Final.Values["sum"] != 8256 || tracker.err != nil || tracker.active != 0 || tracker.peak > 8 {
		t.Fatalf("fan-out result: %+v, %v, tracker=%+v", out, err, tracker)
	}
}

func TestRepeatedBudgetWaitAndRecovery(t *testing.T) {
	for _, profile := range []struct {
		kind          string
		width, rounds int
	}{{"memory", 32, 32}, {"sqlite", 8, 8}} {
		t.Run(profile.kind, func(t *testing.T) {
			ctx := context.Background()
			r, store := fanoutRunner(t, profile.width, 0, 4), openStore(t, profile.kind)
			tracker := &callbackTracker{seen: map[string]bool{}}
			opts := graph.Options[state]{Store: store, MaxSteps: 1, MaxConcurrency: 8, Observer: tracker}
			out, err := r.Start(ctx, "loop", initial("loop"), opts)
			if err != nil || out.Status != graph.StatusBudget || len(out.Checkpoint.Invocations) != profile.width+1 || len(out.Checkpoint.Groups) != 1 {
				t.Fatalf("fork checkpoint: %+v, %v", out, err)
			}
			previousRevision, previousID := out.Checkpoint.Revision, out.Checkpoint.NextID
			inputs := 0
			for round := 1; round <= profile.rounds; round++ {
				opts.MaxSteps = profile.width + 2
				if round == 1 {
					opts.MaxSteps--
				}
				var payloads []graph.ResumeInput
				if out.Status == graph.StatusWaiting {
					payloads = []graph.ResumeInput{{InvocationID: "i1", Payload: []byte("1")}}
					inputs++
				}
				out, err = r.Recover(ctx, "loop", payloads, opts)
				s, stateErr := rootState(out.Checkpoint)
				wantStatus := graph.StatusBudget
				if round%4 == 0 {
					wantStatus = graph.StatusWaiting
				}
				if err != nil || stateErr != nil || out.Status != wantStatus || s.Round != round || s.Total != round*profile.width || s.Values["sum"] != round*profile.width*(profile.width+1)/2 || s.Values["inputs"] != inputs || out.Checkpoint.Steps != uint64(round*(profile.width+2)) || out.Checkpoint.Revision != out.Checkpoint.Steps+1+uint64(inputs) || out.Checkpoint.Revision <= previousRevision || out.Checkpoint.NextID <= previousID || len(out.Checkpoint.Invocations) != 1 || len(out.Checkpoint.Groups) != 0 || (out.Checkpoint.Failure != nil || out.Checkpoint.HadLocalFailures) {
					t.Fatalf("round %d = %+v, %v, %v", round, out, err, stateErr)
				}
				previousRevision, previousID = out.Checkpoint.Revision, out.Checkpoint.NextID
			}
			if tracker.err != nil || tracker.active != 0 || tracker.peak > 8 {
				t.Fatalf("callback tracker: %+v", tracker)
			}
			saved, err := store.Load(ctx, "loop")
			if err != nil || saved.Revision != out.Checkpoint.Revision || saved.Steps != out.Checkpoint.Steps {
				t.Fatalf("saved checkpoint: %+v, %v", saved, err)
			}
		})
	}
}

func TestSharedWaitingPopulation(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			r, store := waitingRunner(t), openStore(t, kind)
			slots := population(t, r, store, 64)
			pool := newBatchPool(len(slots), 8, func(i int) error { return advance(context.Background(), r, store, &slots[i]) })
			defer pool.close()
			for range 4 {
				if err := pool.batch(); err != nil {
					t.Fatal(err)
				}
			}
			verifyPopulation(t, store, slots)
		})
	}
}
