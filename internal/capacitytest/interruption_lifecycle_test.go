package capacitytest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func seedInterrupted(t testing.TB, r *graph.Runner[state], store graph.Store[state], c *interruptionController, id string, width, concurrency int) interruptionSlot {
	t.Helper()
	out, err := r.Start(context.Background(), id, initial(id), graph.Options[state]{Store: store, MaxConcurrency: concurrency})
	if out.Status != graph.StatusInterrupted || !errors.Is(err, graph.ErrInterrupted) || out.Checkpoint.Revision != 1 || out.Checkpoint.Steps != 0 {
		t.Fatalf("seed=%+v %v", out, err)
	}
	if err := checkInterruptionCheckpoint(out.Checkpoint, width); err != nil {
		t.Fatal(err)
	}
	if !c.quiescent(id) {
		t.Fatal("callbacks outlived Start")
	}
	c.reconcile(out.Checkpoint)
	s := interruptionSlot{id: id}
	s.remember(out.Checkpoint, store != nil)
	return s
}

func TestCapacityInterruptionLifecycle(t *testing.T) {
	for _, kind := range []string{"none", "memory", "sqlite"} {
		for _, width := range []int{8, 128} {
			for _, concurrency := range []int{1, 8} {
				t.Run(fmt.Sprintf("%s/width=%d/concurrency=%d", kind, width, concurrency), func(t *testing.T) {
					c := &interruptionController{}
					r, store := interruptionRunner(t, width, c), openStore(t, kind)
					s := seedInterrupted(t, r, store, c, "loop", width, concurrency)
					for round := 1; round <= 4; round++ {
						if err := advanceInterruptedRound(context.Background(), r, store, c, &s, width, concurrency); err != nil {
							t.Fatal(err)
						}
						if s.checkpoint.Steps != uint64(round*(width+2)) || c.active.Load() != 0 {
							t.Fatalf("round=%d checkpoint=%+v active=%d", round, s.checkpoint, c.active.Load())
						}
					}
					c.mu.Lock()
					defer c.mu.Unlock()
					if len(c.pending) != 0 || c.counts[0] == 0 || c.counts[1] != 4 || c.counts[2] != 3 {
						t.Fatalf("controller pending=%d counts=%v", len(c.pending), c.counts)
					}
				})
			}
		}
	}
}

func TestCapacitySharedInterruptionLifecycle(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			c := &interruptionController{}
			r, store := interruptionRunner(t, 8, c), openStore(t, kind)
			slots := make([]interruptionSlot, 16)
			for i := range slots {
				slots[i] = seedInterrupted(t, r, store, c, fmt.Sprintf("shared-%d", i), 8, 8)
			}
			pool := newBatchPool(len(slots), 4, func(i int) error { return advanceInterruptedRound(context.Background(), r, store, c, &slots[i], 8, 8) })
			defer pool.close()
			for round := 1; round <= 8; round++ {
				if err := pool.batch(); err != nil {
					t.Fatal(err)
				}
				if c.active.Load() != 0 {
					t.Fatalf("callbacks outlived batch: %d", c.active.Load())
				}
				for _, s := range slots {
					saved, err := store.Load(context.Background(), s.id)
					if err != nil {
						t.Fatal(err)
					}
					v, err := rootState(saved)
					if err != nil || v.Owner != s.id || v.Round != round || saved.Revision != s.checkpoint.Revision || len(saved.Invocations) != 1 || len(saved.Groups) != 0 {
						t.Fatalf("saved=%+v %v", saved, err)
					}
				}
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			if len(c.pending) != 0 {
				t.Fatalf("retained controller history: %d", len(c.pending))
			}
		})
	}
}
