package capacitytest

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/memory"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func openStore(t testing.TB, kind string) graph.Store[state] {
	t.Helper()
	switch kind {
	case "none":
		return nil
	case "memory":
		s, err := memory.New[state](clone)
		if err != nil {
			t.Fatal(err)
		}
		return s
	case "sqlite":
		s, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "runs.db"), checkpoint.JSON[state]{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		return s
	default:
		t.Fatalf("unknown Store %q", kind)
		return nil
	}
}

// Persisted slots retain only bookkeeping, not a second copy of user state.
type slot struct {
	id         string
	round      int
	revision   uint64
	checkpoint graph.Checkpoint[state] // used only without a Store
}

func population(t testing.TB, r *graph.Runner[state], store graph.Store[state], count int) []slot {
	t.Helper()
	slots := make([]slot, count)
	for i := range slots {
		id := fmt.Sprintf("run-%d", i)
		out, err := r.Start(context.Background(), id, initial(id), graph.Options[state]{Store: store})
		if err != nil || out.Status != graph.StatusWaiting || out.Checkpoint.Steps != 1 {
			t.Fatalf("seed %s = %+v, %v", id, out, err)
		}
		slots[i] = slot{id: id, round: 1, revision: out.Checkpoint.Revision}
		if store == nil {
			slots[i].checkpoint = out.Checkpoint
		}
	}
	return slots
}

func advance(ctx context.Context, r *graph.Runner[state], store graph.Store[state], s *slot) error {
	input := []graph.ResumeInput{{InvocationID: "i1", Payload: []byte("1")}}
	opts := graph.Options[state]{Store: store, MaxSteps: 2, MaxConcurrency: 1}
	var out graph.Result[state]
	var err error
	if store == nil {
		out, err = r.Resume(ctx, s.checkpoint, input, opts)
	} else {
		out, err = r.Recover(ctx, s.id, input, opts)
	}
	if err != nil {
		return err
	}
	value, stateErr := rootState(out.Checkpoint)
	if stateErr != nil {
		return stateErr
	}
	if out.Status != graph.StatusWaiting || value.Owner != s.id || value.Round != s.round+1 || value.Total != value.Round || value.Values["sum"] != value.Round || value.Values["inputs"] != value.Round-1 || out.Checkpoint.Revision != s.revision+2 || out.Checkpoint.Steps != uint64(value.Round) {
		return fmt.Errorf("advance %s mixed state or counters: %+v", s.id, out)
	}
	s.round, s.revision = value.Round, out.Checkpoint.Revision
	if store == nil {
		s.checkpoint = out.Checkpoint
	}
	return nil
}

func verifyPopulation(t testing.TB, store graph.Store[state], slots []slot) {
	t.Helper()
	for _, s := range slots {
		cp := s.checkpoint
		if store != nil {
			var err error
			cp, err = store.Load(context.Background(), s.id)
			if err != nil {
				t.Fatal(err)
			}
		}
		value, err := rootState(cp)
		if err != nil || value.Owner != s.id || value.Round != s.round || cp.Revision != s.revision || cp.Completed || len(cp.Invocations) != 1 || cp.Invocations[0].Status != graph.InvocationWaiting {
			t.Fatalf("stored %s = %+v, %v", s.id, cp, err)
		}
	}
}
