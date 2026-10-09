package capacitytest

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func TestCapacityInterruptionSQLiteReopenFreshRunner(t *testing.T) {
	for _, role := range []string{"node", "join", "apply"} {
		t.Run(role, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "runs.db")
			store, err := sqlite.Open(ctx, path, checkpoint.JSON[state]{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { store.Close() })
			c := &interruptionController{}
			r := interruptionRunner(t, 8, c)
			s := seedInterrupted(t, r, store, c, "reopen", 8, 1)
			index := map[string]int{"node": 0, "join": 1, "apply": 2}[role]
			for attempt := 0; c.counts[index] == 0 && attempt < 100; attempt++ {
				_, err = interruptionCall(ctx, r, store, c, &s, 8, 1)
				if err != nil && !errors.Is(err, graph.ErrInterrupted) {
					t.Fatal(err)
				}
			}
			var wanted string
			for key, attempt := range c.pending {
				if attempt.role == role {
					wanted = key.call
				}
			}
			if wanted == "" {
				t.Fatalf("did not reach interrupted %s", role)
			}
			saved, err := store.Load(ctx, s.id)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = sqlite.Open(ctx, path, checkpoint.JSON[state]{})
			if err != nil {
				t.Fatal(err)
			}
			// A new controller and compiled Runner know nothing about old attempts.
			// Application readiness now permits the persisted callback to proceed.
			fresh := interruptionRunner(t, 8, &interruptionController{allow: true})
			var observed atomic.Bool
			op := map[string]graph.EventOperation{"node": graph.OperationNode, "join": graph.OperationJoin, "apply": graph.OperationApply}[role]
			out, err := fresh.Recover(ctx, s.id, interruptionInputs(saved), graph.Options[state]{Store: store, MaxConcurrency: 1, Observer: graph.ObserverFunc(func(_ context.Context, e graph.Event) {
				if e.Phase == graph.PhaseStarted && e.Operation == op && e.RunID == s.id && e.CallID == wanted {
					observed.Store(true)
				}
			})})
			if err != nil || out.Status != graph.StatusWaiting || !observed.Load() {
				t.Fatalf("reopened=%+v wanted=%s observed=%v %v", out, wanted, observed.Load(), err)
			}
			if err := checkInterruptionCheckpoint(out.Checkpoint, 8); err != nil {
				t.Fatal(err)
			}
			value, err := rootState(out.Checkpoint)
			wantRound := 1
			if role == "apply" {
				wantRound = 2
			}
			if err != nil || value.Round != wantRound || value.Values["inputs"] != wantRound-1 {
				t.Fatalf("value=%+v %v", value, err)
			}
		})
	}
}
