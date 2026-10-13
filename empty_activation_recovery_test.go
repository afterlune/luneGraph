package graph_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func TestEmptyActivationSupportsResumeRecoverAndCloneOnlyFork(t *testing.T) {
	for _, storage := range []string{"memory", "sqlite"} {
		t.Run(storage, func(t *testing.T) {
			ctx := context.Background()
			g := graph.New[int]("dispatch")
			node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.FanOut(s), nil
			})
			calls := map[string][]string{}
			if err := g.AddJoin(graph.JoinSpec[int]{Name: "joined", From: "dispatch", Merge: func(_ context.Context, call graph.CallInfo, s int, branches []graph.BranchResult[int]) (int, error) {
				if len(branches) != 0 {
					return s, errors.New("empty activation acquired branches")
				}
				calls[call.RunID] = append(calls[call.RunID], call.CallID)
				if call.RunID == "empty" && len(calls[call.RunID]) == 1 {
					return s, graph.Interrupt(nil)
				}
				return s + 1, nil
			}}); err != nil {
				t.Fatal(err)
			}
			node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.EndExecution(s), nil
			})
			edge(t, g, "joined", "done")
			runner := intRunner(t, g)
			var store graph.Store[int] = newMemoryStore(t)
			var sqlStore *sqlite.Store[int]
			path := filepath.Join(t.TempDir(), "runs.db")
			if storage == "sqlite" {
				var err error
				sqlStore, err = sqlite.Open(ctx, path, checkpoint.JSON[int]{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if sqlStore != nil {
						_ = sqlStore.Close()
					}
				})
				store = sqlStore
			}
			e, first, err := runner.Start(ctx, "empty", 4, store, graph.Options{})
			if !errors.Is(err, graph.ErrInterrupted) || first.Header.Revision != 2 {
				t.Fatalf("empty join interrupt: %+v %v", first, err)
			}
			cp := mustCheckpoint(e)
			if len(cp.Groups) != 1 || len(cp.Groups[0].Branches) != 0 || cp.Completed {
				t.Fatalf("pending empty activation: %+v", cp)
			}
			forked, err := runner.Fork(ctx, "forked", cp, store)
			if err != nil || forked.Revision != 1 || len(calls["forked"]) != 0 {
				t.Fatalf("clone-only empty fork: %+v %v", forked, err)
			}
			_, resumed, err := runner.Resume(ctx, cp, nil, nil, graph.Options{})
			if err != nil || resumed.Status != graph.StatusCompleted {
				t.Fatalf("resume empty activation: %+v %v", resumed, err)
			}
			if sqlStore != nil {
				if err := sqlStore.Close(); err != nil {
					t.Fatal(err)
				}
				sqlStore, err = sqlite.Open(ctx, path, checkpoint.JSON[int]{})
				if err != nil {
					t.Fatal(err)
				}
				store = sqlStore
			}
			for _, id := range []string{"empty", "forked"} {
				_, result, err := runner.Recover(ctx, id, nil, store, graph.Options{})
				if err != nil || result.Status != graph.StatusCompleted || result.Final == nil || *result.Final != 5 {
					t.Fatalf("recover %s: %+v %v", id, result, err)
				}
				for _, callID := range calls[id] {
					if callID != cp.Groups[0].CallID {
						t.Fatalf("empty join identity changed: %v", calls)
					}
				}
			}
		})
	}
}
