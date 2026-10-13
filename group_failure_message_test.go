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

type rejectGroupFailureStore struct {
	graph.Store[int]
	reject bool
}

func (s *rejectGroupFailureStore) Commit(ctx context.Context, expected uint64, c graph.Change[int]) error {
	for _, inv := range c.UpsertInvocations {
		if s.reject && inv.Node == "middle" && inv.Status == graph.InvocationFailed {
			s.reject = false
			return graph.ErrConflict
		}
	}
	return s.Store.Commit(ctx, expected, c)
}

func TestNestedGroupFailureMessageSurvivesConflictAndReopen(t *testing.T) {
	for _, storage := range []string{"memory", "sqlite"} {
		for _, origin := range []string{"node", "merge"} {
			t.Run(storage+"/"+origin, func(t *testing.T) {
				ctx := context.Background()
				boom := errors.New("nested " + origin + " failed")
				g := graph.New[int]("dispatch")
				node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					return graph.FanOut(s, graph.Branch[int]{Target: "middle", State: s}, graph.Branch[int]{Target: "survivor", State: s}), nil
				})
				node(t, g, "middle", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					return graph.FanOut(s, graph.Branch[int]{Target: "entry", State: s}), nil
				})
				if err := g.AddNode(graph.NodeSpec[int]{Name: "entry", OnError: graph.FailGroup, Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					if origin == "node" {
						return graph.Transition[int]{}, boom
					}
					return graph.FanOut(s, graph.Branch[int]{Target: "leaf", State: s}), nil
				}}); err != nil {
					t.Fatal(err)
				}
				node(t, g, "leaf", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					return graph.CompleteBranch(s), nil
				})
				node(t, g, "survivor", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					return graph.CompleteBranch(s + 1), nil
				})
				for _, join := range []graph.JoinSpec[int]{
					{Name: "inner", From: "entry", OnError: graph.FailGroup, Merge: func(context.Context, graph.CallInfo, int, []graph.BranchResult[int]) (int, error) { return 0, boom }},
					{Name: "middle-join", From: "middle", Merge: func(context.Context, graph.CallInfo, int, []graph.BranchResult[int]) (int, error) {
						return 0, errors.New("pruned middle join must not run")
					}},
				} {
					if err := g.AddJoin(join); err != nil {
						t.Fatal(err)
					}
				}
				merges := 0
				if err := g.AddJoin(graph.JoinSpec[int]{Name: "outer", From: "dispatch", Merge: func(_ context.Context, _ graph.CallInfo, _ int, branches []graph.BranchResult[int]) (int, error) {
					merges++
					if !branches[0].Failed() || branches[0].Error != boom.Error() || !branches[1].OK() {
						return 0, errors.New("outer join lost branch failure message or survivor")
					}
					if merges == 1 {
						return 0, graph.Interrupt(nil)
					}
					return branches[1].State, nil
				}}); err != nil {
					t.Fatal(err)
				}
				node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					return graph.EndExecution(s), nil
				})
				for _, pair := range [][2]string{{"dispatch", "middle"}, {"dispatch", "survivor"}, {"middle", "entry"}, {"entry", "leaf"}, {"outer", "done"}} {
					edge(t, g, pair[0], pair[1])
				}
				runner := intRunner(t, g)
				var raw graph.Store[int] = newMemoryStore(t)
				var sqlStore *sqlite.Store[int]
				dbPath := filepath.Join(t.TempDir(), "runs.db")
				if storage == "sqlite" {
					var err error
					sqlStore, err = sqlite.Open(ctx, dbPath, checkpoint.JSON[int]{})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if sqlStore != nil {
							_ = sqlStore.Close()
						}
					})
					raw = sqlStore
				}
				store := &rejectGroupFailureStore{Store: raw, reject: true}
				_, result, err := runner.Start(ctx, "group-message", 3, store, graph.Options{MaxConcurrency: 1})
				if !errors.Is(err, graph.ErrConflict) || result.Header.HadLocalFailures {
					t.Fatalf("rejected failure: %+v %v", result, err)
				}
				before, err := raw.Load(ctx, "group-message")
				if err != nil {
					t.Fatal(err)
				}
				for _, group := range before.Groups {
					for _, branch := range group.Branches {
						if branch.Error != "" {
							t.Fatal("rejected failure changed persisted message")
						}
					}
				}
				e, result, err := runner.Recover(ctx, "group-message", nil, store, graph.Options{MaxConcurrency: 1})
				if !errors.Is(err, graph.ErrInterrupted) || result.Status != graph.StatusInterrupted || !result.Header.HadLocalFailures {
					t.Fatalf("pending outer merge: %+v %v", result, err)
				}
				cp := mustCheckpoint(e)
				if len(cp.Groups) != 1 || cp.Groups[0].Branches[0].Error != boom.Error() {
					t.Fatalf("failed branch message: %+v", cp.Groups)
				}
				if sqlStore != nil {
					if err := sqlStore.Close(); err != nil {
						t.Fatal(err)
					}
					sqlStore, err = sqlite.Open(ctx, dbPath, checkpoint.JSON[int]{})
					if err != nil {
						t.Fatal(err)
					}
					raw = sqlStore
				}
				_, result, err = runner.Recover(ctx, "group-message", nil, raw, graph.Options{})
				if err != nil || result.Status != graph.StatusCompletedWithFailures || result.Final == nil || *result.Final != 4 || merges != 2 {
					t.Fatalf("recovered outer merge: %+v %v calls=%d", result, err, merges)
				}
			})
		}
	}
}
