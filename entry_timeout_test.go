package graph_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func checkCallbackDeadline(ctx context.Context, entry bool) error {
	deadline, ok := ctx.Deadline()
	remaining := time.Until(deadline)
	if !ok || (entry && (remaining < 29*time.Minute || remaining > 31*time.Minute)) || (!entry && remaining < 45*time.Minute) {
		return fmt.Errorf("wrong callback deadline: entry=%t present=%t remaining=%s", entry, ok, remaining)
	}
	return nil
}

type entryTrackingStore struct {
	graph.Store[int]
	cleared bool
	reject  bool
}

func (s *entryTrackingStore) Commit(ctx context.Context, expected uint64, c graph.Change[int]) error {
	if s.reject {
		s.reject = false
		return graph.ErrConflict
	}
	if err := s.Store.Commit(ctx, expected, c); err != nil {
		return err
	}
	for _, g := range c.UpsertGroups {
		if g.Source == "dispatch" && g.Branches[0].EntryTimeout == 0 {
			s.cleared = true
		}
	}
	return nil
}

func entryTimeoutGraph(t *testing.T, mode string) *graph.Graph[int] {
	t.Helper()
	g := graph.New[int]("dispatch")
	target := "entry"
	if mode == "return" {
		target = "child"
	}
	node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.FanOut(s, graph.Branch[int]{Target: target, State: s, EntryTimeout: 30 * time.Minute}), nil
	})
	entry := func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		if err := checkCallbackDeadline(ctx, mode != "loop" || s == 0); err != nil {
			return graph.Transition[int]{}, err
		}
		switch mode {
		case "static":
			return graph.To(s, "a", "b"), nil
		case "dynamic":
			return graph.FanOut(s, graph.Branch[int]{Target: "a", State: s}), nil
		case "wait":
			return graph.Wait(s, "input", "next"), nil
		case "loop":
			if s == 0 {
				return graph.To(1, "entry"), nil
			}
			return graph.CompleteBranch(s), nil
		case "return":
			return graph.Return(s), nil
		case "complete":
			return graph.CompleteBranch(s), nil
		case "end":
			return graph.EndBranch[int](), nil
		default:
			panic("unknown entry scenario")
		}
	}
	owner := g
	if mode == "return" {
		owner = graph.New[int]("entry")
		if err := g.AddSubgraph("child", owner); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.AddNode(graph.NodeSpec[int]{Name: "entry", OnError: graph.FailExecution, Timeout: time.Hour, Run: entry}); err != nil {
		t.Fatal(err)
	}
	edge(t, g, "dispatch", target)
	if err := g.AddNode(graph.NodeSpec[int]{Name: "next", OnError: graph.FailExecution, Timeout: time.Hour, Run: func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		if err := checkCallbackDeadline(ctx, false); err != nil {
			return graph.Transition[int]{}, err
		}
		return graph.CompleteBranch(s), nil
	}}); err != nil {
		t.Fatal(err)
	}
	if mode == "static" || mode == "dynamic" {
		for _, name := range []string{"a", "b"} {
			node(t, g, name, func(ctx context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				if _, ok := ctx.Deadline(); ok {
					return graph.Transition[int]{}, errors.New("entry deadline leaked into child task")
				}
				return graph.CompleteBranch(s), nil
			})
			edge(t, g, "entry", name)
		}
		if err := g.AddJoin(graph.JoinSpec[int]{Name: "inner", From: "entry", Merge: func(_ context.Context, _ graph.CallInfo, s int, _ []graph.BranchResult[int]) (int, error) {
			return s, nil
		}}); err != nil {
			t.Fatal(err)
		}
		edge(t, g, "inner", "next")
	}
	switch mode {
	case "loop":
		edge(t, g, "entry", "entry")
	case "return":
		edge(t, g, "child", "next")
	case "wait":
		edge(t, g, "entry", "next")
		if err := graph.RegisterJSONContinuation(g, "input", func(_ context.Context, _ graph.CallInfo, s, p int) (int, error) { return s + p, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "outer", From: "dispatch", Merge: func(_ context.Context, _ graph.CallInfo, s int, branches []graph.BranchResult[int]) (int, error) {
		if !branches[0].OK() && !branches[0].Ended() {
			return s, errors.New("entry branch failed")
		}
		return s, nil
	}}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.EndExecution(s), nil
	})
	edge(t, g, "outer", "done")
	return g
}

func TestEntryTimeoutEndsAtAcceptedEntryOutcome(t *testing.T) {
	for _, storage := range []string{"memory", "sqlite"} {
		for _, mode := range []string{"static", "dynamic", "wait", "loop", "return", "complete", "end"} {
			t.Run(storage+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				var raw graph.Store[int] = newMemoryStore(t)
				if storage == "sqlite" {
					s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "runs.db"), checkpoint.JSON[int]{})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = s.Close() })
					raw = s
				}
				store := &entryTrackingStore{Store: raw}
				runner := intRunner(t, entryTimeoutGraph(t, mode))
				e, first, err := runner.Start(ctx, "entry-timeout", 0, store, graph.Options{MaxSteps: 1})
				if err != nil || first.Status != graph.StatusBudget {
					t.Fatalf("dispatch: %+v %v", first, err)
				}
				before := mustCheckpoint(e)
				if before.Groups[0].Branches[0].EntryTimeout != 30*time.Minute {
					t.Fatal("pending entry lost its override")
				}
				e, result, err := runner.Recover(ctx, "entry-timeout", nil, store, graph.Options{})
				if err != nil {
					t.Fatal(err)
				}
				if mode == "wait" {
					if result.Status != graph.StatusWaiting {
						t.Fatalf("wait: %+v", result)
					}
					cp := mustCheckpoint(e)
					if cp.Groups[0].Branches[0].EntryTimeout != 0 {
						t.Fatal("wait retained entry override")
					}
					corrupt := cp
					corrupt.Groups[0].Branches = append([]graph.ActivationBranch(nil), cp.Groups[0].Branches...)
					corrupt.Groups[0].Branches[0].EntryTimeout = 30 * time.Minute
					if handle, _, err := runner.Resume(ctx, corrupt, nil, nil, graph.Options{}); handle != nil || !errors.Is(err, graph.ErrInvalidCheckpoint) {
						t.Fatalf("waiting position accepted pending entry override: handle=%v error=%v", handle, err)
					}
					pending := e.Pending()[0]
					_, result, err = runner.Recover(ctx, "entry-timeout", []graph.ResumeInput{{InvocationID: pending.InvocationID, CallID: pending.CallID, Payload: []byte("1")}}, store, graph.Options{})
				}
				if err != nil || result.Status != graph.StatusCompleted || !store.cleared {
					t.Fatalf("entry outcome: %+v %v cleared=%t", result, err, store.cleared)
				}
				if before.Groups[0].Branches[0].EntryTimeout != 30*time.Minute {
					t.Fatal("accepted outcome mutated earlier checkpoint")
				}
			})
		}
	}
}

func TestRejectedEntryOutcomesRetainOverrideAndReplayIdentity(t *testing.T) {
	for _, reason := range []string{"budget", "commit", "clone", "invalid"} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			g := graph.New[int]("dispatch")
			var callIDs []string
			cloneFails, invalid := false, false
			node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.FanOut(s, graph.Branch[int]{Target: "entry", State: s, EntryTimeout: 30 * time.Minute}), nil
			})
			node(t, g, "entry", func(ctx context.Context, call graph.CallInfo, s int) (graph.Transition[int], error) {
				callIDs = append(callIDs, call.CallID)
				if err := checkCallbackDeadline(ctx, true); err != nil {
					return graph.Transition[int]{}, err
				}
				if invalid {
					return graph.To(s, "missing"), nil
				}
				return graph.FanOut(s, graph.Branch[int]{Target: "leaf", State: s}), nil
			})
			node(t, g, "leaf", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.CompleteBranch(s), nil
			})
			node(t, g, "finish", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.CompleteBranch(s), nil
			})
			node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.EndExecution(s), nil
			})
			for _, join := range []graph.JoinSpec[int]{
				{Name: "inner", From: "entry", Merge: func(_ context.Context, _ graph.CallInfo, s int, _ []graph.BranchResult[int]) (int, error) {
					return s, nil
				}},
				{Name: "outer", From: "dispatch", Merge: func(_ context.Context, _ graph.CallInfo, s int, _ []graph.BranchResult[int]) (int, error) {
					return s, nil
				}},
			} {
				if err := g.AddJoin(join); err != nil {
					t.Fatal(err)
				}
			}
			for _, pair := range [][2]string{{"dispatch", "entry"}, {"entry", "leaf"}, {"inner", "finish"}, {"outer", "done"}} {
				edge(t, g, pair[0], pair[1])
			}
			runner, err := g.Compile(graph.Config[int]{MachineID: "entry-retry", Clone: func(s int) (int, error) {
				if cloneFails {
					return 0, errors.New("clone rejected")
				}
				return s, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			store := &entryTrackingStore{Store: newMemoryStore(t)}
			e, _, err := runner.Start(ctx, "entry-retry", 0, store, graph.Options{MaxSteps: 1})
			if err != nil {
				t.Fatal(err)
			}
			before := mustCheckpoint(e)
			opts := graph.Options{MaxSteps: 1}
			switch reason {
			case "budget":
				opts.MaxInvocations = 2
			case "commit":
				store.reject = true
			case "clone":
				cloneFails = true
			case "invalid":
				invalid = true
			}
			result, err := e.Advance(ctx, nil, opts)
			if reason == "budget" {
				if err != nil || result.BudgetReason != graph.BudgetMaxInvocations {
					t.Fatalf("budget: %+v %v", result, err)
				}
			} else if err == nil {
				t.Fatal("expected rejected entry")
			}
			if reason == "commit" && !errors.Is(err, graph.ErrConflict) {
				t.Fatal(err)
			}
			cloneFails, invalid = false, false
			if !reflect.DeepEqual(mustCheckpoint(e), before) {
				t.Fatal("rejection changed accepted entry metadata")
			}
			e, result, err = runner.Recover(ctx, "entry-retry", nil, store, graph.Options{MaxSteps: 1})
			if err != nil || result.Status != graph.StatusBudget || !store.cleared {
				t.Fatalf("retry: %+v %v", result, err)
			}
			for _, id := range callIDs {
				if id != before.Invocations[1].CallID {
					t.Fatalf("entry identity changed: %v", callIDs)
				}
			}
			if _, result, err = runner.Recover(ctx, "entry-retry", nil, store, graph.Options{}); err != nil || result.Status != graph.StatusCompleted {
				t.Fatalf("finish: %+v %v", result, err)
			}
		})
	}
}
