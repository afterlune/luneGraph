package graph_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func TestAdvanceRejectsEveryTerminalWithAndWithoutStore(t *testing.T) {
	for _, storage := range []string{"none", "memory", "sqlite"} {
		for _, outcome := range []string{"success", "local failure", "execution failure"} {
			t.Run(storage+"/"+outcome, func(t *testing.T) {
				ctx := context.Background()
				var store graph.Store[int]
				switch storage {
				case "memory":
					store = newMemoryStore(t)
				case "sqlite":
					s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "runs.db"), checkpoint.JSON[int]{})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = s.Close() })
					store = s
				}
				calls := 0
				g := graph.New[int]("work")
				scope := graph.FailInvocation
				if outcome == "execution failure" {
					scope = graph.FailExecution
				}
				if err := g.AddNode(graph.NodeSpec[int]{Name: "work", OnError: scope, Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					calls++
					if outcome != "success" {
						return graph.Transition[int]{}, errors.New(outcome)
					}
					return graph.EndExecution(s), nil
				}}); err != nil {
					t.Fatal(err)
				}
				runner := intRunner(t, g)
				e, first, err := runner.Start(ctx, "terminal", 7, store, graph.Options{})
				if (err != nil) != (outcome == "execution failure") || !first.Header.Completed {
					t.Fatalf("start = %+v, %v", first, err)
				}
				before := mustCheckpoint(e)
				for _, inputs := range [][]graph.ResumeInput{nil, {{InvocationID: "i1", CallID: "old", Payload: []byte("x")}}} {
					result, err := e.Advance(ctx, inputs, graph.Options{})
					if !errors.Is(err, graph.ErrRunCompleted) || result.Status != graph.StatusFailed || !reflect.DeepEqual(result.Header, first.Header) || !reflect.DeepEqual(result.Final, first.Final) || calls != 1 {
						t.Fatalf("advance terminal = %+v, %v calls=%d", result, err, calls)
					}
					if !reflect.DeepEqual(mustCheckpoint(e), before) {
						t.Fatal("terminal advance changed local position")
					}
				}
				if store != nil {
					loaded, err := store.Load(ctx, "terminal")
					if err != nil || !reflect.DeepEqual(loaded, before) {
						t.Fatalf("terminal changed in Store: %+v, %v", loaded, err)
					}
				}
			})
		}
	}
}

type cancellingLoadStore struct {
	graph.Store[int]
	cancel context.CancelFunc
	alter  func(*graph.Checkpoint[int])
}

func (s cancellingLoadStore) Load(ctx context.Context, id string) (graph.Checkpoint[int], error) {
	cp, err := s.Store.Load(ctx, id)
	if err == nil {
		if s.alter != nil {
			s.alter(&cp)
		}
		s.cancel()
	}
	return cp, err
}

func TestRecoverNeverExposesUnvalidatedHandleAfterLoadCancellation(t *testing.T) {
	g := graph.New[int]("loop")
	calls := 0
	node(t, g, "loop", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		calls++
		return graph.To(s+1, "loop"), nil
	})
	edge(t, g, "loop", "loop")
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	if _, _, err := runner.Start(context.Background(), "loaded", 0, store, graph.Options{MaxSteps: 1}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		alter func(*graph.Checkpoint[int])
	}{
		{"valid", nil},
		{"wrong run", func(cp *graph.Checkpoint[int]) { cp.RunID = "other" }},
		{"wrong machine", func(cp *graph.Checkpoint[int]) { cp.MachineID = "other" }},
		{"broken topology", func(cp *graph.Checkpoint[int]) { cp.Invocations[0].Node = "missing" }},
		{"format 7", func(cp *graph.Checkpoint[int]) { cp.FormatVersion = 7 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e, result, err := runner.Recover(ctx, "loaded", nil, cancellingLoadStore{Store: store, cancel: cancel, alter: tc.alter}, graph.Options{})
			if tc.alter == nil {
				if e == nil || !errors.Is(err, context.Canceled) || result.Status != graph.StatusCancelled {
					t.Fatalf("valid cancelled load: %+v %v", result, err)
				}
				if _, err := e.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			} else if e != nil || !errors.Is(err, graph.ErrInvalidCheckpoint) || result.Status != graph.StatusFailed {
				t.Fatalf("unvalidated load escaped: handle=%v result=%+v err=%v", e, result, err)
			}
			if calls != 1 {
				t.Fatal("recovery cancellation ran callbacks")
			}
		})
	}
}
