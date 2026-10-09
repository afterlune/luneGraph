package graph_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestContinuationInterruptionPreservesCommittedPrefix(t *testing.T) {
	for _, scope := range []graph.FailureScope{graph.FailInvocation, graph.FailGroup, graph.FailExecution} {
		t.Run(fmt.Sprint(scope), func(t *testing.T) {
			g := graph.New[int]("fork")
			node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.EndBranch[int](), nil
			})
			node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.To(s, "a", "b", "c"), nil
			})
			for _, name := range []string{"a", "b", "c"} {
				if err := g.AddNode(graph.NodeSpec[int]{Name: name, OnError: scope, Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					return graph.Wait(s, "input", "done"), nil
				}}); err != nil {
					t.Fatal(err)
				}
				edge(t, g, "fork", name)
				edge(t, g, name, "done")
			}
			var calls []graph.CallInfo
			interrupted := false
			if err := graph.RegisterJSONContinuation(g, "input", func(_ context.Context, c graph.CallInfo, s, p int) (int, error) {
				calls = append(calls, c)
				if c.Node == "b" && !interrupted {
					interrupted = true
					return 99, graph.Interrupt(nil)
				}
				return s + p, nil
			}); err != nil {
				t.Fatal(err)
			}
			r := intRunner(t, g)
			store := &interruptionStore{Store: newMemoryStore(t)}
			override := graph.FailExecution
			opts := graph.Options[int]{Store: store, MaxConcurrency: 1, FailureOverride: &override}
			first, err := r.Start(context.Background(), "inputs", 0, opts)
			if err != nil || first.Status != graph.StatusWaiting {
				t.Fatalf("first=%+v %v", first, err)
			}
			var inputs []graph.ResumeInput
			var pendingID string
			for _, inv := range first.Checkpoint.Invocations {
				if inv.Status == graph.InvocationWaiting {
					inputs = append(inputs, graph.ResumeInput{InvocationID: inv.ID, Payload: []byte("2")})
					if inv.Node == "b" {
						pendingID = inv.CallID
					}
				}
			}
			beforeCommits := store.commits
			out, err := r.Resume(context.Background(), first.Checkpoint, inputs, opts)
			if !errors.Is(err, graph.ErrInterrupted) || out.Status != graph.StatusInterrupted || out.Checkpoint.Revision != first.Checkpoint.Revision+1 || out.Checkpoint.Steps != first.Checkpoint.Steps || store.commits != beforeCommits+1 || len(calls) != 2 {
				t.Fatalf("out=%+v calls=%+v %v", out, calls, err)
			}
			var remaining []graph.ResumeInput
			for _, inv := range out.Checkpoint.Invocations {
				if inv.Status == graph.InvocationWaiting {
					remaining = append(remaining, graph.ResumeInput{InvocationID: inv.ID, Payload: []byte("2")})
					if inv.Node == "b" && (inv.CallID != pendingID || inv.State != 0) {
						t.Fatalf("pending=%+v", inv)
					}
				}
			}
			if len(remaining) != 2 || out.Checkpoint.HadLocalFailures || out.Checkpoint.Failure != nil {
				t.Fatalf("pending=%+v", out)
			}
			waiting, err := r.Recover(context.Background(), "inputs", nil, opts)
			if err != nil || waiting.Status != graph.StatusWaiting || len(calls) != 2 {
				t.Fatalf("input unexpectedly replayed: %+v %v", waiting, err)
			}
			done, err := r.Recover(context.Background(), "inputs", remaining, opts)
			if err != nil || done.Status != graph.StatusCompleted || len(calls) != 4 || calls[1].CallID != calls[2].CallID {
				t.Fatalf("done=%+v calls=%+v %v", done, calls, err)
			}
		})
	}
}

func TestJoinInterruptionRollsBackCandidate(t *testing.T) {
	for _, fromInput := range []bool{false, true} {
		for _, nested := range []bool{false, true} {
			for _, scope := range []graph.FailureScope{graph.FailInvocation, graph.FailGroup, graph.FailExecution} {
				t.Run(fmt.Sprintf("input=%v/nested=%v/scope=%d", fromInput, nested, scope), func(t *testing.T) {
					g := graph.New[int]("fork")
					node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
						return graph.To(s, "a", "b"), nil
					})
					var triggerCalls []graph.CallInfo
					node(t, g, "a", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
						return graph.To(s+1, "outer"), nil
					})
					if nested {
						node(t, g, "b", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
							return graph.To(s, "x", "y"), nil
						})
					}
					targets := []string{"b"}
					joinName := "outer"
					if nested {
						targets = []string{"x", "y"}
						joinName = "inner"
					}
					for _, name := range targets {
						node(t, g, name, func(_ context.Context, c graph.CallInfo, s int) (graph.Transition[int], error) {
							if fromInput {
								return graph.Wait(s, "input", joinName), nil
							}
							triggerCalls = append(triggerCalls, c)
							return graph.To(s+2, joinName), nil
						})
						if nested {
							edge(t, g, "b", name)
						}
					}
					if err := graph.RegisterJSONContinuation(g, "input", func(_ context.Context, c graph.CallInfo, s, p int) (int, error) {
						triggerCalls = append(triggerCalls, c)
						return s + p, nil
					}); err != nil {
						t.Fatal(err)
					}
					var innerCalls, outerCalls []graph.CallInfo
					if nested {
						if err := g.AddJoin(graph.JoinSpec[int]{Name: "inner", From: "b", Merge: func(_ context.Context, c graph.CallInfo, values []int) (int, error) {
							innerCalls = append(innerCalls, c)
							return values[0] + values[1], nil
						}}); err != nil {
							t.Fatal(err)
						}
					}
					if err := g.AddJoin(graph.JoinSpec[int]{Name: "outer", From: "fork", OnError: scope, Merge: func(_ context.Context, c graph.CallInfo, values []int) (int, error) {
						outerCalls = append(outerCalls, c)
						if len(outerCalls) == 1 {
							return 99, fmt.Errorf("merge: %w", graph.Interrupt(nil))
						}
						return values[0] + values[1], nil
					}}); err != nil {
						t.Fatal(err)
					}
					for _, name := range targets {
						edge(t, g, name, joinName)
					}
					if nested {
						edge(t, g, "inner", "outer")
					}
					edge(t, g, "fork", "a")
					edge(t, g, "fork", "b")
					edge(t, g, "a", "outer")
					node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
						return graph.EndExecution(s), nil
					})
					edge(t, g, "outer", "done")
					r := intRunner(t, g)
					store := &interruptionStore{Store: newMemoryStore(t)}
					override := graph.FailExecution
					opts := graph.Options[int]{Store: store, MaxConcurrency: 1, FailureOverride: &override}
					out, err := r.Start(context.Background(), "joins", 0, opts)
					if fromInput {
						if err != nil || out.Status != graph.StatusWaiting {
							t.Fatalf("start=%+v %v", out, err)
						}
						var inputs []graph.ResumeInput
						for _, inv := range out.Checkpoint.Invocations {
							if inv.Status == graph.InvocationWaiting {
								inputs = append(inputs, graph.ResumeInput{InvocationID: inv.ID, Payload: []byte("2")})
							}
						}
						out, err = r.Resume(context.Background(), out.Checkpoint, inputs, opts)
					}
					if !errors.Is(err, graph.ErrInterrupted) || out.Status != graph.StatusInterrupted || out.Checkpoint.Completed || out.Checkpoint.Failure != nil || out.Checkpoint.HadLocalFailures || len(outerCalls) != 1 {
						t.Fatalf("out=%+v %v", out, err)
					}
					saved, err := store.Load(context.Background(), "joins")
					if err != nil || !reflect.DeepEqual(out.Checkpoint, saved) {
						t.Fatalf("saved=%+v %v", saved, err)
					}
					var remaining []graph.ResumeInput
					for _, inv := range saved.Invocations {
						if inv.Status == graph.InvocationWaiting {
							remaining = append(remaining, graph.ResumeInput{InvocationID: inv.ID, Payload: []byte("2")})
						}
					}
					lastTrigger := triggerCalls[len(triggerCalls)-1]
					done, err := r.Recover(context.Background(), "joins", remaining, opts)
					want := 3
					if nested {
						want = 5
					}
					if err != nil || done.Status != graph.StatusCompleted || *done.Checkpoint.Final != want || len(outerCalls) != 2 || outerCalls[0] != outerCalls[1] || lastTrigger != triggerCalls[len(triggerCalls)-1] {
						t.Fatalf("done=%+v triggers=%+v outer=%+v %v", done, triggerCalls, outerCalls, err)
					}
					if nested && (len(innerCalls) != 2 || innerCalls[0] != innerCalls[1]) {
						t.Fatalf("inner identity=%+v", innerCalls)
					}
				})
			}
		}
	}
}
