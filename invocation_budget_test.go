package graph_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestMaxInvocationsPausesStaticAndDynamicFanOut(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		name := "static"
		if dynamic {
			name = "dynamic"
		}
		t.Run(name, func(t *testing.T) {
			var cloneCalls atomic.Int64
			var forkCalls []string
			g := graph.New[int]("fork")
			node(t, g, "fork", func(_ context.Context, call graph.CallInfo, state int) (graph.Transition[int], error) {
				forkCalls = append(forkCalls, call.CallID)
				if dynamic {
					return graph.FanOut(state,
						graph.Branch[int]{Target: "left", State: state},
						graph.Branch[int]{Target: "right", State: state},
					), nil
				}
				return graph.To(state, "left", "right"), nil
			})
			node(t, g, "left", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
				return graph.CompleteBranch(state + 1), nil
			})
			node(t, g, "right", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
				return graph.CompleteBranch(state + 1), nil
			})
			if err := g.AddJoin(graph.JoinSpec[int]{
				Name: "joined",
				From: "fork",
				Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
					for _, branch := range branches {
						if !branch.OK() {
							return parent, errors.New("branch did not complete")
						}
						parent += branch.State
					}
					return parent, nil
				},
			}); err != nil {
				t.Fatal(err)
			}
			node(t, g, "finish", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
				return graph.EndExecution(state), nil
			})
			edge(t, g, "fork", "left")
			edge(t, g, "fork", "right")
			edge(t, g, "joined", "finish")
			runner, err := g.Compile(graph.Config[int]{
				MachineID: "invocation-budget-" + name,
				Clone: func(state int) (int, error) {
					cloneCalls.Add(1)
					return state, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			store := newMemoryStore(t)
			execution, paused, err := runner.Start(context.Background(), "budget-"+name, 0, store, graph.Options{
				MaxInvocations: 1,
				MaxConcurrency: 2,
			})
			if err != nil || paused.Status != graph.StatusBudget || paused.Header.Revision != 1 || paused.Header.Steps != 0 {
				t.Fatalf("start at limit = %+v, %v", paused, err)
			}
			if got := cloneCalls.Load(); got != 2 {
				t.Fatalf("clone calls after rejected fan-out = %d, want initial and callback-state clones only", got)
			}
			checkpoint, err := execution.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			if len(checkpoint.Invocations) != 1 || len(checkpoint.Groups) != 0 || checkpoint.Invocations[0].Status != graph.InvocationReady || checkpoint.Invocations[0].CallID != "c2" {
				t.Fatalf("budget pause changed checkpoint position: %+v", checkpoint)
			}
			persisted, err := store.Load(context.Background(), "budget-"+name)
			if err != nil || persisted.Revision != 1 || len(persisted.Invocations) != 1 || len(persisted.Groups) != 0 {
				t.Fatalf("persisted paused checkpoint = %+v, %v", persisted, err)
			}

			completed, err := execution.Advance(context.Background(), nil, graph.Options{MaxInvocations: 3, MaxConcurrency: 2})
			if err != nil || completed.Status != graph.StatusCompleted || completed.Final == nil || *completed.Final != 2 {
				t.Fatalf("advance with larger limit = %+v, %v", completed, err)
			}
			if len(forkCalls) != 2 || forkCalls[0] != "c2" || forkCalls[1] != forkCalls[0] {
				t.Fatalf("fork CallIDs after replay = %v", forkCalls)
			}
		})
	}
}

func TestMaxInvocationsAllowsExistingWorkToDrainAboveLoweredLimit(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.FanOut(state,
			graph.Branch[int]{Target: "worker", State: state},
			graph.Branch[int]{Target: "worker", State: state},
		), nil
	})
	node(t, g, "worker", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.CompleteBranch(state + 1), nil
	})
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "joined", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
		for _, branch := range branches {
			parent += branch.State
		}
		return parent, nil
	}}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "finish", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, g, "fork", "worker")
	edge(t, g, "joined", "finish")
	runner := intRunner(t, g)
	execution, paused, err := runner.Start(context.Background(), "lowered-budget", 0, nil, graph.Options{MaxSteps: 1, MaxInvocations: 3})
	if err != nil || paused.Status != graph.StatusBudget {
		t.Fatalf("start = %+v, %v", paused, err)
	}
	checkpoint, err := execution.Checkpoint()
	if err != nil || len(checkpoint.Invocations) != 3 {
		t.Fatalf("fan-out checkpoint = %+v, %v", checkpoint, err)
	}
	completed, err := execution.Advance(context.Background(), nil, graph.Options{MaxInvocations: 1})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Final == nil || *completed.Final != 2 {
		t.Fatalf("drain above lowered limit = %+v, %v", completed, err)
	}
}

func TestMaxInvocationsOptionRejectsNegativeValue(t *testing.T) {
	g := graph.New[int]("done")
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	runner := intRunner(t, g)
	if _, result, err := runner.Start(context.Background(), "negative-invocation-budget", 0, nil, graph.Options{MaxInvocations: -1}); err == nil || result.Status != graph.StatusFailed {
		t.Fatalf("negative invocation limit = %+v, %v", result, err)
	}
}

func TestContinuationFanOutBudgetRetainsOriginalWait(t *testing.T) {
	var applyCallIDs []string
	g := graph.New[int]("wait")
	node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.Wait(state, "resume", "left", "right"), nil
	})
	node(t, g, "left", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.CompleteBranch(state), nil
	})
	node(t, g, "right", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.CompleteBranch(state), nil
	})
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "joined", From: "wait", Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
		for _, branch := range branches {
			parent += branch.State
		}
		return parent, nil
	}}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "finish", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
		return graph.EndExecution(state), nil
	})
	edge(t, g, "wait", "left")
	edge(t, g, "wait", "right")
	edge(t, g, "joined", "finish")
	if err := graph.RegisterJSONContinuation(g, "resume", func(_ context.Context, call graph.CallInfo, state, input int) (int, error) {
		applyCallIDs = append(applyCallIDs, call.CallID)
		return state + input, nil
	}); err != nil {
		t.Fatal(err)
	}
	runner := intRunner(t, g)
	store := newMemoryStore(t)
	execution, waiting, err := runner.Start(context.Background(), "wait-invocation-budget", 0, store, graph.Options{})
	if err != nil || waiting.Status != graph.StatusWaiting || len(execution.Pending()) != 1 {
		t.Fatalf("start waiting = %+v, %v", waiting, err)
	}
	input := graph.ResumeInput{InvocationID: execution.Pending()[0].InvocationID, CallID: execution.Pending()[0].CallID, Payload: []byte("4")}
	paused, err := execution.Advance(context.Background(), []graph.ResumeInput{input}, graph.Options{MaxInvocations: 1})
	if err != nil || paused.Status != graph.StatusBudget || paused.Header.Revision != waiting.Header.Revision {
		t.Fatalf("advance at invocation limit = %+v, %v", paused, err)
	}
	pending := execution.Pending()
	if len(pending) != 1 || pending[0].CallID != input.CallID {
		t.Fatalf("wait identity after rejected continuation = %+v", pending)
	}
	completed, err := execution.Advance(context.Background(), []graph.ResumeInput{input}, graph.Options{MaxInvocations: 3})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Final == nil || *completed.Final != 12 {
		t.Fatalf("advance with larger limit = %+v final=%v, %v", completed, completed.Final, err)
	}
	if len(applyCallIDs) != 2 || applyCallIDs[0] != input.CallID || applyCallIDs[1] != input.CallID {
		t.Fatalf("continuation CallIDs after replay = %v", applyCallIDs)
	}
}
