package executor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

func TestFailGroupRemovesNestedActivationTree(t *testing.T) {
	boom := errors.New("branch failed")
	runner := executorTestRunner(t, "root", []NodeSpec[int]{
		executorTestNode("root", func(_ context.Context, _ CallInfo, state int) (Transition[int], error) {
			return model.To(state, "nested", "killer"), nil
		}, FailInvocation),
		executorTestNode("nested", func(_ context.Context, _ CallInfo, state int) (Transition[int], error) {
			return model.To(state, "inner-a", "inner-b"), nil
		}, FailInvocation),
		executorTestNode("killer", func(context.Context, CallInfo, int) (Transition[int], error) {
			return Transition[int]{}, boom
		}, FailGroup),
		executorTestNode("inner-a", executorTestEnd, FailInvocation),
		executorTestNode("inner-b", executorTestEnd, FailInvocation),
	}, nil, map[string][]string{
		"root":   {"nested", "killer"},
		"nested": {"inner-a", "inner-b"},
	}, nil, nil)
	firstExecution, first, err := runner.Start(context.Background(), "nested-failure", 7, nil, Options{MaxSteps: 2, MaxConcurrency: 1})
	if err != nil || first.Status != StatusBudget || len(mustCheckpoint(firstExecution).Groups) != 2 {
		t.Fatalf("setup run = %+v, %v", first, err)
	}
	resultExecution, result, err := runner.Resume(context.Background(), mustCheckpoint(firstExecution), nil, nil, Options{MaxSteps: 1, MaxConcurrency: 1})
	if err != nil || result.Status != StatusCompletedWithFailures {
		t.Fatalf("FailGroup result = %+v, %v", result, err)
	}
	if !result.Header.Completed || !result.Header.HadLocalFailures || len(mustCheckpoint(resultExecution).Groups) != 0 {
		t.Fatalf("nested activation tree remains: %+v", mustCheckpoint(resultExecution))
	}
	if len(mustCheckpoint(resultExecution).Invocations) != 0 {
		t.Fatalf("completed local failure retained invocations: %+v", mustCheckpoint(resultExecution).Invocations)
	}
}

func TestForkCopiesWaitingCheckpointAndReportsCloneFailure(t *testing.T) {
	runner := executorTestRunner(t, "wait", []NodeSpec[int]{
		executorTestNode("wait", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
			return model.Wait(value, "resume", "finish"), nil
		}, FailInvocation),
		executorTestNode("finish", executorTestEnd, FailInvocation),
	}, nil, map[string][]string{"wait": {"finish"}}, map[string]Continuation[int]{
		"resume": {
			Decode: func(payload []byte) (any, error) { return string(payload), nil },
			Apply:  func(_ context.Context, _ CallInfo, state int, _ any) (int, error) { return state + 1, nil },
		},
	}, nil)
	sourceExecution, source, err := runner.Start(context.Background(), "fork-source", 9, nil, Options{})
	if err != nil || source.Status != StatusWaiting {
		t.Fatalf("source = %+v, %v", source, err)
	}
	sourceCheckpoint := mustCheckpoint(sourceExecution)
	forked, err := runner.Fork(context.Background(), "fork-copy", sourceCheckpoint, nil)
	if err != nil || forked.RunID != "fork-copy" || forked.Revision != 1 {
		t.Fatalf("fork = %+v, %v", forked, err)
	}
	if forked.Invocations[0].Status != InvocationWaiting ||
		forked.Invocations[0].CallID != sourceCheckpoint.Invocations[0].CallID ||
		source.Header.RunID != "fork-source" || source.Header.Revision != 2 {
		t.Fatalf("fork/source position changed: fork=%+v source=%+v", forked, mustCheckpoint(sourceExecution))
	}
	_, resumed, err := runner.Resume(context.Background(), forked, []ResumeInput{{
		InvocationID: forked.Invocations[0].ID,
		CallID:       forked.Invocations[0].CallID,
		Payload:      []byte("continue"),
	}}, nil, Options{})
	if err != nil || resumed.Status != StatusCompleted || resumed.Final == nil || *resumed.Final != 10 {
		t.Fatalf("resume in-memory fork = %+v, %v", resumed, err)
	}

	cloneErr := errors.New("clone failed")
	runner.clone = func(int) (int, error) { return 0, cloneErr }
	failed, err := runner.Fork(context.Background(), "fork-failed", sourceCheckpoint, nil)
	if !errors.Is(err, cloneErr) || failed.RunID != "" {
		t.Fatalf("fork clone failure = %+v, %v", failed, err)
	}
}

func TestObservedCallbacksReportSuccessAndFailure(t *testing.T) {
	boom := errors.New("callback failed")
	runner := executorTestRunner(t, "node", []NodeSpec[int]{
		executorTestNode("node", func(_ context.Context, _ CallInfo, state int) (Transition[int], error) {
			return model.EndExecution(state), nil
		}, FailInvocation),
	}, []JoinSpec[int]{
		{Name: "join", From: "fork", Merge: func(_ context.Context, _ CallInfo, _ int, branches []BranchResult[int]) (int, error) {
			return branches[0].State, nil
		}},
	}, nil, map[string]Continuation[int]{
		"resume": {
			Decode: func(payload []byte) (any, error) {
				if string(payload) == "bad" {
					return nil, boom
				}
				return 3, nil
			},
			Apply: func(_ context.Context, _ CallInfo, state int, value any) (int, error) {
				if value == "bad" {
					return state, boom
				}
				return state + value.(int), nil
			},
		},
	}, nil)

	var events []model.Event
	newSession := func() *observation.Session {
		events = nil
		return observation.New(model.ObserverFunc(func(_ context.Context, event model.Event) {
			events = append(events, event)
		}), runner.id, "observed-run")
	}
	checkEvents := func(operation model.EventOperation, wantErr error) {
		t.Helper()
		if len(events) != 2 || events[0].Operation != operation || events[0].Phase != model.PhaseStarted || events[1].Phase != model.PhaseFinished {
			t.Fatalf("events = %+v", events)
		}
		if !errors.Is(events[1].Err, wantErr) {
			t.Fatalf("finished event error = %v, want %v", events[1].Err, wantErr)
		}
	}

	call := CallInfo{RunID: "observed-run", InvocationID: "i1", CallID: "c2"}
	_, err := runner.observedNode(context.Background(), newSession(), 4, call, runner.nodes["node"], 5)
	if err != nil {
		t.Fatal(err)
	}
	checkEvents(model.OperationNode, nil)
	if events[0].CallID != "c2" || events[1].Action != ActionEndExecution {
		t.Fatalf("node event correlation = %+v", events)
	}

	merge := runner.joins["join"]
	if _, err := runner.observedMerge(context.Background(), newSession(), 5, call, merge, 5, []BranchResult[int]{{State: 6, Status: InvocationJoined}}); err != nil {
		t.Fatal(err)
	}
	checkEvents(model.OperationJoin, nil)
	merge.Merge = func(context.Context, CallInfo, int, []BranchResult[int]) (int, error) { return 0, boom }
	if _, err := runner.observedMerge(context.Background(), newSession(), 5, call, merge, 5, []BranchResult[int]{{State: 6, Status: InvocationJoined}}); !errors.Is(err, boom) {
		t.Fatalf("merge error = %v", err)
	}
	checkEvents(model.OperationJoin, boom)

	inv := Invocation[int]{ID: "i1", Node: "node", Continuation: "resume"}
	if _, err := runner.observedDecode(context.Background(), newSession(), 6, inv, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	checkEvents(model.OperationDecode, nil)
	if _, err := runner.observedDecode(context.Background(), newSession(), 6, inv, []byte("bad")); !errors.Is(err, boom) {
		t.Fatalf("decode error = %v", err)
	}
	checkEvents(model.OperationDecode, boom)

	if _, err := runner.observedApply(context.Background(), newSession(), 7, call, inv, 4, 3); err != nil {
		t.Fatal(err)
	}
	checkEvents(model.OperationApply, nil)
	if _, err := runner.observedApply(context.Background(), newSession(), 7, call, inv, 4, "bad"); !errors.Is(err, boom) {
		t.Fatalf("apply error = %v", err)
	}
	checkEvents(model.OperationApply, boom)
}

func TestMachineExportMermaidAndWideTargetValidation(t *testing.T) {
	var targets []string
	edges := map[string][]string{}
	nodes := []NodeSpec[int]{executorTestNode("root", executorTestEnd, FailInvocation)}
	for i := range 9 {
		name := "target-" + string(rune('a'+i))
		targets = append(targets, name)
		edges["root"] = append(edges["root"], name)
		nodes = append(nodes, executorTestNode(name, executorTestEnd, FailInvocation))
	}
	runner := executorTestRunner(t, "root", nodes, []JoinSpec[int]{
		{Name: "join", From: "root", Merge: func(_ context.Context, _ CallInfo, _ int, branches []BranchResult[int]) (int, error) {
			return branches[0].State, nil
		}},
	}, edges, nil, map[string]string{"child/return": "target-a"})
	if err := runner.checkTargets("root", targets); err != nil {
		t.Fatalf("valid wide target list: %v", err)
	}
	duplicate := append(append([]string(nil), targets...), targets[0])
	if err := runner.checkTargets("root", duplicate); err == nil || !strings.Contains(err.Error(), "duplicate target") {
		t.Fatalf("wide duplicate targets: %v", err)
	}
	if err := runner.checkTargets("root", append(append([]string(nil), targets...), "missing")); err == nil || !strings.Contains(err.Error(), "unknown edge") {
		t.Fatalf("wide unknown target: %v", err)
	}

	diagram := runner.ExportMermaid()
	if !strings.Contains(diagram, `(["root (entry)"])`) || !strings.Contains(diagram, "-.->|return|") || !strings.Contains(diagram, `{{"join (join)"}}`) {
		t.Fatal(diagram)
	}

	var nilRunner *Runner[int]
	if nilRunner.ExportMermaid() != "" {
		t.Fatal("nil runner exported a diagram")
	}
}

func TestDynamicFanOutAndExecutionHandleCoverage(t *testing.T) {
	t.Run("dynamic fanout and complete branch", func(t *testing.T) {
		runner := executorTestRunner(t, "fork", []NodeSpec[int]{
			executorTestNode("fork", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.FanOut(value,
					Branch[int]{Target: "a", State: 10},
					Branch[int]{Target: "b", State: 20},
				), nil
			}, FailInvocation),
			executorTestNode("a", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.CompleteBranch(value + 1), nil
			}, FailInvocation),
			executorTestNode("b", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.CompleteBranch(value + 2), nil
			}, FailInvocation),
			executorTestNode("done", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.EndExecution(value), nil
			}, FailInvocation),
		}, []JoinSpec[int]{{Name: "joined", From: "fork", Merge: func(_ context.Context, _ CallInfo, parent int, branches []BranchResult[int]) (int, error) {
			if len(branches) != 2 || branches[0].State != 11 || branches[1].State != 22 {
				return 0, errors.New("unexpected branches")
			}
			return parent + branches[0].State + branches[1].State, nil
		}}}, map[string][]string{"fork": {"a", "b"}, "joined": {"done"}}, nil, nil)

		_, res, err := runner.Start(context.Background(), "fanout-test", 100, nil, Options{MaxConcurrency: 2})
		if err != nil || res.Status != StatusCompleted || res.Final == nil || *res.Final != 133 {
			t.Fatalf("dynamic fanout failed: %+v, %v", res, err)
		}
	})

	t.Run("dynamic fanout error paths", func(t *testing.T) {
		noJoinRunner := executorTestRunner(t, "fork", []NodeSpec[int]{
			executorTestNode("fork", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.FanOut(value, Branch[int]{Target: "a", State: 10}), nil
			}, FailInvocation),
			executorTestNode("a", executorTestEnd, FailInvocation),
		}, nil, map[string][]string{"fork": {"a"}}, nil, nil)
		_, noJoinRes, err := noJoinRunner.Start(context.Background(), "no-join", 0, nil, Options{})
		if err == nil || noJoinRes.Status != StatusFailed {
			t.Fatalf("expected failure without join: %+v, %v", noJoinRes, err)
		}

		negativeTimeoutRunner := executorTestRunner(t, "fork", []NodeSpec[int]{
			executorTestNode("fork", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.FanOut(value, Branch[int]{Target: "a", State: 10, EntryTimeout: -1}), nil
			}, FailInvocation),
			executorTestNode("a", executorTestEnd, FailInvocation),
		}, []JoinSpec[int]{{Name: "joined", From: "fork", Merge: func(_ context.Context, _ CallInfo, _ int, _ []BranchResult[int]) (int, error) { return 0, nil }}}, map[string][]string{"fork": {"a"}}, nil, nil)
		_, negativeRes, err := negativeTimeoutRunner.Start(context.Background(), "negative-timeout", 0, nil, Options{})
		if err == nil || negativeRes.Status != StatusFailed {
			t.Fatalf("expected failure with negative branch timeout: %+v, %v", negativeRes, err)
		}
	})

	t.Run("execution handle pending advance and nil methods", func(t *testing.T) {
		var nilExec *Execution[int]
		if nilExec.Pending() != nil {
			t.Fatal("expected nil Pending")
		}
		if _, err := nilExec.Checkpoint(); err == nil {
			t.Fatal("expected error on nil Checkpoint")
		}
		if _, err := nilExec.Advance(context.Background(), nil, Options{}); err == nil {
			t.Fatal("expected error on nil Advance")
		}
		if _, err := (&Execution[int]{}).Advance(nil, nil, Options{}); err == nil {
			t.Fatal("expected context error on Advance")
		}

		runner := executorTestRunner(t, "wait", []NodeSpec[int]{
			executorTestNode("wait", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.Wait(value, "input", "done"), nil
			}, FailInvocation),
			executorTestNode("done", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.EndExecution(value), nil
			}, FailInvocation),
		}, nil, map[string][]string{"wait": {"done"}}, map[string]Continuation[int]{
			"input": {
				Decode: func(payload []byte) (any, error) { return string(payload), nil },
				Apply: func(_ context.Context, _ CallInfo, state int, payload any) (int, error) {
					return state + len(payload.(string)), nil
				},
			},
		}, nil)

		exec, waitRes, err := runner.Start(context.Background(), "handle-test", 10, nil, Options{})
		if err != nil || waitRes.Status != StatusWaiting {
			t.Fatalf("Start wait failed: %+v, %v", waitRes, err)
		}
		pending := exec.Pending()
		if len(pending) != 1 || pending[0].Continuation != "input" {
			t.Fatalf("unexpected pending: %+v", pending)
		}
		cp, err := exec.Checkpoint()
		if err != nil || cp.RunID != "handle-test" {
			t.Fatalf("Checkpoint failed: %+v, %v", cp, err)
		}

		// Advance execution with invalid options
		if _, err := exec.Advance(context.Background(), nil, Options{MaxSteps: -1}); err == nil {
			t.Fatal("expected error on invalid options")
		}

		// Advance execution to completion
		advRes, err := exec.Advance(context.Background(), []ResumeInput{{
			InvocationID: pending[0].InvocationID,
			CallID:       pending[0].CallID,
			Payload:      []byte("hello"),
		}}, Options{})
		if err != nil || advRes.Status != StatusCompleted || advRes.Final == nil || *advRes.Final != 15 {
			t.Fatalf("Advance failed: %+v, %v", advRes, err)
		}

		// Test runner.scope
		override := FailExecution
		if runner.scope(FailInvocation, Options{FailureOverride: &override}) != FailExecution {
			t.Fatal("expected FailExecution override")
		}
		if runner.scope(FailInvocation, Options{}) != FailInvocation {
			t.Fatal("expected declared FailInvocation")
		}
	})
}
