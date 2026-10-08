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

	first, err := runner.Start(context.Background(), "nested-failure", 7, Options[int]{MaxSteps: 2, MaxConcurrency: 1})
	if err != nil || first.Status != StatusBudget || len(first.Checkpoint.Groups) != 2 {
		t.Fatalf("setup run = %+v, %v", first, err)
	}
	result, err := runner.Resume(context.Background(), first.Checkpoint, nil, Options[int]{MaxSteps: 1, MaxConcurrency: 1})
	if err != nil || result.Status != StatusCompletedWithFailures {
		t.Fatalf("FailGroup result = %+v, %v", result, err)
	}
	if !result.Checkpoint.Completed || !result.Checkpoint.HadLocalFailures || len(result.Checkpoint.Groups) != 0 {
		t.Fatalf("nested activation tree remains: %+v", result.Checkpoint)
	}
	if len(result.Checkpoint.Invocations) != 0 {
		t.Fatalf("completed local failure retained invocations: %+v", result.Checkpoint.Invocations)
	}
}

func TestForkCopiesWaitingExecutionAndReportsCloneFailure(t *testing.T) {
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
	source, err := runner.Start(context.Background(), "fork-source", 9, Options[int]{})
	if err != nil || source.Status != StatusWaiting {
		t.Fatalf("source = %+v, %v", source, err)
	}

	forked, err := runner.Fork(context.Background(), "fork-copy", source.Checkpoint, Options[int]{})
	if err != nil || forked.Status != StatusWaiting {
		t.Fatalf("fork = %+v, %v", forked, err)
	}
	if forked.Checkpoint.RunID != "fork-copy" || forked.Checkpoint.Revision != 1 || source.Checkpoint.RunID != "fork-source" || source.Checkpoint.Revision != 2 {
		t.Fatalf("fork/source identity or revision changed: fork=%+v source=%+v", forked.Checkpoint, source.Checkpoint)
	}

	cloneErr := errors.New("clone failed")
	runner.clone = func(int) (int, error) { return 0, cloneErr }
	failed, err := runner.Fork(context.Background(), "fork-failed", source.Checkpoint, Options[int]{})
	if !errors.Is(err, cloneErr) || failed.Status != StatusFailed {
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
		{Name: "join", From: "fork", Merge: func(_ context.Context, _ CallInfo, states []int) (int, error) { return states[0], nil }},
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
	if _, err := runner.observedMerge(context.Background(), newSession(), 5, call, merge, []int{6}); err != nil {
		t.Fatal(err)
	}
	checkEvents(model.OperationJoin, nil)
	merge.Merge = func(context.Context, CallInfo, []int) (int, error) { return 0, boom }
	if _, err := runner.observedMerge(context.Background(), newSession(), 5, call, merge, []int{6}); !errors.Is(err, boom) {
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
		{Name: "join", From: "root", Merge: func(_ context.Context, _ CallInfo, states []int) (int, error) { return states[0], nil }},
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
	if !strings.Contains(diagram, "root([\"root (entry)\"])") || !strings.Contains(diagram, "child_return -.->|return| target_a") || !strings.Contains(diagram, "join{{\"join (join)\"}}") {
		t.Fatalf("compiled Mermaid output missing topology: %s", diagram)
	}
	var nilRunner *Runner[int]
	if nilRunner.ExportMermaid() != "" {
		t.Fatal("nil runner exported a diagram")
	}
}
