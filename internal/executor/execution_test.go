package executor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"lune-graph/internal/model"
)

func TestApplyTransitionActionsAndSubgraphReturn(t *testing.T) {
	nodes := []NodeSpec[int]{
		executorTestNode("source", executorTestEnd, FailInvocation),
		executorTestNode("next", executorTestEnd, FailInvocation),
		executorTestNode("done", executorTestEnd, FailInvocation),
	}
	continuations := map[string]Continuation[int]{"resume": {
		Decode: func([]byte) (any, error) { return 1, nil },
		Apply: func(_ context.Context, _ CallInfo, state int, value any) (int, error) {
			return state + value.(int), nil
		},
	}}
	makeRunner := func(returnTargets map[string]string) *Runner[int] {
		return executorTestRunner(t, "source", nodes, nil, map[string][]string{"source": {"next"}}, continuations, returnTargets)
	}

	t.Run("continue", func(t *testing.T) {
		runner := makeRunner(nil)
		checkpoint := executorTestReadyCheckpoint("source", 1)
		ended, err := runner.applyTransition(&checkpoint, "i1", model.To(7, "next"))
		if err != nil || ended || checkpoint.Invocations[0].Node != "next" || checkpoint.Invocations[0].State != 7 || checkpoint.Invocations[0].CallID != "c3" {
			t.Fatalf("continue = %+v, ended=%t err=%v", checkpoint, ended, err)
		}
	})

	t.Run("wait", func(t *testing.T) {
		runner := makeRunner(nil)
		checkpoint := executorTestReadyCheckpoint("source", 1)
		_, err := runner.applyTransition(&checkpoint, "i1", model.Wait(8, "resume", "next"))
		if err != nil || checkpoint.Invocations[0].Status != InvocationWaiting || checkpoint.Invocations[0].State != 8 || checkpoint.Invocations[0].Continuation != "resume" || !reflect.DeepEqual(checkpoint.Invocations[0].Next, []string{"next"}) {
			t.Fatalf("wait = %+v, %v", checkpoint, err)
		}
	})

	for name, transition := range map[string]Transition[int]{
		"no target":             model.To(0),
		"bad edge":              model.To(0, "missing"),
		"duplicate target":      model.To(0, "next", "next"),
		"unknown continuation":  model.Wait(0, "missing", "next"),
		"continue continuation": {Action: ActionContinue, Targets: []string{"next"}, Continuation: "resume"},
		"branch routing":        {Action: ActionEndBranch, Targets: []string{"next"}},
		"execution routing":     {Action: ActionEndExecution, Continuation: "resume"},
		"return routing":        {Action: ActionReturn, Targets: []string{"next"}},
		"invalid action":        {Action: Action(99)},
	} {
		t.Run(name, func(t *testing.T) {
			checkpoint := executorTestReadyCheckpoint("source", 1)
			if _, err := makeRunner(map[string]string{"source": "next"}).applyTransition(&checkpoint, "i1", transition); err == nil {
				t.Fatalf("invalid transition accepted: %+v", transition)
			}
		})
	}

	t.Run("end branch", func(t *testing.T) {
		checkpoint := executorTestReadyCheckpoint("source", 4)
		_, err := makeRunner(nil).applyTransition(&checkpoint, "i1", model.EndBranch(9))
		if err != nil || checkpoint.Invocations[0].Status != InvocationEnded || checkpoint.Invocations[0].CallID != "" || len(checkpoint.Terminals) != 1 || checkpoint.Terminals[0].State != 9 {
			t.Fatalf("EndBranch = %+v, %v", checkpoint, err)
		}
	})

	t.Run("end execution", func(t *testing.T) {
		checkpoint := executorTestReadyCheckpoint("source", 4)
		ended, err := makeRunner(nil).applyTransition(&checkpoint, "i1", model.EndExecution(11))
		if err != nil || !ended || !checkpoint.Completed || checkpoint.Final == nil || *checkpoint.Final != 11 || len(checkpoint.Invocations) != 0 {
			t.Fatalf("EndExecution = %+v, ended=%t err=%v", checkpoint, ended, err)
		}
	})

	t.Run("return with destination", func(t *testing.T) {
		checkpoint := executorTestReadyCheckpoint("source", 4)
		_, err := makeRunner(map[string]string{"source": "done"}).applyTransition(&checkpoint, "i1", model.Return(12))
		if err != nil || checkpoint.Invocations[0].Node != "done" || checkpoint.Invocations[0].State != 12 {
			t.Fatalf("Return route = %+v, %v", checkpoint, err)
		}
	})

	t.Run("return without destination", func(t *testing.T) {
		checkpoint := executorTestReadyCheckpoint("source", 4)
		_, err := makeRunner(map[string]string{"source": ""}).applyTransition(&checkpoint, "i1", model.Return(12))
		if err != nil || checkpoint.Invocations[0].Status != InvocationEnded || len(checkpoint.Terminals) != 1 || checkpoint.Terminals[0].State != 12 {
			t.Fatalf("terminal Return = %+v, %v", checkpoint, err)
		}
	})

	t.Run("return outside mount", func(t *testing.T) {
		checkpoint := executorTestReadyCheckpoint("source", 4)
		_, err := makeRunner(nil).applyTransition(&checkpoint, "i1", model.Return(12))
		var transitionErr *TransitionError
		if !errors.As(err, &transitionErr) || !strings.Contains(err.Error(), "outside a subgraph") {
			t.Fatalf("root Return error = %v", err)
		}
	})

	t.Run("missing invocation", func(t *testing.T) {
		checkpoint := executorTestReadyCheckpoint("source", 4)
		if _, err := makeRunner(nil).applyTransition(&checkpoint, "missing", model.Return(0)); err == nil {
			t.Fatal("transition accepted missing invocation")
		}
	})
}

func TestJoinTargetValidationAndRouting(t *testing.T) {
	join := JoinSpec[int]{Name: "joined", From: "fork", Merge: func(context.Context, CallInfo, []int) (int, error) { return 0, nil }}
	runner := executorTestRunner(t, "fork", []NodeSpec[int]{
		executorTestNode("fork", executorTestEnd, FailInvocation),
		executorTestNode("a", executorTestEnd, FailInvocation),
	}, []JoinSpec[int]{join}, map[string][]string{"fork": {"a", "joined"}, "a": {"joined"}}, nil, nil)
	checkpoint := Checkpoint[int]{Groups: []ActivationGroup{{ID: "g3", JoinNode: "joined"}}}
	inv := &Invocation[int]{ID: "i1", Node: "fork", GroupID: "g3"}
	if err := runner.checkJoinTargets(&checkpoint, inv, []string{"joined"}); err != nil {
		t.Fatalf("matching activation join rejected: %v", err)
	}
	if err := runner.checkJoinTargets(&checkpoint, inv, []string{"a", "joined"}); err != nil {
		t.Fatalf("source fan-out join rejected: %v", err)
	}
	inv.GroupID = "missing"
	if err := runner.checkJoinTargets(&checkpoint, inv, []string{"joined"}); err == nil {
		t.Fatal("join accepted invocation without its activation group")
	}
	inv.GroupID = "g3"
	checkpoint.Groups[0].JoinNode = "other"
	if err := runner.checkJoinTargets(&checkpoint, inv, []string{"joined"}); err == nil {
		t.Fatal("join accepted mismatched activation group")
	}

	for _, test := range []struct {
		name string
		inv  Invocation[int]
		cp   Checkpoint[int]
	}{
		{name: "outside group", inv: Invocation[int]{ID: "i1", Node: "fork"}},
		{name: "missing group", inv: Invocation[int]{ID: "i1", Node: "a", GroupID: "g99"}, cp: Checkpoint[int]{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := runner.setTarget(&test.cp, &test.inv, "joined"); err == nil {
				t.Fatal("invalid join target accepted")
			}
		})
	}
	wrongGroup := Checkpoint[int]{Groups: []ActivationGroup{{ID: "g3", JoinNode: "other"}}}
	wrongInv := Invocation[int]{ID: "i1", Node: "a", GroupID: "g3"}
	if err := runner.setTarget(&wrongGroup, &wrongInv, "joined"); err == nil {
		t.Fatal("join from another group accepted")
	}

	ready := executorTestReadyCheckpoint("fork", 1)
	ready.NextID = 3
	if err := runner.checkTargets("fork", nil); err == nil {
		t.Fatal("empty target list accepted")
	}
	if err := runner.checkTargets("fork", []string{"joined", "joined"}); err == nil {
		t.Fatal("duplicate target list accepted")
	}
}

func TestFanoutJoinAndFailureScopes(t *testing.T) {
	t.Run("merge and continue", func(t *testing.T) {
		runner := executorTestRunner(t, "fork", []NodeSpec[int]{
			executorTestNode("fork", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.To(value, "a", "b"), nil
			}, FailInvocation),
			executorTestNode("a", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.To(value+1, "joined"), nil
			}, FailInvocation),
			executorTestNode("b", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.To(value+2, "joined"), nil
			}, FailInvocation),
			executorTestNode("done", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.EndExecution(value), nil
			}, FailInvocation),
		}, []JoinSpec[int]{{Name: "joined", From: "fork", Merge: func(_ context.Context, call CallInfo, values []int) (int, error) {
			if call.CallID == "" || call.RunID != "parallel-join" || len(values) != 2 || values[0] != 1 || values[1] != 2 {
				return 0, fmt.Errorf("invalid merge call: %+v values=%v", call, values)
			}
			return values[0] + values[1], nil
		}}}, map[string][]string{"fork": {"a", "b"}, "a": {"joined"}, "b": {"joined"}, "joined": {"done"}}, nil, nil)
		result, err := runner.Start(context.Background(), "parallel-join", 0, Options[int]{MaxConcurrency: 1})
		if err != nil || result.Status != StatusCompleted || result.Checkpoint.Final == nil || *result.Checkpoint.Final != 3 {
			t.Fatalf("joined run = %+v, %v", result, err)
		}
	})

	t.Run("fanout without join ends parent branch", func(t *testing.T) {
		runner := executorTestRunner(t, "fork", []NodeSpec[int]{
			executorTestNode("fork", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.To(value, "a", "b"), nil
			}, FailInvocation),
			executorTestNode("a", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.EndBranch(value + 1), nil
			}, FailInvocation),
			executorTestNode("b", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
				return model.EndBranch(value + 2), nil
			}, FailInvocation),
		}, nil, map[string][]string{"fork": {"a", "b"}}, nil, nil)
		result, err := runner.Start(context.Background(), "no-join", 0, Options[int]{MaxConcurrency: 1})
		if err != nil || result.Status != StatusCompleted || len(result.Checkpoint.Terminals) != 2 {
			t.Fatalf("no-join run = %+v, %v", result, err)
		}
	})

	boom := errors.New("node failure")
	for _, scope := range []FailureScope{FailInvocation, FailGroup, FailExecution} {
		t.Run(fmt.Sprintf("failure scope %d", scope), func(t *testing.T) {
			runner := executorTestRunner(t, "fail", []NodeSpec[int]{executorTestNode("fail", func(context.Context, CallInfo, int) (Transition[int], error) { return Transition[int]{}, boom }, scope)}, nil, nil, nil, nil)
			result, err := runner.Start(context.Background(), fmt.Sprintf("failure-%d", scope), 0, Options[int]{})
			if scope == FailExecution || scope == FailGroup {
				if !errors.Is(err, boom) || result.Status != StatusFailed || !result.Checkpoint.Completed {
					t.Fatalf("terminal failure = %+v, %v", result, err)
				}
			} else if err != nil || result.Status != StatusCompletedWithFailures || len(result.Checkpoint.Failures) != 1 {
				t.Fatalf("local failure = %+v, %v", result, err)
			}
		})
	}

	mergeFailure := executorTestRunner(t, "fork", []NodeSpec[int]{
		executorTestNode("fork", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
			return model.To(value, "a", "b"), nil
		}, FailInvocation),
		executorTestNode("a", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
			return model.To(value, "joined"), nil
		}, FailInvocation),
		executorTestNode("b", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
			return model.To(value, "joined"), nil
		}, FailInvocation),
	}, []JoinSpec[int]{{Name: "joined", From: "fork", OnError: FailExecution, Merge: func(context.Context, CallInfo, []int) (int, error) { return 0, boom }}}, map[string][]string{"fork": {"a", "b"}, "a": {"joined"}, "b": {"joined"}}, nil, nil)
	result, err := mergeFailure.Start(context.Background(), "merge-failure", 0, Options[int]{MaxConcurrency: 1})
	if !errors.Is(err, boom) || result.Status != StatusFailed || result.Checkpoint.Failures[len(result.Checkpoint.Failures)-1].Scope != FailExecution {
		t.Fatalf("join failure = %+v, %v", result, err)
	}
}

func TestCheckpointsFailuresAndExecutionLimits(t *testing.T) {
	copyErr := errors.New("copy")
	runner := executorTestRunner(t, "source", []NodeSpec[int]{executorTestNode("source", executorTestEnd, FailInvocation)}, nil, nil, nil, nil)
	checkpoint := executorTestReadyCheckpoint("source", 1)
	if err := runner.checkTargets("source", []string{"next"}); err == nil {
		t.Fatal("unregistered edge accepted")
	}
	if err := runner.route(&checkpoint, "i99", "source", 1, []string{"source"}); err == nil {
		t.Fatal("route accepted missing invocation")
	}

	cloneFailure := executorTestRunner(t, "fork", []NodeSpec[int]{
		executorTestNode("fork", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
			return model.To(value, "a", "b"), nil
		}, FailInvocation),
		executorTestNode("a", executorTestEnd, FailInvocation),
		executorTestNode("b", executorTestEnd, FailInvocation),
	}, nil, map[string][]string{"fork": {"a", "b"}}, nil, nil)
	cloneFailure.clone = func(int) (int, error) { return 0, copyErr }
	checkpoint = executorTestReadyCheckpoint("fork", 1)
	err := cloneFailure.route(&checkpoint, "i1", "fork", 1, []string{"a", "b"})
	var stateErr *stateCopyError
	if !errors.As(err, &stateErr) || !errors.Is(err, copyErr) {
		t.Fatalf("fan-out clone error = %v", err)
	}

	checkpoint = executorTestReadyCheckpoint("source", 1)
	checkpoint.NextID = math.MaxUint64 - 1
	if err := runner.route(&checkpoint, "i1", "source", 1, []string{"source", "source2"}); !errors.Is(err, ErrExecutionLimit) {
		t.Fatalf("fan-out ID overflow = %v", err)
	}

	checkpoint = Checkpoint[int]{Invocations: []Invocation[int]{{ID: "i1", Node: "source", Status: InvocationWaiting}}}
	if err := runner.recordFailure(&checkpoint, "missing", "source", FailInvocation, copyErr); err == nil {
		t.Fatal("recordFailure accepted missing invocation")
	}
	checkpoint = executorTestReadyCheckpoint("source", 1)
	if err := runner.recordFailure(&checkpoint, "i1", "source", FailInvocation, copyErr); err != nil || checkpoint.Invocations[0].Status != InvocationFailed {
		t.Fatalf("local recordFailure = %+v, %v", checkpoint, err)
	}
	rootGroupFailure := executorTestReadyCheckpoint("source", 1)
	if err := runner.recordFailure(&rootGroupFailure, "i1", "source", FailGroup, copyErr); !errors.Is(err, copyErr) || !rootGroupFailure.Completed || rootGroupFailure.Failures[0].Scope != FailExecution {
		t.Fatalf("root FailGroup = %+v, %v", rootGroupFailure, err)
	}
	if !errors.Is(recoveredFailure(rootGroupFailure), ErrRunFailed) || recoveredFailure(Checkpoint[int]{}) != nil {
		t.Fatal("recovered failure mapping incorrect")
	}
}

func TestCallbackPanicWrappers(t *testing.T) {
	boom := errors.New("panic value")
	runner := executorTestRunner(t, "node", []NodeSpec[int]{executorTestNode("node", func(context.Context, CallInfo, int) (Transition[int], error) { panic(boom) }, FailInvocation)}, nil, nil, nil, nil)
	if _, err := callClone[int](func(int) (int, error) { panic(boom) }, 1, "i1"); !errors.Is(err, boom) {
		t.Fatalf("clone panic wrapper = %v", err)
	}
	if _, err := runner.runNode(context.Background(), CallInfo{InvocationID: "i1"}, runner.nodes["node"], 1); !errors.Is(err, boom) {
		t.Fatalf("node panic wrapper = %v", err)
	}
	if _, err := runner.mergeStates(context.Background(), CallInfo{InvocationID: "i1"}, JoinSpec[int]{Name: "join", Merge: func(context.Context, CallInfo, []int) (int, error) { panic(boom) }}, nil); !errors.Is(err, boom) {
		t.Fatalf("join panic wrapper = %v", err)
	}
	runner.continuations = map[string]Continuation[int]{"panic": {
		Decode: func([]byte) (any, error) { panic(boom) },
		Apply:  func(context.Context, CallInfo, int, any) (int, error) { panic(boom) },
	}}
	if _, err := runner.decodeInput("i1", "panic", nil); !errors.Is(err, boom) {
		t.Fatalf("decode panic wrapper = %v", err)
	}
	if _, err := runner.applyInput(context.Background(), CallInfo{InvocationID: "i1"}, "panic", 0, nil); !errors.Is(err, boom) {
		t.Fatalf("apply panic wrapper = %v", err)
	}
	if panicStack(boom) != "" || panicStack(&PanicError{Stack: []byte("stack")}) != "stack" {
		t.Fatal("panic stack extraction incorrect")
	}
}
