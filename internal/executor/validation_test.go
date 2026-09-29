package executor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"lune-graph/internal/model"
)

func executorTestValidationFixtures(t *testing.T) (waitingRunner *Runner[int], waiting Checkpoint[int], fanoutRunner *Runner[int], fanout Checkpoint[int], completed Checkpoint[int], failed Checkpoint[int]) {
	t.Helper()
	waitingRunner = executorTestWaitingRunner(t, nil)
	waiting = executorTestWaitingCheckpoint(t, waitingRunner)

	fanoutRunner = executorTestRunner(t, "fork", []NodeSpec[int]{
		executorTestNode("fork", func(_ context.Context, _ CallInfo, state int) (Transition[int], error) {
			return model.To(state, "left", "right"), nil
		}, FailInvocation),
		executorTestNode("left", executorTestEnd, FailInvocation),
		executorTestNode("right", executorTestEnd, FailInvocation),
	}, []JoinSpec[int]{{Name: "joined", From: "fork", Merge: func(_ context.Context, _ CallInfo, values []int) (int, error) {
		var total int
		for _, value := range values {
			total += value
		}
		return total, nil
	}}}, map[string][]string{"fork": {"left", "right"}, "left": {"joined"}, "right": {"joined"}}, nil, nil)
	result, err := fanoutRunner.Start(context.Background(), "fanout-checkpoint", 3, Options[int]{MaxSteps: 1, MaxConcurrency: 1})
	if err != nil || result.Status != StatusBudget {
		t.Fatalf("create fan-out checkpoint = %+v, %v", result, err)
	}
	fanout = result.Checkpoint
	if err := fanoutRunner.validateCheckpoint(fanout); err != nil {
		t.Fatalf("valid fan-out checkpoint rejected: %v", err)
	}

	completedRunner := executorTestRunner(t, "done", []NodeSpec[int]{executorTestNode("done", executorTestEnd, FailInvocation)}, nil, nil, nil, nil)
	result, err = completedRunner.Start(context.Background(), "completed-checkpoint", 4, Options[int]{})
	if err != nil {
		t.Fatalf("create completed checkpoint: %v", err)
	}
	completed = result.Checkpoint
	if err := completedRunner.validateCheckpoint(completed); err != nil {
		t.Fatalf("valid completed checkpoint rejected: %v", err)
	}

	failedRunner := executorTestRunner(t, "fail", []NodeSpec[int]{executorTestNode("fail", func(context.Context, CallInfo, int) (Transition[int], error) {
		return Transition[int]{}, errors.New("failed")
	}, FailExecution)}, nil, nil, nil, nil)
	result, _ = failedRunner.Start(context.Background(), "failed-checkpoint", 4, Options[int]{})
	failed = result.Checkpoint
	if err := failedRunner.validateCheckpoint(failed); err != nil {
		t.Fatalf("valid failed checkpoint rejected: %v", err)
	}
	return
}

func TestValidateCheckpointHeaderAndInvocationInvariants(t *testing.T) {
	runner, base, _, _, _, _ := executorTestValidationFixtures(t)
	tests := []struct {
		name   string
		mutate func(*Checkpoint[int])
	}{
		{"unsupported format", func(s *Checkpoint[int]) { s.FormatVersion++ }},
		{"invalid run ID", func(s *Checkpoint[int]) { s.RunID = " padded" }},
		{"machine mismatch", func(s *Checkpoint[int]) { s.MachineID = "other" }},
		{"zero revision", func(s *Checkpoint[int]) { s.Revision = 0 }},
		{"final state while active", func(s *Checkpoint[int]) { value := 9; s.Final = &value }},
		{"next ID too small", func(s *Checkpoint[int]) { s.NextID = 2 }},
		{"cursor out of range", func(s *Checkpoint[int]) { s.ScheduleCursor = s.NextID }},
		{"invalid failure scope", func(s *Checkpoint[int]) { s.Failures = append(s.Failures, Failure{Scope: FailureScope(99)}) }},
		{"terminal failure while active", func(s *Checkpoint[int]) { s.Failures = append(s.Failures, Failure{Scope: FailExecution}) }},
		{"multiple terminal failures", func(s *Checkpoint[int]) { s.Failures = []Failure{{Scope: FailExecution}, {Scope: FailExecution}} }},
		{"invalid invocation prefix", func(s *Checkpoint[int]) { s.Invocations[0].ID = "x1" }},
		{"noncanonical invocation ID", func(s *Checkpoint[int]) { s.Invocations[0].ID = "i01" }},
		{"invocation ID already used", func(s *Checkpoint[int]) { s.Invocations[0].ID = "i" + fmt.Sprint(s.NextID) }},
		{"invalid callback ID", func(s *Checkpoint[int]) { s.Invocations[0].CallID = "c0" }},
		{"callback ID already used", func(s *Checkpoint[int]) { s.Invocations[0].CallID = "c" + fmt.Sprint(s.NextID) }},
		{"duplicate invocation ID", func(s *Checkpoint[int]) {
			duplicate := s.Invocations[0]
			duplicate.CallID = ""
			s.Invocations = append(s.Invocations, duplicate)
		}},
		{"duplicate callback ID", func(s *Checkpoint[int]) {
			s.Invocations = append(s.Invocations, Invocation[int]{ID: "i2", CallID: s.Invocations[0].CallID, Node: "pause", Status: InvocationReady})
		}},
		{"negative branch index", func(s *Checkpoint[int]) { s.Invocations[0].BranchIndex = -1 }},
		{"branch index without group", func(s *Checkpoint[int]) { s.Invocations[0].BranchIndex = 1 }},
		{"stray continuation key", func(s *Checkpoint[int]) { s.Invocations[0].Status = InvocationReady }},
		{"stray next targets", func(s *Checkpoint[int]) {
			s.Invocations[0].Status = InvocationReady
			s.Invocations[0].Continuation = ""
		}},
		{"stray child group", func(s *Checkpoint[int]) {
			s.Invocations[0].Status = InvocationReady
			s.Invocations[0].Continuation = ""
			s.Invocations[0].Next = nil
			s.Invocations[0].ChildGroupID = "g8"
		}},
		{"active callback missing", func(s *Checkpoint[int]) { s.Invocations[0].CallID = "" }},
		{"unknown active node", func(s *Checkpoint[int]) { s.Invocations[0].Node = "missing" }},
		{"unknown continuation", func(s *Checkpoint[int]) { s.Invocations[0].Continuation = "missing" }},
		{"empty waiting targets", func(s *Checkpoint[int]) { s.Invocations[0].Next = nil }},
		{"unknown waiting target", func(s *Checkpoint[int]) { s.Invocations[0].Next = []string{"missing"} }},
		{"group parent callback ID", func(s *Checkpoint[int]) { s.Invocations[0].Status = InvocationGroup }},
		{"group parent unknown node", func(s *Checkpoint[int]) {
			s.Invocations[0].Status = InvocationGroup
			s.Invocations[0].CallID = ""
			s.Invocations[0].Node = "missing"
		}},
		{"inactive callback ID", func(s *Checkpoint[int]) {
			s.Invocations[0].Status = InvocationEnded
			s.Invocations[0].Continuation = ""
			s.Invocations[0].Next = nil
		}},
		{"invalid status", func(s *Checkpoint[int]) {
			s.Invocations[0].Status = InvocationStatus("unknown")
			s.Invocations[0].CallID = ""
			s.Invocations[0].Continuation = ""
			s.Invocations[0].Next = nil
		}},
		{"group parent missing child group", func(s *Checkpoint[int]) {
			s.Invocations[0].Status = InvocationGroup
			s.Invocations[0].CallID = ""
			s.Invocations[0].ChildGroupID = "g7"
		}},
		{"joined invocation without group", func(s *Checkpoint[int]) {
			s.Invocations[0].Status = InvocationJoined
			s.Invocations[0].CallID = ""
			s.Invocations[0].Continuation = ""
			s.Invocations[0].Next = nil
		}},
		{"no active invocation", func(s *Checkpoint[int]) {
			s.Invocations[0].Status = InvocationFailed
			s.Invocations[0].CallID = ""
			s.Invocations[0].Continuation = ""
			s.Invocations[0].Next = nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := copyCheckpoint(base)
			test.mutate(&invalid)
			if err := runner.validateCheckpoint(invalid); !errors.Is(err, ErrInvalidCheckpoint) {
				t.Fatalf("validateCheckpoint error = %v", err)
			}
		})
	}

	joinRunner := executorTestRunner(t, "pause", []NodeSpec[int]{executorTestNode("pause", executorTestEnd, FailInvocation)}, []JoinSpec[int]{{Name: "joined", From: "fork", Merge: func(context.Context, CallInfo, []int) (int, error) { return 0, nil }}}, map[string][]string{"pause": {"joined"}}, map[string]Continuation[int]{"resume": {Decode: func([]byte) (any, error) { return nil, nil }, Apply: func(context.Context, CallInfo, int, any) (int, error) { return 0, nil }}}, nil)
	wrongJoin := copyCheckpoint(base)
	wrongJoin.Invocations[0].Next = []string{"joined"}
	if err := joinRunner.validateCheckpoint(wrongJoin); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("waiting invocation reached join outside its group: %v", err)
	}
}

func TestValidateCheckpointActivationGroupInvariants(t *testing.T) {
	_, _, runner, base, _, _ := executorTestValidationFixtures(t)
	parentID := base.Groups[0].ParentID
	childIDs := append([]string(nil), base.Groups[0].Children...)
	tests := []struct {
		name   string
		mutate func(*Checkpoint[int])
	}{
		{"invalid group ID", func(s *Checkpoint[int]) { s.Groups[0].ID = "bad" }},
		{"group ID already used", func(s *Checkpoint[int]) { s.Groups[0].ID = "g" + fmt.Sprint(s.NextID) }},
		{"invalid join callback ID", func(s *Checkpoint[int]) { s.Groups[0].CallID = "x2" }},
		{"join callback ID missing", func(s *Checkpoint[int]) { s.Groups[0].CallID = "" }},
		{"duplicate join callback ID", func(s *Checkpoint[int]) { s.Groups[0].CallID = s.Invocations[1].CallID }},
		{"duplicate group ID", func(s *Checkpoint[int]) { s.Groups = append(s.Groups, s.Groups[0]) }},
		{"group with invalid parent", func(s *Checkpoint[int]) { s.Groups[0].ParentID = "missing" }},
		{"incompatible join", func(s *Checkpoint[int]) { s.Groups[0].JoinNode = "elsewhere" }},
		{"too few children", func(s *Checkpoint[int]) { s.Groups[0].Children = s.Groups[0].Children[:1] }},
		{"too many children", func(s *Checkpoint[int]) { s.Groups[0].Children = append(s.Groups[0].Children, "i99") }},
		{"missing child", func(s *Checkpoint[int]) { s.Groups[0].Children[0] = "i99" }},
		{"child belongs to another group", func(s *Checkpoint[int]) { _, child := invocation(s, childIDs[0]); child.GroupID = "g99" }},
		{"child index mismatch", func(s *Checkpoint[int]) { _, child := invocation(s, childIDs[0]); child.BranchIndex = 1 }},
		{"joined child at wrong node", func(s *Checkpoint[int]) {
			_, child := invocation(s, childIDs[0])
			child.Status = InvocationJoined
			child.CallID = ""
			child.Node = "left"
		}},
		{"already settled group", func(s *Checkpoint[int]) {
			for _, id := range childIDs {
				_, child := invocation(s, id)
				child.Status = InvocationJoined
				child.CallID = ""
				child.Node = "joined"
			}
		}},
		{"child has missing group", func(s *Checkpoint[int]) { _, child := invocation(s, childIDs[0]); child.GroupID = "g99" }},
		{"group parent points at wrong node", func(s *Checkpoint[int]) { _, parent := invocation(s, parentID); parent.Node = "left" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := copyCheckpoint(base)
			test.mutate(&invalid)
			if err := runner.validateCheckpoint(invalid); !errors.Is(err, ErrInvalidCheckpoint) {
				t.Fatalf("validateCheckpoint error = %v", err)
			}
		})
	}

	noJoinRunner := executorTestRunner(t, "fork", []NodeSpec[int]{
		executorTestNode("fork", func(_ context.Context, _ CallInfo, state int) (Transition[int], error) {
			return model.To(state, "left", "right"), nil
		}, FailInvocation),
		executorTestNode("left", executorTestEnd, FailInvocation),
		executorTestNode("right", executorTestEnd, FailInvocation),
	}, nil, map[string][]string{"fork": {"left", "right"}}, nil, nil)
	noJoin, err := noJoinRunner.Start(context.Background(), "no-join-checkpoint", 1, Options[int]{MaxSteps: 1, MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	unexpected := copyCheckpoint(noJoin.Checkpoint)
	unexpected.Groups[0].CallID = "c9"
	if err := noJoinRunner.validateCheckpoint(unexpected); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("no-join group callback error = %v", err)
	}
}

func TestValidateCheckpointCompletedExecutionShapes(t *testing.T) {
	runner, _, _, fanout, completed, failed := executorTestValidationFixtures(t)
	for _, test := range []struct {
		name   string
		base   Checkpoint[int]
		mutate func(*Checkpoint[int])
	}{
		{"completed without a step", completed, func(s *Checkpoint[int]) { s.Steps = 0 }},
		{"completed with an activation group", fanout, func(s *Checkpoint[int]) { s.Completed = true }},
		{"failed execution with final state", failed, func(s *Checkpoint[int]) { value := 1; s.Final = &value }},
		{"completed with active invocation", completed, func(s *Checkpoint[int]) {
			s.Invocations = []Invocation[int]{{ID: "i1", Node: "done", Status: InvocationReady, CallID: "c2"}}
		}},
		{"completed without final state or invocation", completed, func(s *Checkpoint[int]) { s.Final = nil }},
		{"completed with both final and terminal invocation", completed, func(s *Checkpoint[int]) {
			s.Invocations = []Invocation[int]{{ID: "i1", Node: "done", Status: InvocationEnded}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := copyCheckpoint(test.base)
			test.mutate(&invalid)
			validator := runner
			if invalid.MachineID != runner.id {
				t.Fatal("fixture machine ID changed unexpectedly")
			}
			if err := validator.validateCheckpoint(invalid); !errors.Is(err, ErrInvalidCheckpoint) {
				t.Fatalf("validateCheckpoint error = %v", err)
			}
		})
	}
}

func TestCheckpointIDParser(t *testing.T) {
	for _, test := range []struct {
		id     string
		prefix string
		valid  bool
	}{
		{"i1", "i", true},
		{"i01", "i", false},
		{"c0", "c", false},
		{"gmax", "g", false},
		{"x1", "i", false},
	} {
		if _, valid := checkpointID(test.id, test.prefix); valid != test.valid {
			t.Errorf("checkpointID(%q, %q) validity = %t, want %t", test.id, test.prefix, valid, test.valid)
		}
	}
}
