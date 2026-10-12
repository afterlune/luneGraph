package executor

import (
	"context"
	"errors"
	"testing"
)

func canMergeFixture(predicate func(int, []BranchResult[int]) bool, clone Clone[int], allResolved bool) (*Runner[int], *live[int]) {
	if clone == nil {
		clone = func(value int) (int, error) { return value, nil }
	}
	spec := JoinSpec[int]{
		Name:     "join",
		From:     "fork",
		CanMerge: predicate,
		Merge: func(_ context.Context, _ CallInfo, parent int, _ []BranchResult[int]) (int, error) {
			return parent, nil
		},
	}
	runner := &Runner[int]{id: "machine", clone: clone, joins: map[string]JoinSpec[int]{"join": spec}}
	secondStatus := InvocationReady
	secondNode := "worker"
	if allResolved {
		secondStatus = InvocationJoined
		secondNode = "join"
	}
	checkpoint := Checkpoint[int]{
		RunID: "run", MachineID: "machine", Revision: 7, Steps: 2, NextID: 8,
		Invocations: []Invocation[int]{
			{ID: "i1", Node: "fork", Status: InvocationGroup, ChildGroupID: "g7", State: 10},
			{ID: "i3", Node: "join", Status: InvocationJoined, GroupID: "g7", BranchIndex: 0, State: 20},
			{ID: "i5", Node: secondNode, Status: secondStatus, GroupID: "g7", BranchIndex: 1, State: 30},
		},
		Groups: []ActivationGroup{{
			ID: "g7", CallID: "c6", Source: "fork", ParentID: "i1", JoinNode: "join",
			Branches: []ActivationBranch{{InvocationID: "i3", Target: "worker"}, {InvocationID: "i5", Target: "worker"}},
		}},
	}
	return runner, newLive(checkpoint, runner.joins)
}

func TestPrepareCanMergeTaskIsolatesPredicateInputs(t *testing.T) {
	called := false
	runner, live := canMergeFixture(func(parent int, branches []BranchResult[int]) bool {
		called = true
		if parent != 10 || len(branches) != 2 || branches[0].State != 20 || branches[1].State != 30 || !branches[0].OK() || !branches[1].Pending() {
			t.Fatalf("predicate inputs = parent %d, branches %+v", parent, branches)
		}
		branches[0].State = 200
		branches[1].State = 300
		return true
	}, nil, false)
	task, err := runner.prepareTask(context.Background(), live, taskRef{id: "g7", join: true}, nil)
	if err != nil || !task.ready || task.preflightErr != nil || !called {
		t.Fatalf("prepared task = %+v, called=%v, err=%v", task, called, err)
	}
	if task.state != 10 || len(task.branches) != 2 || task.branches[0].State != 20 || task.branches[1].State != 0 {
		t.Fatalf("predicate mutation leaked into Merge inputs: %+v", task)
	}
}

func TestPrepareCanMergeTaskDefersAndHandlesFailures(t *testing.T) {
	t.Run("false", func(t *testing.T) {
		runner, live := canMergeFixture(func(int, []BranchResult[int]) bool { return false }, nil, false)
		task, err := runner.prepareTask(context.Background(), live, taskRef{id: "g7", join: true}, nil)
		if err != nil || task.ready || task.preflightErr != nil {
			t.Fatalf("false predicate task = %+v, %v", task, err)
		}
	})

	t.Run("panic", func(t *testing.T) {
		runner, live := canMergeFixture(func(int, []BranchResult[int]) bool { panic("predicate failed") }, nil, false)
		task, err := runner.prepareTask(context.Background(), live, taskRef{id: "g7", join: true}, nil)
		var panicErr *PanicError
		if err != nil || !task.ready || !errors.As(task.preflightErr, &panicErr) || panicErr.Callback != "can-merge join" {
			t.Fatalf("panic predicate task = %+v, %v", task, err)
		}
	})

	t.Run("clone error", func(t *testing.T) {
		boom := errors.New("clone branch state")
		clone := func(value int) (int, error) {
			if value == 30 {
				return 0, boom
			}
			return value, nil
		}
		runner, live := canMergeFixture(func(int, []BranchResult[int]) bool { return true }, clone, false)
		_, err := runner.prepareTask(context.Background(), live, taskRef{id: "g7", join: true}, nil)
		if !errors.Is(err, boom) {
			t.Fatalf("clone error = %v", err)
		}
	})

	t.Run("all branches resolved", func(t *testing.T) {
		called := false
		runner, live := canMergeFixture(func(int, []BranchResult[int]) bool {
			called = true
			return false
		}, nil, true)
		task, err := runner.prepareTask(context.Background(), live, taskRef{id: "g7", join: true}, nil)
		if err != nil || !task.ready || task.preflightErr != nil || called || len(task.branches) != 2 {
			t.Fatalf("settled join task = %+v, called=%v, err=%v", task, called, err)
		}
	})
}

func TestCanMergeConvertsPredicatePanic(t *testing.T) {
	call := CallInfo{InvocationID: "i1"}
	ready, err := canMerge(JoinSpec[int]{Name: "joined", CanMerge: func(int, []BranchResult[int]) bool { return true }}, call, 1, nil)
	if err != nil || !ready {
		t.Fatalf("ready predicate = %v, %v", ready, err)
	}
	ready, err = canMerge(JoinSpec[int]{Name: "joined", CanMerge: func(int, []BranchResult[int]) bool { panic("boom") }}, call, 1, nil)
	var panicErr *PanicError
	if ready || !errors.As(err, &panicErr) || panicErr.Callback != "can-merge joined" || panicErr.InvocationID != call.InvocationID {
		t.Fatalf("panic predicate = %v, %v", ready, err)
	}
}
