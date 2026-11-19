package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
)

func TestPrepareTaskRejectsUnknownNode(t *testing.T) {
	clone := func(v int) (int, error) { return v, nil }
	cp := Checkpoint[int]{
		FormatVersion: CheckpointFormatVersion,
		RunID:         "run",
		DefinitionID:  "machine",
		NextID:        2,
		Invocations: []Invocation[int]{
			{ID: "i1", Node: "missing", Status: InvocationReady, State: 1},
		},
	}

	cases := []struct {
		name   string
		runner *Runner[int]
		ref    taskRef
	}{
		{
			name:   "empty nodes",
			runner: &Runner[int]{definitionID: "machine", clone: clone},
			ref:    taskRef{id: "i1"},
		},
		{
			name: "mismatched cached node index",
			runner: &Runner[int]{
				definitionID: "machine",
				clone:        clone,
				nodes:        []CompiledNode[int]{{Spec: NodeSpec[int]{Name: "known"}}},
				nodeIndex:    map[string]int{"known": 0},
			},
			ref: taskRef{id: "i1", nodeIdx: 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := newLive(cp, tc.runner)
			_, err := tc.runner.prepareTask(context.Background(), l, tc.ref, nil)
			if !errors.Is(err, ErrInvalidCheckpoint) {
				t.Fatalf("prepareTask err = %v, want ErrInvalidCheckpoint", err)
			}
		})
	}
}

func TestPrepareTaskRejectsUnknownJoin(t *testing.T) {
	runner := &Runner[int]{
		definitionID: "machine",
		clone:        func(v int) (int, error) { return v, nil },
	}
	cp := Checkpoint[int]{
		FormatVersion: CheckpointFormatVersion,
		RunID:         "run",
		DefinitionID:  "machine",
		NextID:        6,
		Invocations: []Invocation[int]{
			{ID: "i1", Node: "fork", Status: InvocationGroup, ChildGroupID: "g3", State: 1},
			{ID: "i4", Node: "missing-join", Status: InvocationJoined, GroupID: "g3", BranchIndex: 0, State: 2},
		},
		Groups: []ActivationGroup{{
			ID:       "g3",
			CallID:   "c5",
			Source:   "fork",
			ParentID: "i1",
			JoinNode: "missing-join",
			Branches: []ActivationBranch{{InvocationID: "i4", Target: "worker"}},
		}},
	}
	l := newLive(cp, runner)
	if _, err := runner.prepareTask(context.Background(), l, taskRef{id: "g3", isJoin: true}, nil); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("prepareTask err = %v, want ErrInvalidCheckpoint", err)
	}
}

func TestResultFromLiveAndSnapshotIsolateFailure(t *testing.T) {
	final := 42
	runner := &Runner[int]{
		definitionID: "machine",
		clone:        func(v int) (int, error) { return v, nil },
	}
	cp := Checkpoint[int]{
		FormatVersion: CheckpointFormatVersion,
		RunID:         "run",
		DefinitionID:  "machine",
		NextID:        2,
		Completed:     true,
		Final:         &final,
		Failure: &Failure{
			InvocationID: "i1",
			Node:         "n1",
			Message:      "boom",
			PanicStack:   "stack",
		},
	}

	l := newLive(cp, runner)

	res, err := runner.resultFromLive(l, StatusFailed)
	if err != nil {
		t.Fatalf("resultFromLive err = %v", err)
	}
	if res.Header.Failure == nil || res.Header.Failure == l.header.Failure {
		t.Fatalf("Result.Header.Failure aliases live header Failure")
	}
	if res.Final == nil || *res.Final != 42 {
		t.Fatalf("Result.Final = %v, want 42", res.Final)
	}
	res.Header.Failure.Message = "mutated-result"
	*res.Final = 99
	if l.header.Failure.Message != "boom" {
		t.Fatalf("live failure mutated to %q", l.header.Failure.Message)
	}
	if l.final == nil || *l.final != 42 {
		t.Fatalf("live final mutated to %v", l.final)
	}

	snap := l.snapshot()
	if snap.Failure == nil || snap.Failure == l.header.Failure {
		t.Fatalf("snapshot Failure aliases live header Failure")
	}
	if snap.Final == nil || snap.Final == l.final {
		t.Fatalf("snapshot Final aliases live final")
	}
	snap.Failure.Message = "mutated-snapshot"
	*snap.Final = 77
	if l.header.Failure.Message != "boom" {
		t.Fatalf("live failure mutated by snapshot to %q", l.header.Failure.Message)
	}
	if *l.final != 42 {
		t.Fatalf("live final mutated by snapshot to %d", *l.final)
	}
}

func TestBatchResumeSkipsSiblingCanceledByFailGroup(t *testing.T) {
	boom := errors.New("apply failed")
	runner := executorTestRunner(t, "fork", []NodeSpec[int]{
		executorTestNode("fork", func(_ context.Context, _ CallInfo, state int) (Transition[int], error) {
			return model.To(state, "wait-a", "wait-b"), nil
		}, FailInvocation),
		executorTestNode("wait-a", func(_ context.Context, _ CallInfo, state int) (Transition[int], error) {
			return model.Wait(state, "cont", "done"), nil
		}, FailGroup),
		executorTestNode("wait-b", func(_ context.Context, _ CallInfo, state int) (Transition[int], error) {
			return model.Wait(state, "cont", "done"), nil
		}, FailGroup),
		executorTestNode("done", executorTestEnd, FailInvocation),
	}, nil, map[string][]string{
		"fork":   {"wait-a", "wait-b"},
		"wait-a": {"done"},
		"wait-b": {"done"},
	}, map[string]Continuation[int]{
		"cont": {
			Decode: func(b []byte) (any, error) { return string(b), nil },
			Apply: func(_ context.Context, _ CallInfo, state int, val any) (int, error) {
				if val.(string) == "fail" {
					return 0, boom
				}
				return state + 1, nil
			},
		},
	}, nil)

	exec, res, err := runner.Start(context.Background(), "batch-failgroup", 1, nil, Options{})
	if err != nil || res.Status != StatusWaiting {
		t.Fatalf("Start = %+v, %v", res, err)
	}
	pending := exec.Pending()
	if len(pending) != 2 {
		t.Fatalf("Pending = %+v", pending)
	}
	resumed, err := exec.Advance(context.Background(), []ResumeInput{
		{InvocationID: pending[0].InvocationID, CallID: pending[0].CallID, Payload: []byte("fail")},
		{InvocationID: pending[1].InvocationID, CallID: pending[1].CallID, Payload: []byte("ok")},
	}, Options{})
	if err != nil || resumed.Status != StatusCompletedWithFailures {
		t.Fatalf("Advance = %+v, err = %v; want StatusCompletedWithFailures, nil", resumed, err)
	}
}

func TestLiveJoinAtMaxRevisionStaysEnqueuedAcrossAdvance(t *testing.T) {
	runner := executorTestRunner(t, "fork", []NodeSpec[int]{
		executorTestNode("fork", executorTestEnd, FailInvocation),
		executorTestNode("worker", executorTestEnd, FailInvocation),
	}, []JoinSpec[int]{{
		Name:         "join",
		FanOutSource: "fork",
		Merge: func(_ context.Context, _ CallInfo, parent int, _ []BranchResult[int]) (int, error) {
			return parent, nil
		},
	}}, map[string][]string{"fork": {"worker"}}, nil, nil)

	cp := Checkpoint[int]{
		FormatVersion: CheckpointFormatVersion,
		RunID:         "max-rev-join",
		DefinitionID:  runner.definitionID,
		Revision:      ^uint64(0),
		Steps:         2,
		NextID:        6,
		Invocations: []Invocation[int]{
			{ID: "i1", Node: "fork", Status: InvocationGroup, ChildGroupID: "g3", State: 1},
			{ID: "i4", Node: "join", Status: InvocationJoined, GroupID: "g3", BranchIndex: 0, State: 2},
		},
		Groups: []ActivationGroup{{
			ID:       "g3",
			CallID:   "c5",
			Source:   "fork",
			ParentID: "i1",
			JoinNode: "join",
			Branches: []ActivationBranch{{InvocationID: "i4", Target: "worker"}},
		}},
	}
	exec := &Execution[int]{runner: runner}
	exec.live = initLive(&exec.liveVal, cp, runner)

	for attempt := range 2 {
		res, err := exec.Advance(context.Background(), nil, Options{})
		if !errors.Is(err, ErrExecutionLimit) || res.Status != StatusFailed {
			t.Fatalf("attempt %d: Advance = %+v, %v; want ErrExecutionLimit", attempt, res, err)
		}
	}
}
