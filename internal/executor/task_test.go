package executor

import (
	"context"
	"errors"
	"testing"
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
