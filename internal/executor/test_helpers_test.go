package executor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
)

func executorTestRunner(t *testing.T, entry string, nodes []NodeSpec[int], joins []JoinSpec[int], edges map[string][]string, continuations map[string]Continuation[int], returnTargets map[string]string) *Runner[int] {
	t.Helper()
	machine := Machine[int]{
		ID:            "machine-v1",
		Entry:         entry,
		Clone:         func(value int) (int, error) { return value, nil },
		Nodes:         make(map[string]NodeSpec[int], len(nodes)),
		Joins:         make(map[string]JoinSpec[int], len(joins)),
		JoinBySource:  make(map[string]string),
		Edges:         make(map[string]map[string]struct{}),
		Continuations: continuations,
		ReturnTargets: returnTargets,
	}
	for _, node := range nodes {
		machine.Nodes[node.Name] = node
	}
	for _, join := range joins {
		machine.Joins[join.Name] = join
		machine.JoinBySource[join.From] = join.Name
	}
	for from, targets := range edges {
		machine.Edges[from] = make(map[string]struct{}, len(targets))
		for _, target := range targets {
			machine.Edges[from][target] = struct{}{}
		}
	}
	runner, err := New(machine)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func executorTestNode(name string, run model.Node[int], scope FailureScope) NodeSpec[int] {
	return NodeSpec[int]{Name: name, Run: run, OnError: scope}
}

func executorTestEnd(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
	return model.EndExecution(value), nil
}

func executorTestReadyCheckpoint(node string, state int) Checkpoint[int] {
	return Checkpoint[int]{
		FormatVersion: CheckpointFormatVersion,
		RunID:         "run-v1",
		MachineID:     "machine-v1",
		Revision:      1,
		NextID:        3,
		Invocations:   []Invocation[int]{{ID: "i1", CallID: "c2", Node: node, State: state, Status: InvocationReady}},
	}
}

type executorTestStore struct {
	values     map[string]Checkpoint[int]
	createErr  error
	loadErr    error
	compareErr error
}

func newExecutorTestStore() *executorTestStore {
	return &executorTestStore{values: make(map[string]Checkpoint[int])}
}

func (s *executorTestStore) Create(ctx context.Context, checkpoint Checkpoint[int]) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.createErr != nil {
		return s.createErr
	}
	if _, exists := s.values[checkpoint.RunID]; exists {
		return ErrConflict
	}
	s.values[checkpoint.RunID] = copyCheckpoint(checkpoint)
	return nil
}

func (s *executorTestStore) Load(ctx context.Context, runID string) (Checkpoint[int], error) {
	if ctx == nil {
		return Checkpoint[int]{}, errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return Checkpoint[int]{}, err
	}
	if s.loadErr != nil {
		return Checkpoint[int]{}, s.loadErr
	}
	checkpoint, exists := s.values[runID]
	if !exists {
		return Checkpoint[int]{}, fmt.Errorf("missing run %q", runID)
	}
	return copyCheckpoint(checkpoint), nil
}

func (s *executorTestStore) CompareAndSwap(ctx context.Context, expected uint64, next Checkpoint[int]) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.compareErr != nil {
		return s.compareErr
	}
	current, exists := s.values[next.RunID]
	if !exists || current.Revision != expected || next.Revision != expected+1 || current.MachineID != next.MachineID {
		return ErrConflict
	}
	s.values[next.RunID] = copyCheckpoint(next)
	return nil
}
