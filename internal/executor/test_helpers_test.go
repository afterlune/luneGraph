package executor

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/testchange"
)

func executorTestRunner(t *testing.T, entry string, nodes []NodeSpec[int], joins []JoinSpec[int], edges map[string][]string, continuations map[string]Continuation[int], returnTargets map[string]string) *Runner[int] {
	t.Helper()
	nodeIdx := make(map[string]int, len(nodes))
	for i, node := range nodes {
		nodeIdx[node.Name] = i
	}
	joinIdx := make(map[string]int, len(joins))
	for i, join := range joins {
		joinIdx[join.Name] = i
	}

	compiledJoins := make([]CompiledJoin[int], len(joins))
	for i, join := range joins {
		nextTarget := ""
		if targets := edges[join.Name]; len(targets) > 0 {
			nextTarget = targets[0]
		}
		nIdx := -1
		if idx, ok := nodeIdx[nextTarget]; ok {
			nIdx = idx
		}
		jIdx := -1
		if idx, ok := joinIdx[nextTarget]; ok {
			jIdx = idx
		}
		compiledJoins[i] = CompiledJoin[int]{
			Spec:          join,
			NextTarget:    nextTarget,
			NextNodeIndex: nIdx,
			NextJoinIndex: jIdx,
		}
	}

	joinBySource := make(map[string]int, len(joins))
	for i, join := range joins {
		joinBySource[join.From] = i
	}

	var edgeTargets []string
	compiledNodes := make([]CompiledNode[int], len(nodes))
	for i, node := range nodes {
		jIdx := -1
		if idx, ok := joinBySource[node.Name]; ok {
			jIdx = idx
		}
		returnTarget := ""
		mounted := false
		if target, ok := returnTargets[node.Name]; ok {
			returnTarget = target
			mounted = true
		}
		returnNodeIdx := -1
		if idx, ok := nodeIdx[returnTarget]; ok {
			returnNodeIdx = idx
		}

		targets := edges[node.Name]
		edgeStart := len(edgeTargets)
		edgeTargets = append(edgeTargets, targets...)
		edgeEnd := len(edgeTargets)

		compiledNodes[i] = CompiledNode[int]{
			Spec:            node,
			JoinIndex:       jIdx,
			ReturnTarget:    returnTarget,
			ReturnNodeIndex: returnNodeIdx,
			Mounted:         mounted,
			EdgeStart:       edgeStart,
			EdgeEnd:         edgeEnd,
		}
	}

	for from, target := range returnTargets {
		if _, ok := nodeIdx[from]; !ok {
			nodeIdx[from] = len(compiledNodes)
			rIdx := -1
			if idx, ok := nodeIdx[target]; ok {
				rIdx = idx
			}
			compiledNodes = append(compiledNodes, CompiledNode[int]{
				Spec:            NodeSpec[int]{Name: from},
				JoinIndex:       -1,
				ReturnTarget:    target,
				ReturnNodeIndex: rIdx,
				Mounted:         true,
			})
		}
	}

	keys := make([]string, 0, len(continuations))
	for k := range continuations {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	compiledContinuations := make([]CompiledContinuation[int], len(keys))
	for i, k := range keys {
		compiledContinuations[i] = CompiledContinuation[int]{
			Key:  k,
			Cont: continuations[k],
		}
	}

	machine := Machine[int]{
		ID:            "machine-v1",
		Entry:         entry,
		Clone:         func(value int) (int, error) { return value, nil },
		Nodes:         compiledNodes,
		EdgeTargets:   edgeTargets,
		Joins:         compiledJoins,
		Continuations: compiledContinuations,
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

func (s *executorTestStore) LoadHeader(ctx context.Context, runID string) (Header, error) {
	checkpoint, err := s.Load(ctx, runID)
	return checkpoint.Header(), err
}

func (s *executorTestStore) Commit(ctx context.Context, expected uint64, change Change[int]) error {
	next := change.Checkpoint()
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
	s.values[next.RunID] = copyCheckpoint(testchange.Apply(current, change))
	return nil
}

func (s *executorTestStore) Delete(ctx context.Context, runID string) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, exists := s.values[runID]; !exists {
		return fmt.Errorf("missing run %q", runID)
	}
	delete(s.values, runID)
	return nil
}
