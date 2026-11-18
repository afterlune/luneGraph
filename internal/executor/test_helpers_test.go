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
	compiled := CompiledGraph[int]{
		ID:                "machine-v1",
		Entry:             entry,
		Clone:             func(value int) (int, error) { return value, nil },
		NodeIndex:         make(map[string]int, len(nodes)),
		JoinIndex:         make(map[string]int, len(joins)),
		ContinuationIndex: make(map[string]int, len(continuations)),
	}
	for i, node := range nodes {
		compiled.NodeIndex[node.Name] = i
	}
	for from := range returnTargets {
		if _, ok := compiled.NodeIndex[from]; !ok {
			compiled.NodeIndex[from] = len(nodes)
			nodes = append(nodes, NodeSpec[int]{Name: from})
		}
	}
	for i, join := range joins {
		compiled.JoinIndex[join.Name] = i
	}
	compiled.EntryIndex = compiled.NodeIndex[entry]

	joinBySource := make(map[string]int, len(joins))
	for i, join := range joins {
		joinBySource[join.FanOutSource] = i
		next := ""
		if targets := edges[join.Name]; len(targets) > 0 {
			next = targets[0]
		}
		compiled.Joins = append(compiled.Joins, CompiledJoin[int]{Spec: join, NextEdge: compiled.ResolveTarget(next)})
	}

	for i, node := range nodes {
		joinIndex, ok := joinBySource[node.Name]
		if !ok {
			joinIndex = -1
		}
		target, mounted := returnTargets[node.Name]
		start := len(compiled.Edges)
		for _, to := range edges[node.Name] {
			compiled.Edges = append(compiled.Edges, compiled.ResolveTarget(to))
		}
		compiled.Nodes = append(compiled.Nodes, CompiledNode[int]{
			Spec: node, Index: i, FanOutJoinIndex: joinIndex,
			ReturnEdge: compiled.ResolveTarget(target), Mounted: mounted,
			EdgeStart: start, EdgeEnd: len(compiled.Edges),
		})
	}

	keys := make([]string, 0, len(continuations))
	for key := range continuations {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for i, key := range keys {
		compiled.ContinuationIndex[key] = i
		compiled.Continuations = append(compiled.Continuations, CompiledContinuation[int]{Key: key, Cont: continuations[key]})
	}
	return New(compiled)
}

func executorTestNode(name string, run model.NodeFunc[int], scope FailureScope) NodeSpec[int] {
	return NodeSpec[int]{Name: name, Run: run, OnError: scope}
}

func executorTestEnd(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
	return model.EndExecution(value), nil
}

func executorTestReadyCheckpoint(node string, state int) Checkpoint[int] {
	return Checkpoint[int]{
		FormatVersion: CheckpointFormatVersion,
		RunID:         "run-v1",
		DefinitionID:  "machine-v1",
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
	if !exists || current.Revision != expected || next.Revision != expected+1 || current.DefinitionID != next.DefinitionID {
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
