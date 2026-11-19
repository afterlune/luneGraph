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
	nodeSeen := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		nodeSeen[node.Name] = true
	}
	for from := range returnTargets {
		if !nodeSeen[from] {
			nodeSeen[from] = true
			nodes = append(nodes, NodeSpec[int]{Name: from})
		}
	}
	compiled := CompiledGraph[int]{
		ID:         "machine-v1",
		Entry:      entry,
		Clone:      func(value int) (int, error) { return value, nil },
		Nodes:      make([]CompiledNode[int], len(nodes)),
		Joins:      make([]CompiledJoin[int], len(joins)),
		NodeByName: make(map[string]*CompiledNode[int], len(nodes)),
		JoinByName: make(map[string]*CompiledJoin[int], len(joins)),
	}
	for i, node := range nodes {
		_, mounted := returnTargets[node.Name]
		compiled.Nodes[i] = CompiledNode[int]{Spec: node, Mounted: mounted}
		compiled.NodeByName[node.Name] = &compiled.Nodes[i]
	}
	joinBySource := make(map[string]*CompiledJoin[int], len(joins))
	for i, join := range joins {
		compiled.Joins[i] = CompiledJoin[int]{Spec: join}
		compiled.JoinByName[join.Name] = &compiled.Joins[i]
		joinBySource[join.FanOutSource] = &compiled.Joins[i]
	}
	for i, join := range joins {
		if targets := edges[join.Name]; len(targets) > 0 {
			compiled.Joins[i].NextEdge = compiled.ResolveTarget(targets[0])
		}
	}
	for i, node := range nodes {
		compiled.Nodes[i].FanOutJoin = joinBySource[node.Name]
		compiled.Nodes[i].ReturnEdge = compiled.ResolveTarget(returnTargets[node.Name])
		if targets := edges[node.Name]; len(targets) > 0 {
			nodeEdges := make([]CompiledEdge[int], len(targets))
			for j, to := range targets {
				nodeEdges[j] = compiled.ResolveTarget(to)
			}
			compiled.Nodes[i].Edges = nodeEdges
		}
	}

	keys := make([]string, 0, len(continuations))
	for key := range continuations {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	compiled.Continuations = make([]CompiledContinuation[int], len(keys))
	compiled.ContByKey = make(map[string]*Continuation[int], len(keys))
	for i, key := range keys {
		compiled.Continuations[i] = CompiledContinuation[int]{Key: key, Cont: continuations[key]}
		compiled.ContByKey[key] = &compiled.Continuations[i].Cont
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
