// Package memory provides a concurrent in-memory Store for graph executions.
package memory

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	graph "lune-graph"
	"lune-graph/checkpoint"
)

// Store owns independent copies of its checkpoints. It is safe for concurrent
// calls, including calls from different runners.
type Store[S any] struct {
	mu     sync.RWMutex
	values map[string]graph.Checkpoint[S]
	clone  graph.Clone[S]
}

// New requires the same deep-copy behavior as graph.Config.Clone.
func New[S any](clone graph.Clone[S]) (*Store[S], error) {
	if clone == nil {
		return nil, errors.New("clone is required")
	}
	return &Store[S]{values: make(map[string]graph.Checkpoint[S]), clone: clone}, nil
}

func (s *Store[S]) Create(ctx context.Context, checkpoint graph.Checkpoint[S]) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil {
		return errors.New("store is nil")
	}
	clone, err := checkpoint.Clone(s.clone)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, exists := s.values[checkpoint.RunID]; exists {
		return graph.ErrConflict
	}
	if s.values == nil {
		s.values = make(map[string]graph.Checkpoint[S])
	}
	s.values[checkpoint.RunID] = clone
	return nil
}

func (s *Store[S]) Load(ctx context.Context, runID string) (graph.Checkpoint[S], error) {
	if err := checkContext(ctx); err != nil {
		return graph.Checkpoint[S]{}, err
	}
	if s == nil {
		return graph.Checkpoint[S]{}, errors.New("store is nil")
	}
	s.mu.RLock()
	stored, exists := s.values[runID]
	s.mu.RUnlock()
	if !exists {
		return graph.Checkpoint[S]{}, fmt.Errorf("run %q: %w", runID, checkpoint.ErrNotFound)
	}
	return stored.Clone(s.clone)
}

func (s *Store[S]) CompareAndSwap(ctx context.Context, expected uint64, next graph.Checkpoint[S]) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil {
		return errors.New("store is nil")
	}
	if expected == math.MaxUint64 {
		return graph.ErrExecutionLimit
	}
	clone, err := next.Clone(s.clone)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	current, exists := s.values[next.RunID]
	if !exists || current.Revision != expected || current.MachineID != next.MachineID || next.Revision != expected+1 {
		return graph.ErrConflict
	}
	s.values[next.RunID] = clone
	return nil
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	return ctx.Err()
}

var _ graph.Store[int] = (*Store[int])(nil)
