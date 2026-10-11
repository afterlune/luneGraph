// Package memory provides a concurrent in-memory Store.
package memory

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/internal/model"
	"slices"
	"sync"
)

type record[S any] struct {
	header graph.Checkpoint[S]
	inv    map[string]graph.Invocation[S]
	groups map[string]graph.ActivationGroup
}

// Store owns its states. Calls, including Load and Commit, are concurrent safe.
type Store[S any] struct {
	mu     sync.RWMutex
	values map[string]*record[S]
	clone  graph.Clone[S]
}

func New[S any](clone graph.Clone[S]) (*Store[S], error) {
	if clone == nil {
		return nil, errors.New("clone is required")
	}
	return &Store[S]{values: make(map[string]*record[S]), clone: clone}, nil
}
func (s *Store[S]) Create(ctx context.Context, cp graph.Checkpoint[S]) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil {
		return errors.New("store is nil")
	}
	if err := model.ValidateCheckpointHeader(cp); err != nil {
		return err
	}
	if cp.Revision != 1 {
		return graph.ErrInvalidCheckpoint
	}
	if err := model.ValidateChange(0, cp.Change()); err != nil {
		return err
	}
	copied, err := cp.Clone(s.clone)
	if err != nil {
		return err
	}
	rec := &record[S]{header: copied, inv: make(map[string]graph.Invocation[S]), groups: make(map[string]graph.ActivationGroup)}
	rec.header.Invocations = nil
	rec.header.Groups = nil
	for _, v := range copied.Invocations {
		rec.inv[v.ID] = v
	}
	for _, g := range copied.Groups {
		rec.groups[g.ID] = g
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := s.values[cp.RunID]; ok {
		return graph.ErrConflict
	}
	if s.values == nil {
		s.values = make(map[string]*record[S])
	}
	s.values[cp.RunID] = rec
	return nil
}
func (s *Store[S]) Load(ctx context.Context, id string) (graph.Checkpoint[S], error) {
	if err := checkContext(ctx); err != nil {
		return graph.Checkpoint[S]{}, err
	}
	if s == nil {
		return graph.Checkpoint[S]{}, errors.New("store is nil")
	}
	s.mu.RLock()
	rec, ok := s.values[id]
	if !ok {
		s.mu.RUnlock()
		return graph.Checkpoint[S]{}, fmt.Errorf("run %q: %w", id, checkpoint.ErrNotFound)
	}
	cp := rec.header
	for _, v := range rec.inv {
		cp.Invocations = append(cp.Invocations, v)
	}
	for _, g := range rec.groups {
		cp.Groups = append(cp.Groups, g)
	}
	s.mu.RUnlock()
	slices.SortFunc(cp.Invocations, func(a, b graph.Invocation[S]) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(cp.Groups, func(a, b graph.ActivationGroup) int { return cmp.Compare(a.ID, b.ID) })
	return cp.Clone(s.clone)
}

func (s *Store[S]) LoadHeader(ctx context.Context, id string) (graph.Header, error) {
	if err := checkContext(ctx); err != nil {
		return graph.Header{}, err
	}
	if s == nil {
		return graph.Header{}, errors.New("store is nil")
	}
	s.mu.RLock()
	rec, ok := s.values[id]
	if !ok {
		s.mu.RUnlock()
		return graph.Header{}, fmt.Errorf("run %q: %w", id, checkpoint.ErrNotFound)
	}
	header := rec.header.Header()
	s.mu.RUnlock()
	return header, nil
}

func (s *Store[S]) Commit(ctx context.Context, expected uint64, c graph.Change[S]) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil {
		return errors.New("store is nil")
	}
	if err := model.ValidateChange(expected, c); err != nil {
		return err
	}
	owned, err := c.Clone(s.clone)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	rec, ok := s.values[c.Header.RunID]
	if !ok || rec.header.Revision != expected {
		return graph.ErrConflict
	}
	if err := model.ValidateAdvance(rec.header.Header(), c.Header); err != nil {
		return err
	}
	rec.header = owned.Checkpoint()
	rec.header.Invocations = nil
	rec.header.Groups = nil
	if c.Header.Completed {
		clear(rec.inv)
		clear(rec.groups)
		return nil
	}
	for _, id := range owned.DeleteInvocations {
		delete(rec.inv, id)
	}
	for _, v := range owned.UpsertInvocations {
		rec.inv[v.ID] = v
	}
	for _, id := range owned.DeleteGroups {
		delete(rec.groups, id)
	}
	for _, g := range owned.UpsertGroups {
		rec.groups[g.ID] = g
	}
	return nil
}
func (s *Store[S]) Delete(ctx context.Context, id string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil {
		return errors.New("store is nil")
	}
	if !model.ValidName(id) {
		return errors.New("invalid run ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := s.values[id]; !ok {
		return checkpoint.ErrNotFound
	}
	delete(s.values, id)
	return nil
}
func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	return ctx.Err()
}

var _ graph.Store[int] = (*Store[int])(nil)
