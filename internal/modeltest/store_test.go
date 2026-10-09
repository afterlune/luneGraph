package modeltest

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/memory"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

var errStore = errors.New("injected store error")

type faultStore struct {
	graph.Store[state]
	c      *controller
	m      model
	steps  uint64
	inputs uint64
	path   string
	disk   *sqlite.Store[state]
}

func openStore(t testing.TB, kind string, m model, c *controller) *faultStore {
	t.Helper()
	s := &faultStore{m: m, c: c}
	if kind == "memory" {
		var err error
		s.Store, err = memory.New[state](clone)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		s.path = filepath.Join(t.TempDir(), "runs.db")
		s.reopen(t)
		t.Cleanup(func() {
			if s.disk != nil {
				if err := s.disk.Close(); err != nil {
					t.Error(err)
				}
			}
		})
	}
	return s
}

func (s *faultStore) reopen(t testing.TB) {
	t.Helper()
	if s.disk != nil {
		if err := s.disk.Close(); err != nil {
			t.Fatal(err)
		}
		s.disk = nil
	}
	disk, err := sqlite.Open(context.Background(), s.path, checkpoint.JSON[state]{})
	if err != nil {
		t.Fatal(err)
	}
	s.disk = disk
	s.Store = disk
}

func (s *faultStore) CompareAndSwap(ctx context.Context, expected uint64, next graph.Checkpoint[state]) error {
	if err := s.m.check(next); err != nil {
		return err
	}
	if next.Revision != expected+1 || next.Steps < s.steps || next.Steps > s.steps+1 {
		return fmt.Errorf("invalid commit counters: %+v", next)
	}
	inputs := s.inputs
	if next.Steps == s.steps {
		inputs++
	}
	if next.Revision != 1+next.Steps+inputs {
		return fmt.Errorf("revision differs from commit model")
	}
	s.c.mu.Lock()
	mode := s.c.current.mode
	fault := (mode >= 1 && mode <= 3) && s.c.triggerLocked(mode)
	s.c.mu.Unlock()
	if fault && mode == 1 {
		return graph.ErrConflict
	}
	if fault && mode == 2 {
		return errStore
	}
	if err := s.Store.CompareAndSwap(ctx, expected, next); err != nil {
		return err
	}
	s.steps = next.Steps
	s.inputs = inputs
	s.c.mu.Lock()
	s.c.writes[next.Revision] = true
	s.c.mu.Unlock()
	if fault && mode == 3 {
		return errStore
	}
	return nil
}
