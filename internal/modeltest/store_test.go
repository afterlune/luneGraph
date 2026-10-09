package modeltest

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/memory"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

var errStore = errors.New("injected store error")

type checkpointOracle[S any] interface {
	check(graph.Checkpoint[S]) error
}

type faultStore[S any] struct {
	graph.Store[S]
	c      *controller
	oracle checkpointOracle[S]
	steps  uint64
	inputs uint64
	path   string
	disk   *sqlite.Store[S]
}

func openStore[S any](t testing.TB, kind string, oracle checkpointOracle[S], clone graph.Clone[S], c *controller) *faultStore[S] {
	t.Helper()
	s := &faultStore[S]{oracle: oracle, c: c}
	if kind == "memory" {
		var err error
		s.Store, err = memory.New[S](clone)
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

func (s *faultStore[S]) reopen(t testing.TB) {
	t.Helper()
	if s.disk != nil {
		if err := s.disk.Close(); err != nil {
			t.Fatal(err)
		}
		s.disk = nil
	}
	disk, err := sqlite.Open(context.Background(), s.path, checkpoint.JSON[S]{})
	if err != nil {
		t.Fatal(err)
	}
	s.disk = disk
	s.Store = disk
}

func (s *faultStore[S]) CompareAndSwap(ctx context.Context, expected uint64, next graph.Checkpoint[S]) error {
	if err := s.oracle.check(next); err != nil {
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
	if fault {
		before, err := s.Store.Load(ctx, next.RunID)
		if err != nil {
			return fmt.Errorf("load injected-fault candidate base: %w", err)
		}
		calls := storeCandidateCalls(s.c, before, next)
		s.c.mu.Lock()
		s.c.faultCalls = calls
		s.c.mu.Unlock()
	}
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

func storeCandidateCalls[S any](c *controller, before, next graph.Checkpoint[S]) []string {
	changed := make(map[string]bool)
	for _, old := range before.Invocations {
		var current *graph.Invocation[S]
		for i := range next.Invocations {
			if next.Invocations[i].ID == old.ID {
				current = &next.Invocations[i]
				break
			}
		}
		if old.CallID == "" || current == nil || old.Node != current.Node || old.CallID != current.CallID || old.Status != current.Status || old.GroupID != current.GroupID || old.ChildGroupID != current.ChildGroupID || old.BranchIndex != current.BranchIndex || old.Continuation != current.Continuation || !reflect.DeepEqual(old.State, current.State) || !reflect.DeepEqual(old.Next, current.Next) {
			if old.CallID != "" {
				changed[old.CallID] = true
			}
		}
	}
	for _, old := range before.Groups {
		found := false
		for _, current := range next.Groups {
			if current.ID == old.ID {
				found = true
				break
			}
		}
		if !found && old.CallID != "" {
			changed[old.CallID] = true
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var calls []string
	for callID := range changed {
		request, ok := c.requests[callID]
		if !ok {
			continue
		}
		var operation graph.EventOperation
		switch request.role {
		case "node":
			operation = graph.OperationNode
		case "join":
			operation = graph.OperationJoin
		case "apply":
			operation = graph.OperationApply
		default:
			continue
		}
		key := eventKey{c.operation, operation, callID}
		event := c.events[key]
		if event.started == 1 && event.finished == 1 && event.resolved == 0 {
			calls = append(calls, callID)
		}
	}
	return calls
}
