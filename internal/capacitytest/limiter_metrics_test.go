package capacitytest

import (
	"context"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type limiterCallbacks struct {
	Started   [3]uint64 `json:"started_node_join_apply"`
	Finished  [3]uint64 `json:"finished_node_join_apply"`
	Committed uint64    `json:"committed"`
	Discarded uint64    `json:"discarded"`
	Unknown   uint64    `json:"unknown"`
	Active    int32     `json:"active"`
	Peak      int32     `json:"peak"`
	Invalid   bool      `json:"invalid_events"`
}

func (m *limiterTracker) snapshot() limiterCallbacks {
	s := limiterCallbacks{Committed: m.committed.Load(), Discarded: m.discarded.Load(), Unknown: m.unknown.Load(), Active: m.active.Load(), Peak: m.peak.Load(), Invalid: m.invalid.Load()}
	for i := range s.Started {
		s.Started[i], s.Finished[i] = m.starts[i].Load(), m.finishes[i].Load()
	}
	return s
}

func (m *limiterTracker) verifyDrained(t testing.TB, capacity int) {
	t.Helper()
	s := m.snapshot()
	started := s.Started[0] + s.Started[1] + s.Started[2]
	if s.Invalid || s.Active != 0 || s.Peak > int32(capacity) || s.Started != s.Finished || started != s.Committed+s.Discarded+s.Unknown {
		t.Fatalf("undrained callback budget/results: %+v capacity=%d", s, capacity)
	}
}

func TestLimiterTrackerCountsEveryResultOutcome(t *testing.T) {
	var m limiterTracker
	ctx := context.Background()
	for i, op := range []graph.EventOperation{graph.OperationNode, graph.OperationJoin, graph.OperationApply} {
		m.Observe(ctx, graph.Event{Operation: op, Phase: graph.PhaseStarted})
		m.Observe(ctx, graph.Event{Operation: op, Phase: graph.PhaseFinished})
		m.Observe(ctx, graph.Event{Operation: op, Phase: graph.PhaseResolved, Outcome: []graph.CallbackOutcome{graph.OutcomeCommitted, graph.OutcomeDiscarded, graph.OutcomeUnknown}[i]})
	}
	m.Observe(ctx, graph.Event{Operation: graph.OperationRecover, Phase: graph.PhaseFinished})
	m.verifyDrained(t, 1)
	s := m.snapshot()
	if s.Started != [3]uint64{1, 1, 1} || s.Committed != 1 || s.Discarded != 1 || s.Unknown != 1 {
		t.Fatal(s)
	}
	for _, e := range []graph.Event{{Operation: graph.OperationNode, Phase: graph.PhaseResolved}, {Operation: graph.OperationNode, Phase: "invalid"}} {
		var invalid limiterTracker
		invalid.Observe(ctx, e)
		if !invalid.invalid.Load() {
			t.Fatal("invalid callback event was ignored")
		}
	}
}
