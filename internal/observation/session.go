// Package observation delivers transient execution metadata independently of
// execution policy and application state.
package observation

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/afterlune/luneGraph/internal/model"
)

var nextOperationID atomic.Uint64

// Session belongs to one public Runner call. It retains only unresolved
// callback metadata; application state and payloads never enter this ledger.
type Session struct {
	observer         model.Observer
	id               uint64
	machineID, runID string
	mu               sync.Mutex
	pending          map[string]pendingCallback
	// Resolution is driven by the public-call goroutine, never a worker.
	// Reuse its delivery buffer without retaining delivered metadata.
	resolved []model.Event
}

func New(observer model.Observer, machineID, runID string) *Session {
	if observer == nil {
		return nil
	}
	return &Session{observer: observer, id: nextOperationID.Add(1), machineID: machineID, runID: runID}
}

// Span is owned by the goroutine executing the observed operation.
type Span struct {
	session *Session
	event   model.Event
	started time.Time
}

func (s *Session) Begin(ctx context.Context, event model.Event) Span {
	event.OperationID, event.MachineID, event.RunID = s.id, s.machineID, s.runID
	event.Phase, event.Time = model.PhaseStarted, time.Now()
	if executionCallback(event.Operation) {
		s.mu.Lock()
		if s.pending == nil {
			s.pending = make(map[string]pendingCallback)
		}
		s.pending[event.CallID] = pendingCallback{event: event, selected: event.Operation != model.OperationNode}
		s.mu.Unlock()
	}
	s.deliver(ctx, event)
	return Span{session: s, event: event, started: time.Now()}
}

func (span Span) End(ctx context.Context, result model.Event) {
	event := span.event
	event.Phase, event.Time = model.PhaseFinished, time.Now()
	event.Duration = event.Time.Sub(span.started)
	event.Status, event.Action, event.Revision, event.Err = result.Status, result.Action, result.Revision, result.Err
	if executionCallback(event.Operation) {
		span.session.mu.Lock()
		pending := span.session.pending[event.CallID]
		pending.event.Action = event.Action
		span.session.pending[event.CallID] = pending
		span.session.mu.Unlock()
	}
	span.session.deliver(ctx, event)
}

func (s *Session) deliver(ctx context.Context, event model.Event) {
	defer func() { _ = recover() }()
	s.observer.Observe(ctx, event)
}
