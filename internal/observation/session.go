// Package observation delivers transient execution metadata independently of
// execution policy and application state.
package observation

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/afterlune/luneGraph/internal/model"
)

var nextOperationID atomic.Uint64

// Session is immutable and belongs to one public Runner call.
type Session struct {
	observer         model.Observer
	id               uint64
	machineID, runID string
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
	s.deliver(ctx, event)
	return Span{session: s, event: event, started: time.Now()}
}

func (span Span) End(ctx context.Context, result model.Event) {
	event := span.event
	event.Phase, event.Time = model.PhaseFinished, time.Now()
	event.Duration = event.Time.Sub(span.started)
	event.Status, event.Action, event.Revision, event.Err = result.Status, result.Action, result.Revision, result.Err
	span.session.deliver(ctx, event)
}

func (s *Session) deliver(ctx context.Context, event model.Event) {
	defer func() { _ = recover() }()
	s.observer.Observe(ctx, event)
}
