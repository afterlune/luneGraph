package observation

import (
	"context"
	"time"

	"github.com/afterlune/luneGraph/internal/model"
)

type pendingCallback struct {
	event    model.Event
	selected bool
}

func executionCallback(op model.EventOperation) bool {
	return op == model.OperationNode || op == model.OperationJoin || op == model.OperationApply
}

// ResolveSelected resolves the selected independent callback. Revision zero preserves
// each callback's base revision when commit was not entered.
func (s *Session) ResolveSelected(ctx context.Context, outcome model.CallbackOutcome, revision uint64, err error) {
	s.resolve(ctx, false, outcome, revision, err)
}

// DiscardPending runs only after workers drain and before public completion.
func (s *Session) DiscardPending(ctx context.Context, err error) {
	s.resolve(ctx, true, model.OutcomeDiscarded, 0, err)
}

func (s *Session) resolve(ctx context.Context, all bool, outcome model.CallbackOutcome, revision uint64, err error) {
	if s == nil {
		return
	}
	// Delivery must not hold the ledger lock: observers are application code.
	s.mu.Lock()
	events := s.resolved[:0]
	for id, pending := range s.pending {
		if !all && !pending.selected {
			continue
		}
		event := pending.event
		event.Phase, event.Outcome, event.Err = model.PhaseResolved, outcome, err
		if revision != 0 {
			event.Revision = revision
		}
		event.Time = time.Now()
		events = append(events, event)
		delete(s.pending, id)
	}
	s.mu.Unlock()
	for _, event := range events {
		s.deliver(ctx, event)
	}
	clear(events)
	s.resolved = events[:0]
}

// SelectCallback attaches exactly one independently committed callback.
func (s *Session) SelectCallback(callID string) {
	if s == nil || callID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pending[callID]
	if ok {
		p.selected = true
		s.pending[callID] = p
	}
}
func (s *Session) DiscardCallback(ctx context.Context, callID string) {
	if s == nil || callID == "" {
		return
	}
	s.SelectCallback(callID)
	s.ResolveSelected(ctx, model.OutcomeDiscarded, 0, nil)
}
