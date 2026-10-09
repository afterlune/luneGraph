package observation

import (
	"context"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
)

func TestResolutionLedgerRetainsOnlyPendingMetadata(t *testing.T) {
	ctx := context.Background()
	var events []model.Event
	s := New(model.ObserverFunc(func(_ context.Context, e model.Event) { events = append(events, e) }), "machine", "run")
	for range 100 {
		span := s.Begin(ctx, model.Event{Operation: model.OperationNode, CallID: "c2", InvocationID: "i1", Revision: 3})
		span.End(ctx, model.Event{Revision: 3, Action: model.ActionEndExecution})
		s.SelectNode("missing")
		s.SelectNode("i1")
		s.ResolveSelected(ctx, model.OutcomeCommitted, 4, nil)
		if len(s.pending) != 0 {
			t.Fatal("retained callback history")
		}
	}
	if len(events) != 300 {
		t.Fatal("wrong phase count")
	}
	for i := 2; i < len(events); i += 3 {
		e := events[i]
		if e.Phase != model.PhaseResolved || e.Outcome != model.OutcomeCommitted || e.Revision != 4 || e.Action != model.ActionEndExecution || e.Duration != 0 {
			t.Fatalf("resolution=%+v", e)
		}
	}
	node := s.Begin(ctx, model.Event{Operation: model.OperationNode, CallID: "c3", InvocationID: "i2", Revision: 4})
	node.End(ctx, model.Event{Revision: 4})
	s.ResolveSelected(ctx, model.OutcomeCommitted, 5, nil)
	if len(s.pending) != 1 {
		t.Fatal("unselected node resolved with another candidate")
	}
	s.DiscardNode(ctx, "i2")
	if len(s.pending) != 0 || events[len(events)-1].Outcome != model.OutcomeDiscarded || events[len(events)-1].Revision != 4 {
		t.Fatal("discard did not retain base revision")
	}
	join := s.Begin(ctx, model.Event{Operation: model.OperationJoin, CallID: "c4", Revision: 5})
	join.End(ctx, model.Event{Revision: 5})
	s.DiscardPending(ctx, context.Canceled)
	if len(s.pending) != 0 || events[len(events)-1].Err != context.Canceled {
		t.Fatal("pending join not discarded")
	}
	var disabled *Session
	disabled.SelectNode("i1")
	disabled.DiscardNode(ctx, "i1")
	disabled.ResolveSelected(ctx, model.OutcomeUnknown, 3, nil)
	disabled.DiscardPending(ctx, nil)
}
