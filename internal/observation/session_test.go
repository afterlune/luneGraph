package observation

import (
	"context"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
)

func TestDisabledSessionHasNoAllocationsOrIDs(t *testing.T) {
	before := nextOperationID.Load()
	if allocs := testing.AllocsPerRun(100, func() {
		session := New(nil, "machine", "run")
		if session != nil || WrapStore[int](nil, session) != nil {
			t.Fatal("disabled observation created a wrapper")
		}
	}); allocs != 0 {
		t.Fatalf("allocations = %v", allocs)
	}
	if nextOperationID.Load() != before {
		t.Fatal("disabled observation allocated an ID")
	}
}

func TestSpanTimingExcludesStartDelivery(t *testing.T) {
	var start, end model.Event
	observer := model.ObserverFunc(func(_ context.Context, e model.Event) {
		if e.Phase == model.PhaseStarted {
			start = e
		} else {
			end = e
		}
	})
	ctx := context.Background()
	span := New(observer, "machine", "run").Begin(ctx, model.Event{Operation: model.OperationNode})
	span.End(ctx, model.Event{})
	if span.started.Before(start.Time) || end.Time.Before(span.started) || end.Duration != end.Time.Sub(span.started) {
		t.Fatal("duration included start delivery")
	}
}
