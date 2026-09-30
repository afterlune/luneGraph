package observationtest

import (
	"context"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type panicStore struct{ graph.Store[int] }

func (s panicStore) Create(context.Context, graph.Checkpoint[int]) error { panic("store panic") }

func TestStorePanicRemainsOutsideCallbackBoundary(t *testing.T) {
	log := &recorder{}
	r := singleRunner(t)
	func() {
		defer func() {
			if value := recover(); value != "store panic" {
				t.Fatalf("store panic = %v", value)
			}
		}()
		_, _ = r.Start(context.Background(), "store-panic", 0, graph.Options[int]{Store: panicStore{}, Observer: log})
	}()
	events := log.snapshot()
	if len(events) != 2 || events[0].Operation != graph.OperationStart || events[1].Operation != graph.OperationCreate {
		t.Fatalf("misleading outcome: %+v", events)
	}
	for _, e := range events {
		if e.Phase != graph.PhaseStarted {
			t.Fatal("panic reported as a returned outcome")
		}
	}
}
