package observationtest

import (
	"context"
	"errors"
	"testing"

	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

func TestStoreDeleteObservation(t *testing.T) {
	rawStore := memoryStore(t)

	// WrapStore nil guards
	if observation.WrapStore[int](nil, nil) != nil {
		t.Fatal("expected nil store when store is nil")
	}
	if observation.WrapStore[int](rawStore, nil) != rawStore {
		t.Fatal("expected original store when session is nil")
	}

	rec := &recorder{}
	sessWithRec := observation.New(rec, "m-1", "run-1")
	obsStore := observation.WrapStore[int](rawStore, sessWithRec)

	// 1. Create a checkpoint first
	err := obsStore.Create(context.Background(), model.Checkpoint[int]{
		FormatVersion: model.CheckpointFormatVersion,
		RunID:         "run-1",
		MachineID:     "m-1",
		Revision:      1,
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// 2. Delete existing
	err = obsStore.Delete(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	events := rec.snapshot()
	var deleteEvents []model.Event
	for _, e := range events {
		if e.Operation == model.OperationDelete {
			deleteEvents = append(deleteEvents, e)
		}
	}
	if len(deleteEvents) != 2 {
		t.Fatalf("expected 2 delete events, got: %+v", deleteEvents)
	}
	if deleteEvents[0].Phase != model.PhaseStarted || deleteEvents[1].Phase != model.PhaseFinished {
		t.Fatalf("unexpected phases: %+v", deleteEvents)
	}
	if deleteEvents[1].Err != nil {
		t.Fatalf("unexpected error in finish: %v", deleteEvents[1].Err)
	}

	// 3. Delete non-existent
	err = obsStore.Delete(context.Background(), "run-non-existent")
	if !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
	lastEvent := rec.snapshot()[len(rec.snapshot())-1]
	if lastEvent.Phase != model.PhaseFinished || !errors.Is(lastEvent.Err, checkpoint.ErrNotFound) {
		t.Fatalf("expected finished event with ErrNotFound, got: %+v", lastEvent)
	}
}
