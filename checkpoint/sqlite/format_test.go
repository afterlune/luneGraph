package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
)

func TestFormatEightRejectsFormatSevenWritesAndLoads(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "runs.db"), checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cp := graph.Checkpoint[int]{FormatVersion: graph.CheckpointFormatVersion, RunID: "format", MachineID: "m", Revision: 1, NextID: 3,
		Invocations: []graph.Invocation[int]{{ID: "i1", CallID: "c2", Node: "work", Status: graph.InvocationReady, State: 1}}}
	old := cp
	old.FormatVersion = 7
	if err := store.Create(ctx, old); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("old Create: %v", err)
	}
	if err := store.Create(ctx, cp); err != nil {
		t.Fatal(err)
	}
	old.Revision = 2
	if err := store.Commit(ctx, 1, old.Change()); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatalf("old Commit: %v", err)
	}
	h, err := store.LoadHeader(ctx, "format")
	if err != nil || h.Revision != 1 || h.FormatVersion != 8 {
		t.Fatalf("rejected write changed header: %+v %v", h, err)
	}
	h.FormatVersion = 7
	payload, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, "UPDATE checkpoints SET payload=? WHERE run_id=?", payload, "format"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(ctx, "format"); !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatalf("old Load: %v", err)
	}
	if _, err := store.LoadHeader(ctx, "format"); !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatalf("old LoadHeader: %v", err)
	}
}
