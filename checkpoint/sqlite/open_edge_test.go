package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/sqlite"
)

func TestOpenRejectsInvalidInputs(t *testing.T) {
	path := databasePath(t)
	ctx := context.Background()

	if _, err := sqlite.Open[state](nil, path, checkpoint.JSON[state]{}); err == nil {
		t.Fatal("Open accepted a nil context")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := sqlite.Open(canceled, path, checkpoint.JSON[state]{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open with canceled context = %v", err)
	}
	if _, err := sqlite.Open[state](ctx, path, nil); err == nil {
		t.Fatal("Open accepted a nil codec")
	}
	var nilCodec *checkpoint.JSON[state]
	if _, err := sqlite.Open(ctx, path, nilCodec); err == nil {
		t.Fatal("Open accepted a typed nil codec")
	}

	for _, invalidPath := range []string{"", "  ", ":memory:", "file:relative.db"} {
		if _, err := sqlite.Open(ctx, invalidPath, checkpoint.JSON[state]{}); err == nil {
			t.Errorf("Open accepted path %q", invalidPath)
		}
	}
	missingParent := filepath.Join(t.TempDir(), "missing", "checkpoints.db")
	if _, err := sqlite.Open(ctx, missingParent, checkpoint.JSON[state]{}); err == nil {
		t.Fatal("Open accepted a missing parent directory")
	}
	parentFile := filepath.Join(t.TempDir(), "parent")
	if err := os.WriteFile(parentFile, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlite.Open(ctx, filepath.Join(parentFile, "checkpoints.db"), checkpoint.JSON[state]{}); err == nil {
		t.Fatal("Open accepted a file as the database parent")
	}
}

func TestStoreMethodsRejectNilAndCanceledContextAndNilReceiver(t *testing.T) {
	ctx := context.Background()
	value := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "run", MachineID: "machine-v1", Revision: 1}
	next := value
	next.Revision = 2

	var nilStore *sqlite.Store[state]
	if err := nilStore.Create(ctx, value); err == nil {
		t.Fatal("nil store Create succeeded")
	}
	if _, err := nilStore.Load(ctx, "run"); err == nil {
		t.Fatal("nil store Load succeeded")
	}
	if err := nilStore.CompareAndSwap(ctx, 1, next); err == nil {
		t.Fatal("nil store CompareAndSwap succeeded")
	}
	if err := nilStore.Close(); err == nil {
		t.Fatal("nil store Close succeeded")
	}

	store := openStore(t, databasePath(t))
	var nilContext context.Context
	if err := store.Create(nilContext, value); err == nil {
		t.Fatal("Create accepted a nil context")
	}
	if _, err := store.Load(nilContext, "run"); err == nil {
		t.Fatal("Load accepted a nil context")
	}
	if err := store.CompareAndSwap(nilContext, 1, next); err == nil {
		t.Fatal("CompareAndSwap accepted a nil context")
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.Create(canceled, value); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Create = %v", err)
	}
	if _, err := store.Load(canceled, "run"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Load = %v", err)
	}
	if err := store.CompareAndSwap(canceled, 1, next); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CompareAndSwap = %v", err)
	}
}

func TestOpenRejectsIncompatibleSchemaShape(t *testing.T) {
	path := databasePath(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE checkpoints (
		run_id TEXT NOT NULL,
		machine_id TEXT NOT NULL,
		revision TEXT NOT NULL,
		payload BLOB NOT NULL
	)`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA user_version = 1"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlite.Open(context.Background(), path, checkpoint.JSON[state]{}); !errors.Is(err, sqlite.ErrSchemaVersion) {
		t.Fatalf("incompatible schema shape = %v", err)
	}
}
