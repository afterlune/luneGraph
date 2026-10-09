package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
	"testing"
)

func TestDeleteManyRollbackAcrossChunks(t *testing.T) {
	ctx := context.Background()
	path := databasePath(t)
	store := openStore(t, path)
	ids := make([]string, 513)
	for i := range ids {
		ids[i] = fmt.Sprintf("delete-%03d", i)
		if err := store.Create(ctx, graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: ids[i], MachineID: "m", Revision: 1}); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE TRIGGER reject_batch BEFORE DELETE ON checkpoints WHEN OLD.run_id = 'delete-256' BEGIN SELECT RAISE(ABORT, 'reject second chunk'); END"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteMany(ctx, ids); err == nil {
		t.Fatal("trigger error ignored")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openStore(t, path)
	for _, id := range ids {
		if _, err := store.Load(ctx, id); err != nil {
			t.Fatal("batch partially committed", id, err)
		}
	}
	if _, err := db.ExecContext(ctx, "DROP TRIGGER reject_batch"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteMany(ctx, ids); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteManyInvalidStore(t *testing.T) {
	var absent *sqlite.Store[state]
	if err := absent.DeleteMany(context.Background(), nil); err == nil {
		t.Fatal("nil store accepted")
	}
	store := openStore(t, databasePath(t))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteMany(context.Background(), []string{"run"}); err == nil {
		t.Fatal("closed store accepted")
	}
}
