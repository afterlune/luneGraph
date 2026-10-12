package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
)

func TestForeignKeysSurviveConnectionReplacement(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "rows.db"), checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cp := graph.Checkpoint[int]{FormatVersion: graph.CheckpointFormatVersion, RunID: "r", MachineID: "m", Revision: 1, Invocations: []graph.Invocation[int]{{ID: "i1", State: 1}}, Groups: []graph.ActivationGroup{{ID: "g3", Branches: []graph.ActivationBranch{{InvocationID: "i1"}}}}}
	if err := s.Create(ctx, cp); err != nil {
		t.Fatal(err)
	}
	// Force the next Store call to use a fresh physical connection.
	s.db.SetMaxIdleConns(0)
	if err := s.Delete(ctx, cp.RunID); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"invocations", "groups"} {
		var n int
		if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 0 {
			t.Fatal(table, n, err)
		}
	}
}

func TestSchemaRejectsMissingOrIncorrectCascade(t *testing.T) {
	for _, foreignKey := range []string{"", ",FOREIGN KEY(run_id) REFERENCES checkpoints(run_id) ON DELETE RESTRICT"} {
		path := filepath.Join(t.TempDir(), "wrong.db")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`CREATE TABLE checkpoints(run_id TEXT PRIMARY KEY,machine_id TEXT NOT NULL,revision TEXT NOT NULL,payload BLOB NOT NULL,final BLOB) WITHOUT ROWID`,
			`CREATE TABLE invocations(run_id TEXT NOT NULL,id TEXT NOT NULL,metadata BLOB NOT NULL,state BLOB NOT NULL,PRIMARY KEY(run_id,id)` + foreignKey + `) WITHOUT ROWID`,
			`CREATE TABLE groups(run_id TEXT NOT NULL,id TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(run_id,id),FOREIGN KEY(run_id) REFERENCES checkpoints(run_id) ON DELETE CASCADE) WITHOUT ROWID`,
			`PRAGMA user_version=2`,
		} {
			if _, err := db.Exec(query); err != nil {
				db.Close()
				t.Fatal(err)
			}
		}
		db.Close()
		if s, err := Open(context.Background(), path, checkpoint.JSON[int]{}); !errors.Is(err, ErrSchemaVersion) {
			if s != nil {
				s.Close()
			}
			t.Fatal(err)
		}
	}
}
