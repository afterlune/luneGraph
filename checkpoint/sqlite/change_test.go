package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"maps"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/memory"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func rejectChange(t *testing.T, path, statement string, cp graph.Checkpoint[state], change graph.Change[state], expectedRevision uint64) {
	t.Helper()
	store := openStore(t, path)
	defer store.Close()
	if err := store.Create(context.Background(), cp); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(statement); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	if err := store.Commit(context.Background(), expectedRevision, change); err == nil {
		t.Fatal("triggered change succeeded")
	}
	saved, err := store.Load(context.Background(), cp.RunID)
	if err != nil || saved.Revision != cp.Revision {
		t.Fatal(saved, err)
	}
}

func TestSQLiteDeltaDeleteAndUpsertStatementFailures(t *testing.T) {
	inv := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "delete-inv", MachineID: "m", Revision: 1, Invocations: []graph.Invocation[state]{{ID: "i1", State: state{Values: map[string]int{"n": 1}}}}}
	h := inv.Header()
	h.Revision = 2
	rejectChange(t, databasePath(t), `CREATE TRIGGER reject_delete BEFORE DELETE ON invocations BEGIN SELECT RAISE(ABORT,'delete'); END`, inv, graph.Change[state]{Header: h, DeleteInvocations: []string{"i1"}}, 1)
	g := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "delete-group", MachineID: "m", Revision: 1, Groups: []graph.ActivationGroup{{ID: "g3"}}}
	h = g.Header()
	h.Revision = 2
	rejectChange(t, databasePath(t), `CREATE TRIGGER reject_delete BEFORE DELETE ON groups BEGIN SELECT RAISE(ABORT,'delete'); END`, g, graph.Change[state]{Header: h, DeleteGroups: []string{"g3"}}, 1)
	base := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "insert-inv", MachineID: "m", Revision: 1}
	next := base.Header()
	next.Revision = 2
	change := graph.Change[state]{Header: next, UpsertInvocations: []graph.Invocation[state]{{ID: "i3", State: state{Values: map[string]int{"n": 3}}}}}
	rejectChange(t, databasePath(t), `CREATE TRIGGER reject_insert BEFORE INSERT ON invocations BEGIN SELECT RAISE(ABORT,'insert'); END`, base, change, 1)
}

type countingCodec struct{ count *atomic.Int32 }

func (c countingCodec) Append(dst []byte, s state) ([]byte, error) {
	c.count.Add(1)
	return (checkpoint.JSON[state]{}).Append(dst, s)
}
func (c countingCodec) Unmarshal(data []byte) (state, error) {
	return (checkpoint.JSON[state]{}).Unmarshal(data)
}

type finalCodec struct{ checkpoint.JSON[state] }

func (finalCodec) Append(dst []byte, value state) ([]byte, error) {
	if value.Values["reject"] != 0 {
		return append(dst, `{"partial":`...), errors.New("final encoding failed")
	}
	return (checkpoint.JSON[state]{}).Append(dst, value)
}

func TestFinalStateCodecFailureDoesNotCreateRun(t *testing.T) {
	ctx := context.Background()
	path := databasePath(t)
	s, err := sqlite.Open(ctx, path, finalCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	final := state{Values: map[string]int{"reject": 1}}
	cp := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "codec-error", MachineID: "m", Revision: 1, Completed: true, Final: &final}
	if err := s.Create(ctx, cp); err == nil {
		t.Fatal("partial final encoding accepted")
	}
	if _, err := s.Load(ctx, cp.RunID); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatal("codec error left a run", err)
	}
}

func TestIncrementalCommitTouchesOnlyChangedStates(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var count atomic.Int32
			var store graph.Store[state]
			if kind == "memory" {
				s, err := memory.New(func(s state) (state, error) { count.Add(1); s.Values = maps.Clone(s.Values); return s, nil })
				if err != nil {
					t.Fatal(err)
				}
				store = s
			} else {
				s, err := sqlite.Open(ctx, databasePath(t), countingCodec{&count})
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				store = s
			}
			cp := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "incremental", MachineID: "machine", Revision: 1, NextID: 100}
			for i := range 32 {
				cp.Invocations = append(cp.Invocations, graph.Invocation[state]{ID: string(rune('a' + i)), State: state{Values: map[string]int{"n": i}}})
			}
			if err := store.Create(ctx, cp); err != nil {
				t.Fatal(err)
			}
			count.Store(0)
			header := cp.Header()
			header.Revision = 2
			changed := cp.Invocations[0]
			changed.State = state{Values: map[string]int{"n": 99}}
			if err := store.Commit(ctx, 1, graph.Change[state]{Header: header, UpsertInvocations: []graph.Invocation[state]{changed}}); err != nil {
				t.Fatal(err)
			}
			if count.Load() != 1 {
				t.Fatalf("processed %d states for one update", count.Load())
			}
			changed.State.Values["n"] = -1
			saved, err := store.Load(ctx, cp.RunID)
			if err != nil || len(saved.Invocations) != 32 {
				t.Fatal(saved, err)
			}
			for _, v := range saved.Invocations {
				if v.ID == changed.ID && v.State.Values["n"] != 99 {
					t.Fatal("committed state aliased caller")
				}
			}
			header.Revision = 3
			if err := store.Commit(ctx, 2, graph.Change[state]{Header: header, DeleteInvocations: []string{changed.ID}}); err != nil {
				t.Fatal(err)
			}
			saved, err = store.Load(ctx, cp.RunID)
			if err != nil || len(saved.Invocations) != 31 {
				t.Fatal(saved, err)
			}
			if err := store.Commit(ctx, 2, graph.Change[state]{Header: header}); !errors.Is(err, graph.ErrConflict) {
				t.Fatal(err)
			}
		})
	}
}

func TestSQLiteDeltaRollsBackAllTables(t *testing.T) {
	ctx := context.Background()
	path := databasePath(t)
	store := openStore(t, path)
	cp := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "rollback", MachineID: "m", Revision: 1, Invocations: []graph.Invocation[state]{{ID: "i1", State: state{Values: map[string]int{"n": 1}}}}}
	if err := store.Create(ctx, cp); err != nil {
		t.Fatal(err)
	}
	corruptRow(t, path, `CREATE TRIGGER reject_group BEFORE INSERT ON groups BEGIN SELECT RAISE(ABORT,'group rejected'); END`)
	cp.Revision = 2
	changed := cp.Invocations[0]
	changed.State = state{Values: map[string]int{"n": 2}}
	err := store.Commit(ctx, 1, graph.Change[state]{Header: cp.Header(), UpsertInvocations: []graph.Invocation[state]{changed}, UpsertGroups: []graph.ActivationGroup{{ID: "g1"}}})
	if err == nil {
		t.Fatal("transaction succeeded")
	}
	saved, err := store.Load(ctx, cp.RunID)
	if err != nil || saved.Revision != 1 || saved.Invocations[0].State.Values["n"] != 1 || len(saved.Groups) != 0 {
		t.Fatal(saved, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openStore(t, path)
	saved, err = store.Load(ctx, cp.RunID)
	if err != nil || saved.Revision != 1 {
		t.Fatal(saved, err)
	}
	// Corrupt metadata is not silently accepted as another invocation.
	bad, _ := json.Marshal(map[string]string{"ID": "other"})
	corruptRow(t, path, "UPDATE invocations SET metadata=? WHERE run_id=?", bad, cp.RunID)
	if _, err := store.Load(ctx, cp.RunID); !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestSQLiteCorruptGroupsAndInvalidAdvance(t *testing.T) {
	ctx := context.Background()
	path := databasePath(t)
	store := openStore(t, path)
	cp := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "groups", MachineID: "m", Revision: 1, Steps: 3, NextID: 10, Groups: []graph.ActivationGroup{{ID: "g3"}}}
	if err := store.Create(ctx, cp); err != nil {
		t.Fatal(err)
	}
	header := cp.Header()
	header.Revision = 2
	header.Steps = 2
	if err := store.Commit(ctx, 1, graph.Change[state]{Header: header}); !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
	corruptRow(t, path, "UPDATE groups SET payload=? WHERE run_id=?", []byte(`{"ID":"other"}`), cp.RunID)
	if _, err := store.Load(ctx, cp.RunID); !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatal(err)
	}
	corruptRow(t, path, "UPDATE checkpoints SET payload=? WHERE run_id=?", []byte(`{`), cp.RunID)
	header.Steps = 3
	if err := store.Commit(ctx, 1, graph.Change[state]{Header: header}); !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatal(err)
	}
}
