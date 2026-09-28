package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/sqlite"
	"lune-graph/internal/storetest"
)

type state struct{ Values map[string]int }

func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) graph.Store[storetest.State] {
		path := filepath.Join(t.TempDir(), "contract.db")
		store, err := sqlite.Open(context.Background(), path, checkpoint.JSON[storetest.State]{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func databasePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "checkpoints.db")
}

func openStore(t *testing.T, path string) *sqlite.Store[state] {
	t.Helper()
	store, err := sqlite.Open(context.Background(), path, checkpoint.JSON[state]{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestReopenAndCompareAndSwap(t *testing.T) {
	ctx := context.Background()
	path := databasePath(t)
	store := openStore(t, path)
	first := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "run", MachineID: "machine-v1", Revision: 1,
		Invocations: []graph.Invocation[state]{{ID: "i1", CallID: "c2", State: state{Values: map[string]int{"n": 1}}}}}
	if err := store.Create(ctx, first); err != nil {
		t.Fatal(err)
	}
	first.Invocations[0].State.Values["n"] = 99
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openStore(t, path)
	loaded, err := store.Load(ctx, "run")
	if err != nil || loaded.Invocations[0].CallID != "c2" || loaded.Invocations[0].State.Values["n"] != 1 {
		t.Fatalf("reopened checkpoint = %+v, %v", loaded, err)
	}
	loaded.Invocations[0].State.Values["n"] = 88
	again, err := store.Load(ctx, "run")
	if err != nil || again.Invocations[0].State.Values["n"] != 1 {
		t.Fatalf("independent load = %+v, %v", again, err)
	}
	again.Revision = 2
	again.Invocations[0].State.Values["n"] = 2
	if err := store.CompareAndSwap(ctx, 1, again); err != nil {
		t.Fatal(err)
	}
	if err := store.CompareAndSwap(ctx, 1, again); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("stale CAS = %v", err)
	}
	if err := store.Create(ctx, first); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("duplicate create = %v", err)
	}
	if _, err := store.Load(ctx, "missing"); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("missing checkpoint = %v", err)
	}
	otherMachine := again
	otherMachine.Revision = 3
	otherMachine.MachineID = "other"
	if err := store.CompareAndSwap(ctx, 2, otherMachine); !errors.Is(err, graph.ErrConflict) {
		t.Fatalf("different machine = %v", err)
	}
}

type failingCodec struct{ failure error }

func (c failingCodec) Marshal(graph.Checkpoint[state]) ([]byte, error) {
	return nil, c.failure
}
func (c failingCodec) Unmarshal([]byte) (graph.Checkpoint[state], error) {
	return graph.Checkpoint[state]{}, c.failure
}

func TestCodecFailuresAndCorruptData(t *testing.T) {
	ctx := context.Background()
	path := databasePath(t)
	boom := errors.New("codec failed")
	broken, err := sqlite.Open(ctx, path, failingCodec{failure: boom})
	if err != nil {
		t.Fatal(err)
	}
	initial := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "run", MachineID: "machine-v1", Revision: 1}
	if err := broken.Create(ctx, initial); !errors.Is(err, boom) {
		t.Fatalf("encode error = %v", err)
	}
	if err := broken.Close(); err != nil {
		t.Fatal(err)
	}
	good := openStore(t, path)
	if _, err := good.Load(ctx, "run"); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("failed encode wrote data: %v", err)
	}
	if err := good.Create(ctx, initial); err != nil {
		t.Fatal(err)
	}
	if err := good.Close(); err != nil {
		t.Fatal(err)
	}
	broken, err = sqlite.Open(ctx, path, failingCodec{failure: boom})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := broken.Load(ctx, "run"); !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatalf("decode error = %v", err)
	}
	next := initial
	next.Revision = 2
	if err := broken.CompareAndSwap(ctx, 1, next); !errors.Is(err, boom) {
		t.Fatalf("CAS encode error = %v", err)
	}
	if err := broken.Close(); err != nil {
		t.Fatal(err)
	}
	good = openStore(t, path)
	unchanged, err := good.Load(ctx, "run")
	if err != nil || unchanged.Revision != 1 {
		t.Fatalf("failed CAS changed checkpoint = %+v, %v", unchanged, err)
	}
	if err := good.Close(); err != nil {
		t.Fatal(err)
	}
	corruptRow(t, path, "UPDATE checkpoints SET payload = ? WHERE run_id = ?", []byte("{bad json"), "run")
	good = openStore(t, path)
	if _, err := good.Load(ctx, "run"); !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatalf("corrupt payload = %v", err)
	}
	if err := good.Close(); err != nil {
		t.Fatal(err)
	}
	validPayload, err := (checkpoint.JSON[state]{}).Marshal(initial)
	if err != nil {
		t.Fatal(err)
	}
	corruptRow(t, path, "UPDATE checkpoints SET payload = ? WHERE run_id = ?", validPayload, "run")
	corruptRow(t, path, "UPDATE checkpoints SET revision = ? WHERE run_id = ?", "2", "run")
	good = openStore(t, path)
	if _, err := good.Load(ctx, "run"); !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatalf("corrupt identity = %v", err)
	}
}

func TestUnknownSchemaVersion(t *testing.T) {
	ctx := context.Background()
	path := databasePath(t)
	store := openStore(t, path)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	corruptRow(t, path, "PRAGMA user_version = 99")
	if _, err := sqlite.Open(ctx, path, checkpoint.JSON[state]{}); !errors.Is(err, sqlite.ErrSchemaVersion) {
		t.Fatalf("unknown schema = %v", err)
	}
	missingTable := databasePath(t)
	corruptRow(t, missingTable, "PRAGMA user_version = 1")
	if _, err := sqlite.Open(ctx, missingTable, checkpoint.JSON[state]{}); !errors.Is(err, sqlite.ErrSchemaVersion) {
		t.Fatalf("missing schema table = %v", err)
	}
}

func TestConcurrentOpenFreshDatabase(t *testing.T) {
	path := databasePath(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan error, 4)
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			store, err := sqlite.Open(ctx, path, checkpoint.JSON[int]{})
			if err == nil {
				err = store.Close()
			}
			results <- err
		}()
	}
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent open: %v", err)
		}
	}
}

func TestRevisionUsesFullUint64Range(t *testing.T) {
	ctx := context.Background()
	path := databasePath(t)
	store := openStore(t, path)
	initial := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, RunID: "wide", MachineID: "machine-v1", Revision: 1}
	if err := store.Create(ctx, initial); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	wide := initial
	wide.Revision = math.MaxUint64
	payload, err := (checkpoint.JSON[state]{}).Marshal(wide)
	if err != nil {
		t.Fatal(err)
	}
	corruptRow(t, path, "UPDATE checkpoints SET revision = ?, payload = ? WHERE run_id = ?", strconv.FormatUint(wide.Revision, 10), payload, "wide")
	store = openStore(t, path)
	loaded, err := store.Load(ctx, "wide")
	if err != nil || loaded.Revision != math.MaxUint64 {
		t.Fatalf("wide revision = %+v, %v", loaded, err)
	}
	if err := store.CompareAndSwap(ctx, math.MaxUint64, wide); !errors.Is(err, graph.ErrExecutionLimit) {
		t.Fatalf("revision overflow = %v", err)
	}
}

func corruptRow(t *testing.T, path, statement string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), statement, args...); err != nil {
		t.Fatal(err)
	}
}
