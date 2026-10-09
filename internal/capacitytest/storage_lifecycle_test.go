package capacitytest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func storageLifecycle(t *testing.T, kind string, keepAll bool, rounds, batch, callers int) {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
	defer stop()
	var store graph.Store[state]
	var disk *sqlite.Store[state]
	var db *sql.DB
	var path string
	if kind == "sqlite" {
		path = filepath.Join(t.TempDir(), "lifecycle.db")
		var err error
		disk, err = sqlite.Open(ctx, path, checkpoint.JSON[state]{})
		if err != nil {
			t.Fatal(err)
		}
		store = disk
		t.Cleanup(func() {
			if db != nil {
				db.Close()
			}
			if disk != nil {
				disk.Close()
			}
		})
		db, err = sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
	} else {
		store = openStore(t, kind)
	}
	r := compile(t, chainGraph(t, 2))
	waiting := waitingRunner(t)
	const paused = 16
	for i := range paused {
		id := fmt.Sprintf("paused-%d", i)
		if _, err := waiting.Start(ctx, id, initial(id), graph.Options[state]{Store: store}); err != nil {
			t.Fatal(err)
		}
	}
	queue := make([][]string, 0, rounds)
	var deleted int
	for round := range rounds {
		ids := make([]string, batch)
		for i := range ids {
			ids[i] = fmt.Sprintf("completed-%02d-%02d", round, i)
		}
		started := time.Now()
		pool := newBatchPool(batch, callers, func(i int) error {
			out, err := r.Start(ctx, ids[i], initial(ids[i]), graph.Options[state]{Store: store})
			if err != nil {
				return err
			}
			if out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || out.Checkpoint.Final.Total != 2 {
				return fmt.Errorf("invalid completion: %+v", out)
			}
			return nil
		})
		err := pool.batch()
		pool.close()
		if err != nil {
			t.Fatal(err)
		}
		createMS := float64(time.Since(started)) / float64(time.Millisecond)
		queue = append(queue, ids)
		id := fmt.Sprintf("paused-%d", round%paused)
		cp, err := store.Load(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := waiting.Recover(ctx, id, interruptionInputs(cp), graph.Options[state]{Store: store}); err != nil {
			t.Fatal(err)
		}
		var deleteMS float64
		if !keepAll && len(queue) > 2 {
			old := queue[0]
			saved, err := store.Load(ctx, old[0])
			if err != nil {
				t.Fatal(err)
			}
			started = time.Now()
			if err := store.DeleteMany(ctx, old); err != nil {
				t.Fatal(err)
			}
			deleteMS = float64(time.Since(started)) / float64(time.Millisecond)
			deleted += len(old)
			queue = queue[1:]
			for _, id := range old {
				if _, err := store.Load(ctx, id); !errors.Is(err, checkpoint.ErrNotFound) {
					t.Fatal("deleted load", err)
				}
			}
			if _, err := r.Recover(ctx, old[0], nil, graph.Options[state]{Store: store}); !errors.Is(err, checkpoint.ErrNotFound) {
				t.Fatal("deleted recovery", err)
			}
			expected := saved.Revision
			saved.Revision++
			if err := store.CompareAndSwap(ctx, expected, saved); !errors.Is(err, graph.ErrConflict) {
				t.Fatal("deleted CAS resurrected run", err)
			}
		}
		for _, ids := range queue {
			for _, id := range ids {
				cp, err := store.Load(ctx, id)
				if err != nil || cp.Final == nil || cp.Final.Owner != id || cp.Final.Values["steps"] != 2 || cp.Steps != 2 || cp.Revision != 3 {
					t.Fatalf("retained result: %+v %v", cp, err)
				}
			}
		}
		for i := range paused {
			id := fmt.Sprintf("paused-%d", i)
			cp, err := store.Load(ctx, id)
			if err != nil || len(cp.Invocations) != 1 || cp.Invocations[0].Status != graph.InvocationWaiting || cp.Invocations[0].State.Owner != id {
				t.Fatalf("paused isolation: %+v %v", cp, err)
			}
		}
		s := storageSample{Round: round + 1, Created: (round + 1) * batch, Deleted: deleted, Retained: (round+1)*batch - deleted, CreateMS: createMS, DeleteMS: deleteMS, Resources: resources(true)}
		if db != nil {
			if err := sampleStorage(ctx, path, db, &s); err != nil {
				t.Fatal(err)
			}
			if s.Rows != int64(s.Retained+paused) {
				t.Fatal("incorrect row count", s)
			}
		}
		data, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(string(data))
	}
	if db != nil {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = nil
		if err := disk.Close(); err != nil {
			t.Fatal(err)
		}
		disk = nil
		main, err := fileBytes(path)
		if err != nil {
			t.Fatal(err)
		}
		wal, err := fileBytes(path + "-wal")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("after_all_connections_close database_bytes=%d wal_bytes=%d", main, wal)
		var errOpen error
		disk, errOpen = sqlite.Open(ctx, path, checkpoint.JSON[state]{})
		if errOpen != nil {
			t.Fatal(errOpen)
		}
		store = disk
		for _, ids := range queue {
			for _, id := range ids {
				out, err := r.Recover(ctx, id, nil, graph.Options[state]{Store: store})
				if err != nil || out.Status != graph.StatusCompleted {
					t.Fatal("reopen retained recovery", err)
				}
			}
		}
		if !keepAll {
			for round := 0; round < rounds-2; round++ {
				for i := range batch {
					id := fmt.Sprintf("completed-%02d-%02d", round, i)
					if _, err := store.Load(ctx, id); !errors.Is(err, checkpoint.ErrNotFound) {
						t.Fatal("reopen resurrected deletion", id, err)
					}
				}
			}
		}
		for i := range paused {
			id := fmt.Sprintf("paused-%d", i)
			out, err := waiting.Recover(ctx, id, nil, graph.Options[state]{Store: store})
			if err != nil || out.Status != graph.StatusWaiting || out.Checkpoint.Invocations[0].State.Owner != id {
				t.Fatal("reopen paused recovery", id, err)
			}
		}
	}
	runtime.KeepAlive(store)
}

func TestCapacityStorageLifecycle(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, keepAll := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/keep_all=%t", kind, keepAll), func(t *testing.T) { storageLifecycle(t, kind, keepAll, 3, 8, 2) })
		}
	}
}

func TestCapacityStorageLifecycleMeasurement(t *testing.T) {
	if os.Getenv("LUNEGRAPH_STORAGE_MEASURE") != "1" {
		t.Skip("set LUNEGRAPH_STORAGE_MEASURE=1 for 16 rounds of 64 executions")
	}
	t.Logf("Go=%s OS=%s arch=%s GOMAXPROCS=%d rounds=16 batch=64 callers=8 paused=16", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0))
	for _, kind := range []string{"memory", "sqlite"} {
		for _, keepAll := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/keep_all=%t", kind, keepAll), func(t *testing.T) { storageLifecycle(t, kind, keepAll, 16, 64, 8) })
		}
	}
}
