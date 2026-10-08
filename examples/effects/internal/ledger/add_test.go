package ledger

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"sync"
	"testing"
)

func openTest(t *testing.T, path string) *Ledger {
	t.Helper()
	l, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	})
	return l
}

func TestReplayAndIsolation(t *testing.T) {
	l := openTest(t, filepath.Join(t.TempDir(), "effects.db"))
	ctx := context.Background()
	key := Key{"v1", "run", "call"}
	for _, request := range []struct {
		key         Key
		delta, want int64
	}{
		{key, 3, 3}, {Key{"v1", "other", "call"}, 4, 7}, {key, 3, 3},
		{Key{"v2", "run", "call"}, 2, 2}, {Key{"v1", "run", "next"}, -1, 6},
	} {
		got, err := l.Add(ctx, request.key, request.delta)
		if err != nil || got != request.want {
			t.Fatalf("Add(%+v) = %d, %v; want %d", request, got, err, request.want)
		}
	}
	if _, err := l.Add(ctx, key, 8); !errors.Is(err, ErrRequestMismatch) {
		t.Fatalf("mismatched replay: %v", err)
	}
	var value, receipts int64
	if err := l.db.QueryRow("SELECT value FROM counters WHERE namespace='v1'").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if err := l.db.QueryRow("SELECT count(*) FROM receipts WHERE namespace='v1'").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if value != 6 || receipts != 3 {
		t.Fatalf("value=%d receipts=%d", value, receipts)
	}
}

func TestOpenRejectsInvalidPathsAndInitializationFailures(t *testing.T) {
	ctx := context.Background()
	for _, path := range []string{"", "  ", ":memory:", "file:effects.db"} {
		if _, err := Open(ctx, path); err == nil {
			t.Errorf("Open accepted path %q", path)
		}
	}
	missingParent := filepath.Join(t.TempDir(), "missing", "effects.db")
	if _, err := Open(ctx, missingParent); err == nil {
		t.Fatal("Open accepted a path with a missing parent directory")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Open(canceled, filepath.Join(t.TempDir(), "canceled.db")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open with canceled context = %v", err)
	}
}

func TestAddRejectsIncompleteReceiptKeys(t *testing.T) {
	l := openTest(t, filepath.Join(t.TempDir(), "effects.db"))
	for _, key := range []Key{
		{},
		{RunID: "run", CallID: "call"},
		{Namespace: "v1", CallID: "call"},
		{Namespace: "v1", RunID: "run"},
	} {
		if _, err := l.Add(context.Background(), key, 1); err == nil {
			t.Errorf("Add accepted incomplete key %+v", key)
		}
	}
}

func TestConcurrentDuplicateAcrossConnections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "effects.db")
	ledgers := []*Ledger{openTest(t, path), openTest(t, path)}
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			got, err := ledgers[i%2].Add(context.Background(), Key{"v1", "run", "call"}, 7)
			if err != nil || got != 7 {
				t.Errorf("concurrent Add = %d, %v", got, err)
			}
		})
	}
	wg.Wait()
	var value, receipts int64
	if err := ledgers[0].db.QueryRow("SELECT value FROM counters WHERE namespace='v1'").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if err := ledgers[0].db.QueryRow("SELECT count(*) FROM receipts").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if value != 7 || receipts != 1 {
		t.Fatalf("value=%d receipts=%d", value, receipts)
	}
}

func TestReceiptFailureRollsBackCounter(t *testing.T) {
	l := openTest(t, filepath.Join(t.TempDir(), "effects.db"))
	ctx := context.Background()
	if _, err := l.Add(ctx, Key{"v1", "run", "first"}, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := l.db.Exec(`CREATE TRIGGER reject_receipt BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT,'receipt failure'); END`); err != nil {
		t.Fatal(err)
	}
	key := Key{"v1", "run", "second"}
	if _, err := l.Add(ctx, key, 5); err == nil {
		t.Fatal("receipt write succeeded")
	}
	var value, receipts int64
	if err := l.db.QueryRow("SELECT value FROM counters WHERE namespace='v1'").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if err := l.db.QueryRow("SELECT count(*) FROM receipts").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if value != 2 || receipts != 1 {
		t.Fatalf("partial transaction: value=%d receipts=%d", value, receipts)
	}
	if _, err := l.db.Exec("DROP TRIGGER reject_receipt"); err != nil {
		t.Fatal(err)
	}
	if got, err := l.Add(ctx, key, 5); err != nil || got != 7 {
		t.Fatalf("retry = %d, %v", got, err)
	}
}

func TestCounterUpdateFailureRollsBackNewNamespace(t *testing.T) {
	l := openTest(t, filepath.Join(t.TempDir(), "effects.db"))
	if _, err := l.db.Exec(`CREATE TRIGGER reject_counter_update BEFORE UPDATE ON counters BEGIN SELECT RAISE(ABORT,'counter failure'); END`); err != nil {
		t.Fatal(err)
	}
	key := Key{"v1", "run", "call"}
	if _, err := l.Add(context.Background(), key, 5); err == nil {
		t.Fatal("counter update failure was ignored")
	}
	var counters, receipts int
	if err := l.db.QueryRow("SELECT count(*) FROM counters").Scan(&counters); err != nil {
		t.Fatal(err)
	}
	if err := l.db.QueryRow("SELECT count(*) FROM receipts").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if counters != 0 || receipts != 0 {
		t.Fatalf("failed transaction left counters=%d receipts=%d", counters, receipts)
	}
	if _, err := l.db.Exec("DROP TRIGGER reject_counter_update"); err != nil {
		t.Fatal(err)
	}
	if got, err := l.Add(context.Background(), key, 5); err != nil || got != 5 {
		t.Fatalf("retry after update failure = %d, %v", got, err)
	}
}

func TestOverflowAndCanceledRequest(t *testing.T) {
	l := openTest(t, filepath.Join(t.TempDir(), "effects.db"))
	ctx := context.Background()
	if _, err := l.Add(ctx, Key{"v1", "run", "first"}, math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Add(ctx, Key{"v1", "run", "overflow"}, 1); err == nil {
		t.Fatal("overflow accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.Add(canceled, Key{"v1", "run", "canceled"}, -1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Add: %v", err)
	}
	var count int
	if err := l.db.QueryRow("SELECT count(*) FROM receipts").Scan(&count); err != nil || count != 1 {
		t.Fatalf("receipts=%d, %v", count, err)
	}
}
