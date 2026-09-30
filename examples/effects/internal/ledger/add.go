package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

var ErrRequestMismatch = errors.New("receipt key reused with a different increment")

type Key struct{ Namespace, RunID, CallID string }

// Add atomically increments a namespace's counter and saves the original result.
// Replay returns that result even if other requests have advanced the counter.
// The namespace versions this application's operation and request semantics.
func (l *Ledger) Add(ctx context.Context, key Key, delta int64) (int64, error) {
	if key.Namespace == "" || key.RunID == "" || key.CallID == "" {
		return 0, errors.New("namespace, run ID and call ID are required")
	}
	// BEGIN IMMEDIATE serializes writers across connections, before the lookup.
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var recordedDelta, value int64
	err = tx.QueryRowContext(ctx, `SELECT delta, value FROM receipts
		WHERE namespace=? AND run_id=? AND call_id=?`, key.Namespace, key.RunID, key.CallID).Scan(&recordedDelta, &value)
	if err == nil {
		if recordedDelta != delta {
			return 0, fmt.Errorf("%w: %s/%s/%s", ErrRequestMismatch, key.Namespace, key.RunID, key.CallID)
		}
		return value, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO counters(namespace,value) VALUES(?,0) ON CONFLICT DO NOTHING", key.Namespace); err != nil {
		return 0, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT value FROM counters WHERE namespace=?", key.Namespace).Scan(&value); err != nil {
		return 0, err
	}
	if (delta > 0 && value > math.MaxInt64-delta) || (delta < 0 && value < math.MinInt64-delta) {
		return 0, errors.New("counter overflow")
	}
	value += delta
	if _, err = tx.ExecContext(ctx, "UPDATE counters SET value=? WHERE namespace=?", value, key.Namespace); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO receipts(namespace,run_id,call_id,delta,value) VALUES(?,?,?,?,?)", key.Namespace, key.RunID, key.CallID, delta, value); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return value, nil
}
