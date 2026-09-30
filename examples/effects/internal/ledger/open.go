// Package ledger demonstrates application-owned transactional effect receipts.
// It is an example, not a graph persistence implementation.
package ledger

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

type Ledger struct{ db *sql.DB }

// Open opens a local effect database. Its parent directory must exist.
func Open(ctx context.Context, path string) (*Ledger, error) {
	if strings.TrimSpace(path) == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, errors.New("ledger requires a local file path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	name := filepath.ToSlash(abs)
	if filepath.VolumeName(abs) != "" {
		name = "/" + name
	}
	u := url.URL{Scheme: "file", Path: name}
	q := url.Values{"_txlock": {"immediate"}, "_pragma": {"busy_timeout(5000)", "journal_mode(WAL)", "synchronous(FULL)"}}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS counters (
		namespace TEXT PRIMARY KEY, value INTEGER NOT NULL
	);
	CREATE TABLE IF NOT EXISTS receipts (
		namespace TEXT NOT NULL, run_id TEXT NOT NULL, call_id TEXT NOT NULL,
		delta INTEGER NOT NULL, value INTEGER NOT NULL,
		PRIMARY KEY(namespace, run_id, call_id)
	)`); err != nil {
		db.Close()
		return nil, err
	}
	return &Ledger{db: db}, nil
}

func (l *Ledger) Close() error { return l.db.Close() }
