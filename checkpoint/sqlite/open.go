// Package sqlite stores the latest checkpoint for each run in a local SQLite file.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	graph "lune-graph"
	"lune-graph/checkpoint"

	sqliteDriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrSchemaVersion reports an unsupported SQLite checkpoint schema.
var ErrSchemaVersion = errors.New("unsupported checkpoint database schema")

const schemaVersion = 1

// Store is a concurrent-safe, persistent implementation of graph.Store.
type Store[S any] struct {
	db             *sql.DB
	codec          checkpoint.Codec[S]
	payloadBuffers sync.Pool
}

// Open opens or creates a local SQLite database. Its parent directory must
// already exist. Close the returned Store when it is no longer needed.
func Open[S any](ctx context.Context, dbPath string, codec checkpoint.Codec[S]) (*Store[S], error) {
	if ctx == nil {
		return nil, errors.New("context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if codec == nil || nilCodec(codec) {
		return nil, errors.New("checkpoint codec is required")
	}
	if strings.TrimSpace(dbPath) == "" || dbPath == ":memory:" || strings.HasPrefix(dbPath, "file:") {
		return nil, errors.New("database path must name a local file")
	}
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, fmt.Errorf("database path: %w", err)
	}
	parent, err := os.Stat(filepath.Dir(abs))
	if err != nil {
		return nil, fmt.Errorf("database parent: %w", err)
	}
	if !parent.IsDir() {
		return nil, errors.New("database parent is not a directory")
	}
	path := filepath.ToSlash(abs)
	if filepath.VolumeName(abs) != "" {
		path = "/" + path
	}
	u := url.URL{Scheme: "file", Path: path}
	params := url.Values{}
	params.Set("_busy_timeout", "5000")
	params.Set("_journal_mode", "WAL")
	params.Set("_synchronous", "FULL")
	u.RawQuery = params.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("open checkpoint database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := openAndInitialize(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store[S]{db: db, codec: codec}, nil
}

// SQLite's busy timeout does not cover every lock encountered while two
// processes first enable WAL or create the schema. Retry only lock contention;
// schema errors and all other failures must reach the caller unchanged.
func openAndInitialize(ctx context.Context, db *sql.DB) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := db.PingContext(ctx)
		if err == nil {
			err = initialize(ctx, db)
		}
		if err == nil {
			return nil
		}
		var sqliteErr *sqliteDriver.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code()&0xff != sqlite3.SQLITE_BUSY {
			return fmt.Errorf("open checkpoint database: %w", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("open checkpoint database: %w", err)
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func nilCodec[S any](codec checkpoint.Codec[S]) bool {
	value := reflect.ValueOf(codec)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func initialize(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read checkpoint schema version: %w", err)
	}
	if version != 0 && version != schemaVersion {
		return fmt.Errorf("%w: version %d", ErrSchemaVersion, version)
	}
	if version == schemaVersion {
		return checkSchema(ctx, db)
	}
	const schema = `CREATE TABLE IF NOT EXISTS checkpoints (
		run_id TEXT PRIMARY KEY,
		machine_id TEXT NOT NULL,
		revision TEXT NOT NULL,
		payload BLOB NOT NULL
	) WITHOUT ROWID`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("create checkpoint schema: %w", err)
	}
	if err := checkSchema(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "PRAGMA user_version = 1"); err != nil {
		return fmt.Errorf("record checkpoint schema version: %w", err)
	}
	return nil
}

func checkSchema(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info(checkpoints)")
	if err != nil {
		return fmt.Errorf("inspect checkpoint schema: %w", err)
	}
	defer rows.Close()
	expected := []struct{ name, kind string }{{"run_id", "TEXT"}, {"machine_id", "TEXT"}, {"revision", "TEXT"}, {"payload", "BLOB"}}
	index := 0
	for rows.Next() {
		var cid, notNull, primary int
		var name, kind string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primary); err != nil {
			return fmt.Errorf("inspect checkpoint schema: %w", err)
		}
		if index >= len(expected) || cid != index || name != expected[index].name || !strings.EqualFold(kind, expected[index].kind) || notNull != 1 || defaultValue.Valid || (index == 0 && primary != 1) || (index != 0 && primary != 0) {
			return ErrSchemaVersion
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("inspect checkpoint schema: %w", err)
	}
	if index != len(expected) {
		return ErrSchemaVersion
	}
	return nil
}

// Close releases SQLite connections. It does not remove the database.
func (s *Store[S]) Close() error {
	if s == nil || s.db == nil {
		return errors.New("store is nil")
	}
	return s.db.Close()
}

var _ graph.Store[int] = (*Store[int])(nil)
