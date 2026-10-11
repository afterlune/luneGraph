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

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"

	sqliteDriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrSchemaVersion reports an unsupported SQLite checkpoint schema.
var ErrSchemaVersion = errors.New("unsupported checkpoint database schema")

const schemaVersion = 2

// Store is a concurrent-safe, persistent implementation of graph.Store.
type Store[S any] struct {
	db             *sql.DB
	codec          checkpoint.StateCodec[S]
	payloadBuffers sync.Pool
}

// Open opens or creates a local SQLite database. Its parent directory must
// already exist. Close the returned Store when it is no longer needed.
func Open[S any](ctx context.Context, dbPath string, codec checkpoint.StateCodec[S]) (*Store[S], error) {
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
	params.Set("_txlock", "immediate")
	params.Set("_journal_mode", "WAL")
	params.Set("_synchronous", "FULL")
	params.Set("_foreign_keys", "ON")
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

func nilCodec[S any](codec checkpoint.StateCodec[S]) bool {
	value := reflect.ValueOf(codec)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func initialize(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return err
	}
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version != 0 && version != schemaVersion {
		return fmt.Errorf("%w: version %d", ErrSchemaVersion, version)
	}
	if version == 0 {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, query := range []string{
			`CREATE TABLE IF NOT EXISTS checkpoints (run_id TEXT PRIMARY KEY,machine_id TEXT NOT NULL,revision TEXT NOT NULL,payload BLOB NOT NULL,final BLOB) WITHOUT ROWID`,
			`CREATE TABLE IF NOT EXISTS invocations (run_id TEXT NOT NULL,id TEXT NOT NULL,metadata BLOB NOT NULL,state BLOB NOT NULL,PRIMARY KEY(run_id,id),FOREIGN KEY(run_id) REFERENCES checkpoints(run_id) ON DELETE CASCADE) WITHOUT ROWID`,
			`CREATE TABLE IF NOT EXISTS groups (run_id TEXT NOT NULL,id TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(run_id,id),FOREIGN KEY(run_id) REFERENCES checkpoints(run_id) ON DELETE CASCADE) WITHOUT ROWID`,
		} {
			if _, err := tx.ExecContext(ctx, query); err != nil {
				return err
			}
		}
		if err := checkSchemaTx(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "PRAGMA user_version = 2"); err != nil {
			return err
		}
		return tx.Commit()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return checkSchemaTx(ctx, tx)
}
func checkSchemaTx(ctx context.Context, tx *sql.Tx) error {
	expected := map[string][]string{"checkpoints": {"run_id:TEXT", "machine_id:TEXT", "revision:TEXT", "payload:BLOB", "final:BLOB"}, "invocations": {"run_id:TEXT", "id:TEXT", "metadata:BLOB", "state:BLOB"}, "groups": {"run_id:TEXT", "id:TEXT", "payload:BLOB"}}
	for table, columns := range expected {
		rows, err := tx.QueryContext(ctx, "PRAGMA table_info("+table+")")
		if err != nil {
			return err
		}
		index := 0
		for rows.Next() {
			var cid, notNull, primary int
			var name, kind string
			var defaultValue sql.NullString
			if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primary); err != nil {
				rows.Close()
				return err
			}
			wantPK := 0
			if index == 0 {
				wantPK = 1
			} else if table != "checkpoints" && index == 1 {
				wantPK = 2
			}
			wantNull := 1
			if table == "checkpoints" && index == 4 {
				wantNull = 0
			}
			if index >= len(columns) || columns[index] != name+":"+strings.ToUpper(kind) || cid != index || primary != wantPK || notNull != wantNull || defaultValue.Valid {
				rows.Close()
				return ErrSchemaVersion
			}
			index++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if index != len(columns) {
			return ErrSchemaVersion
		}
		if table != "checkpoints" {
			if err := checkForeignKey(ctx, tx, table); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkForeignKey(ctx context.Context, tx *sql.Tx, table string) error {
	rows, err := tx.QueryContext(ctx, "PRAGMA foreign_key_list("+table+")")
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id, sequence int
		var target, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &sequence, &target, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return err
		}
		if sequence != 0 || target != "checkpoints" || from != "run_id" || to != "run_id" || onUpdate != "NO ACTION" || onDelete != "CASCADE" || match != "NONE" {
			return ErrSchemaVersion
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != 1 {
		return ErrSchemaVersion
	}
	return nil
}

// Close releases SQLite connections. It does not remove the database.
func (s *Store[S]) Close() error {
	if s == nil || s.db == nil {
		return errors.New("store is nil")
	}
	// Callers must drain users before closing. database/sql owns close coordination.
	return s.db.Close()
}

var _ graph.Store[int] = (*Store[int])(nil)
