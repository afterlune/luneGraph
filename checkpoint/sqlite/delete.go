package sqlite

import (
	"context"
	"errors"
	"fmt"
	"github.com/afterlune/luneGraph/internal/model"
	"strings"
)

const deletionChunkSize = 256

// DeleteMany removes all named runs in one transaction, including batches
// larger than a SQL chunk. Missing runs are harmless. Commit errors may leave
// the atomic submission outcome unknown; reload before retrying.
func (s *Store[S]) DeleteMany(ctx context.Context, runIDs []string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return errors.New("store is nil")
	}
	ids, err := model.DeletionIDs(runIDs)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin batch deletion: %w", err)
	}
	defer tx.Rollback()
	for len(ids) > 0 {
		n := min(len(ids), deletionChunkSize)
		args := make([]any, n)
		for i, id := range ids[:n] {
			args[i] = id
		}
		query := "DELETE FROM checkpoints WHERE run_id IN (" + strings.TrimSuffix(strings.Repeat("?,", n), ",") + ")"
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("delete checkpoint batch: %w", err)
		}
		ids = ids[n:]
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch deletion: %w", err)
	}
	return nil
}
