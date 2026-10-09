package memory

import (
	"context"
	"errors"
	"github.com/afterlune/luneGraph/internal/model"
)

// DeleteMany atomically removes runs; missing runs are already deleted.
func (s *Store[S]) DeleteMany(ctx context.Context, runIDs []string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil {
		return errors.New("store is nil")
	}
	ids, err := model.DeletionIDs(runIDs)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		delete(s.values, id)
	}
	return nil
}
