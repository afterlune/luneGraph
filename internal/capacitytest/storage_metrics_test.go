package capacitytest

import (
	"context"
	"database/sql"
	"errors"
	"os"
)

type storageSample struct {
	Round         int            `json:"round"`
	Created       int            `json:"created"`
	Deleted       int            `json:"deleted"`
	Retained      int            `json:"retained_completed"`
	CreateMS      float64        `json:"create_ms"`
	DeleteMS      float64        `json:"delete_ms"`
	Resources     resourceSample `json:"resources_gc"`
	DatabaseBytes int64          `json:"database_bytes"`
	WALBytes      int64          `json:"wal_bytes"`
	Rows          int64          `json:"rows"`
	PayloadBytes  int64          `json:"payload_bytes"`
	Pages         int64          `json:"pages"`
	FreePages     int64          `json:"free_pages"`
}

func fileBytes(path string) (int64, error) {
	s, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return s.Size(), nil
}

func sampleStorage(ctx context.Context, path string, db *sql.DB, s *storageSample) error {
	var err error
	s.DatabaseBytes, err = fileBytes(path)
	if err != nil {
		return err
	}
	s.WALBytes, err = fileBytes(path + "-wal")
	if err != nil {
		return err
	}
	for query, dst := range map[string]*int64{"SELECT count(*) FROM checkpoints": &s.Rows, "SELECT coalesce(sum(length(payload)),0) FROM checkpoints": &s.PayloadBytes, "PRAGMA page_count": &s.Pages, "PRAGMA freelist_count": &s.FreePages} {
		if err := db.QueryRowContext(ctx, query).Scan(dst); err != nil {
			return err
		}
	}
	return nil
}
