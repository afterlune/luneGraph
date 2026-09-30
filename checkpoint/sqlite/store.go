package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/internal/model"
)

// Create stores revision one. An existing run returns graph.ErrConflict.
func (s *Store[S]) Create(ctx context.Context, value graph.Checkpoint[S]) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return errors.New("store is nil")
	}
	if err := model.ValidateCheckpointHeader(value); err != nil {
		return err
	}
	if value.Revision != 1 {
		return graph.ErrInvalidCheckpoint
	}
	payload, err := s.encodePayload(value)
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	defer s.releasePayloadBuffer(payload)
	result, err := s.db.ExecContext(ctx,
		"INSERT INTO checkpoints (run_id, machine_id, revision, payload) VALUES (?, ?, ?, ?) ON CONFLICT(run_id) DO NOTHING",
		value.RunID, value.MachineID, strconv.FormatUint(value.Revision, 10), payload.data)
	if err != nil {
		return fmt.Errorf("create checkpoint: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("create checkpoint result: %w", err)
	}
	if changed == 0 {
		return graph.ErrConflict
	}
	return nil
}

// Load returns an independent checkpoint decoded from the latest stored bytes.
func (s *Store[S]) Load(ctx context.Context, runID string) (graph.Checkpoint[S], error) {
	var empty graph.Checkpoint[S]
	if err := checkContext(ctx); err != nil {
		return empty, err
	}
	if s == nil || s.db == nil {
		return empty, errors.New("store is nil")
	}
	var machineID, revisionText string
	var payload []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT machine_id, revision, payload FROM checkpoints WHERE run_id = ?", runID,
	).Scan(&machineID, &revisionText, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, fmt.Errorf("run %q: %w", runID, checkpoint.ErrNotFound)
	}
	if err != nil {
		return empty, fmt.Errorf("load checkpoint: %w", err)
	}
	revision, err := strconv.ParseUint(revisionText, 10, 64)
	if err != nil || revision == 0 || strconv.FormatUint(revision, 10) != revisionText {
		return empty, fmt.Errorf("run %q revision: %w", runID, checkpoint.ErrCorrupt)
	}
	value, err := s.codec.Unmarshal(payload)
	if err != nil {
		return empty, fmt.Errorf("run %q decode: %w: %w", runID, checkpoint.ErrCorrupt, err)
	}
	if value.RunID != runID || value.MachineID != machineID || value.Revision != revision {
		return empty, fmt.Errorf("run %q identity: %w", runID, checkpoint.ErrCorrupt)
	}
	return value, nil
}

// CompareAndSwap atomically replaces the latest checkpoint at expected.
func (s *Store[S]) CompareAndSwap(ctx context.Context, expected uint64, next graph.Checkpoint[S]) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return errors.New("store is nil")
	}
	if err := model.ValidateCheckpointHeader(next); err != nil {
		return err
	}
	if expected == math.MaxUint64 {
		return graph.ErrExecutionLimit
	}
	if next.Revision != expected+1 {
		return graph.ErrConflict
	}
	payload, err := s.encodePayload(next)
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	defer s.releasePayloadBuffer(payload)
	result, err := s.db.ExecContext(ctx,
		"UPDATE checkpoints SET revision = ?, payload = ? WHERE run_id = ? AND machine_id = ? AND revision = ?",
		strconv.FormatUint(next.Revision, 10), payload.data, next.RunID, next.MachineID, strconv.FormatUint(expected, 10))
	if err != nil {
		return fmt.Errorf("compare and swap checkpoint: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("compare and swap result: %w", err)
	}
	if changed == 0 {
		return graph.ErrConflict
	}
	return nil
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	return ctx.Err()
}
