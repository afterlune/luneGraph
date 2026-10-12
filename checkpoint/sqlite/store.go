package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/internal/model"
)

type invocationMetadata struct {
	ID, CallID, Node      string
	Status                graph.InvocationStatus
	GroupID, ChildGroupID string
	BranchIndex           int
	Continuation          string
	Next                  []string
	Error                 string
	Target                string
	Timeout               time.Duration
}

func metadata[S any](v graph.Invocation[S]) invocationMetadata {
	return invocationMetadata{v.ID, v.CallID, v.Node, v.Status, v.GroupID, v.ChildGroupID, v.BranchIndex, v.Continuation, v.Next, v.Error, v.Target, v.Timeout}
}
func (s *Store[S]) Create(ctx context.Context, value graph.Checkpoint[S]) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if err := model.ValidateCheckpointHeader(value); err != nil {
		return err
	}
	if value.Revision != 1 {
		return graph.ErrInvalidCheckpoint
	}
	if err := model.ValidateChange(0, value.Change()); err != nil {
		return err
	}
	change := value.Change()
	encoded, err := s.encodeChange(change)
	if err != nil {
		return err
	}
	defer s.releaseEncoded(encoded)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, "INSERT INTO checkpoints(run_id,machine_id,revision,payload,final) VALUES(?,?,?,?,?) ON CONFLICT(run_id) DO NOTHING", value.RunID, value.MachineID, "1", encoded.header, encoded.final)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return graph.ErrConflict
	}
	if err := s.writeRecords(ctx, tx, change, encoded); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store[S]) Commit(ctx context.Context, expected uint64, c graph.Change[S]) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if err := model.ValidateChange(expected, c); err != nil {
		return err
	}
	encoded, err := s.encodeChange(c)
	if err != nil {
		return err
	}
	defer s.releaseEncoded(encoded)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var oldPayload []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM checkpoints WHERE run_id=? AND machine_id=? AND revision=?", c.Header.RunID, c.Header.MachineID, strconv.FormatUint(expected, 10)).Scan(&oldPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return graph.ErrConflict
	}
	if err != nil {
		return err
	}
	var old graph.Header
	if err := json.Unmarshal(oldPayload, &old); err != nil {
		return fmt.Errorf("header: %w", checkpoint.ErrCorrupt)
	}
	if err := model.ValidateAdvance(old, c.Header); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE checkpoints SET revision=?,payload=?,final=? WHERE run_id=? AND machine_id=? AND revision=?", strconv.FormatUint(c.Header.Revision, 10), encoded.header, encoded.final, c.Header.RunID, c.Header.MachineID, strconv.FormatUint(expected, 10))
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return graph.ErrConflict
	}
	if err := s.writeRecords(ctx, tx, c, encoded); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store[S]) Load(ctx context.Context, id string) (graph.Checkpoint[S], error) {
	var empty graph.Checkpoint[S]
	if err := s.check(ctx); err != nil {
		return empty, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	var machine, revision string
	var payload, final []byte
	err = tx.QueryRowContext(ctx, "SELECT machine_id,revision,payload,final FROM checkpoints WHERE run_id=?", id).Scan(&machine, &revision, &payload, &final)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, checkpoint.ErrNotFound
	}
	if err != nil {
		return empty, err
	}
	h, err := decodeHeader(id, machine, revision, payload)
	if err != nil {
		return empty, err
	}
	cp := (graph.Change[S]{Header: h}).Checkpoint()
	if final != nil {
		v, err := s.codec.Unmarshal(final)
		if err != nil {
			return empty, fmt.Errorf("final: %w: %w", checkpoint.ErrCorrupt, err)
		}
		cp.Final = &v
	}
	rows, err := tx.QueryContext(ctx, "SELECT id,metadata,state FROM invocations WHERE run_id=? ORDER BY id", id)
	if err != nil {
		return empty, err
	}
	for rows.Next() {
		var key string
		var meta, state []byte
		if err := rows.Scan(&key, &meta, &state); err != nil {
			rows.Close()
			return empty, err
		}
		var m invocationMetadata
		if err := json.Unmarshal(meta, &m); err != nil || m.ID != key {
			rows.Close()
			return empty, checkpoint.ErrCorrupt
		}
		value, err := s.codec.Unmarshal(state)
		if err != nil {
			rows.Close()
			return empty, fmt.Errorf("state: %w: %w", checkpoint.ErrCorrupt, err)
		}
		cp.Invocations = append(cp.Invocations, graph.Invocation[S]{ID: m.ID, CallID: m.CallID, Node: m.Node, State: value, Status: m.Status, GroupID: m.GroupID, ChildGroupID: m.ChildGroupID, BranchIndex: m.BranchIndex, Continuation: m.Continuation, Next: m.Next, Error: m.Error, Target: m.Target, Timeout: m.Timeout})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return empty, err
	}
	rows, err = tx.QueryContext(ctx, "SELECT id,payload FROM groups WHERE run_id=? ORDER BY id", id)
	if err != nil {
		return empty, err
	}
	for rows.Next() {
		var key string
		var data []byte
		if err := rows.Scan(&key, &data); err != nil {
			rows.Close()
			return empty, err
		}
		var g graph.ActivationGroup
		if err := json.Unmarshal(data, &g); err != nil || g.ID != key {
			rows.Close()
			return empty, checkpoint.ErrCorrupt
		}
		cp.Groups = append(cp.Groups, g)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return empty, err
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	return cp, nil
}

func (s *Store[S]) LoadHeader(ctx context.Context, id string) (graph.Header, error) {
	var empty graph.Header
	if err := s.check(ctx); err != nil {
		return empty, err
	}
	var machine, revision string
	var payload []byte
	err := s.db.QueryRowContext(ctx, "SELECT machine_id,revision,payload FROM checkpoints WHERE run_id=?", id).Scan(&machine, &revision, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, checkpoint.ErrNotFound
	}
	if err != nil {
		return empty, err
	}
	return decodeHeader(id, machine, revision, payload)
}

func decodeHeader(id, machine, revision string, payload []byte) (graph.Header, error) {
	var header graph.Header
	if err := json.Unmarshal(payload, &header); err != nil {
		return graph.Header{}, fmt.Errorf("header: %w", checkpoint.ErrCorrupt)
	}
	if header.RunID != id || header.MachineID != machine || header.Revision == 0 || strconv.FormatUint(header.Revision, 10) != revision || header.FormatVersion != graph.CheckpointFormatVersion {
		return graph.Header{}, checkpoint.ErrCorrupt
	}
	return header, nil
}

func (s *Store[S]) Delete(ctx context.Context, id string) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if !model.ValidName(id) {
		return errors.New("invalid run ID")
	}
	result, err := s.db.ExecContext(ctx, "DELETE FROM checkpoints WHERE run_id=?", id)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return checkpoint.ErrNotFound
	}
	return nil
}
func (s *Store[S]) check(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return errors.New("store is nil")
	}
	return nil
}
func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	return ctx.Err()
}
