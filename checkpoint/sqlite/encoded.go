package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"

	graph "github.com/afterlune/luneGraph"
)

type encodedInvocation struct {
	id       string
	metadata []byte
	state    *payloadBuffer
}
type encodedGroup struct {
	id      string
	payload []byte
}
type encodedChange[S any] struct {
	header      []byte
	final       []byte
	invocations []encodedInvocation
	groups      []encodedGroup
	finalBuffer *payloadBuffer
}

// encodeChange invokes application codecs and JSON marshalers before opening
// the SQLite write transaction. Slow or stateful encoders do not hold a lock.
func (s *Store[S]) encodeChange(c graph.Change[S]) (out encodedChange[S], err error) {
	out.header, err = json.Marshal(c.Header)
	if err != nil {
		return out, err
	}
	defer func() {
		if err != nil {
			s.releaseEncoded(out)
		}
	}()
	if c.Final != nil {
		out.finalBuffer, err = s.encodePayload(*c.Final)
		if err != nil {
			return out, err
		}
		out.final = append([]byte(nil), out.finalBuffer.data...)
	}
	for _, v := range c.UpsertInvocations {
		meta, e := json.Marshal(metadata(v))
		if e != nil {
			return out, e
		}
		state, e := s.encodePayload(v.State)
		if e != nil {
			return out, e
		}
		out.invocations = append(out.invocations, encodedInvocation{id: v.ID, metadata: meta, state: state})
	}
	for _, g := range c.UpsertGroups {
		data, e := json.Marshal(g)
		if e != nil {
			return out, e
		}
		out.groups = append(out.groups, encodedGroup{id: g.ID, payload: data})
	}
	return out, nil
}

func (s *Store[S]) releaseEncoded(c encodedChange[S]) {
	s.releasePayloadBuffer(c.finalBuffer)
	for _, v := range c.invocations {
		s.releasePayloadBuffer(v.state)
	}
}

func (s *Store[S]) writeRecords(ctx context.Context, tx *sql.Tx, c graph.Change[S], encoded encodedChange[S]) error {
	id := c.Header.RunID
	if c.Header.Completed {
		for _, table := range []string{"invocations", "groups"} {
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE run_id=?", id); err != nil {
				return err
			}
		}
		return nil
	}
	for _, child := range c.DeleteInvocations {
		if _, err := tx.ExecContext(ctx, "DELETE FROM invocations WHERE run_id=? AND id=?", id, child); err != nil {
			return err
		}
	}
	for _, v := range encoded.invocations {
		if _, err := tx.ExecContext(ctx, "INSERT INTO invocations(run_id,id,metadata,state) VALUES(?,?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET metadata=excluded.metadata,state=excluded.state", id, v.id, v.metadata, v.state.data); err != nil {
			return err
		}
	}
	for _, group := range c.DeleteGroups {
		if _, err := tx.ExecContext(ctx, "DELETE FROM groups WHERE run_id=? AND id=?", id, group); err != nil {
			return err
		}
	}
	for _, g := range encoded.groups {
		if _, err := tx.ExecContext(ctx, "INSERT INTO groups(run_id,id,payload) VALUES(?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET payload=excluded.payload", id, g.id, g.payload); err != nil {
			return err
		}
	}
	return nil
}
