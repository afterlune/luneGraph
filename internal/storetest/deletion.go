package storetest

import (
	"context"
	"errors"
	"fmt"
	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"testing"
)

func deletion(t *testing.T, open func(*testing.T) graph.Store[State]) {
	t.Run("delete-many-lost-ack", func(t *testing.T) {
		base := open(t)
		ctx := context.Background()
		for _, id := range []string{"a", "b"} {
			cp := fixture()
			cp.RunID = id
			if err := base.Create(ctx, cp); err != nil {
				t.Fatal(err)
			}
		}
		fault := errors.New("lost deletion acknowledgement")
		wrapped := &lostDeletionAck{Store: base, fault: fault}
		ids := []string{"a", "b"}
		if err := wrapped.DeleteMany(ctx, ids); !errors.Is(err, fault) {
			t.Fatal(err)
		}
		for _, id := range ids {
			if _, err := base.Load(ctx, id); !errors.Is(err, checkpoint.ErrNotFound) {
				t.Fatal("authoritative deletion", err)
			}
		}
		if err := wrapped.DeleteMany(ctx, ids); err != nil {
			t.Fatal("idempotent retry", err)
		}
	})
	t.Run("delete-many", func(t *testing.T) {
		s := open(t)
		ctx := context.Background()
		ids := make([]string, 513)
		for i := range ids {
			ids[i] = fmt.Sprintf("batch-%d", i)
			cp := fixture()
			cp.RunID = ids[i]
			if err := s.Create(ctx, cp); err != nil {
				t.Fatal(err)
			}
		}
		for _, batch := range [][]string{nil, {}, {ids[0], " "}} {
			err := s.DeleteMany(ctx, batch)
			if len(batch) == 0 && err != nil {
				t.Fatal(err)
			}
			if len(batch) > 0 && err == nil {
				t.Fatal("invalid batch accepted")
			}
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := s.DeleteMany(cancelled, ids); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := s.DeleteMany(cancelled, nil); !errors.Is(err, context.Canceled) {
			t.Fatal("cancelled empty batch", err)
		}
		if err := s.DeleteMany(nil, ids); err == nil {
			t.Fatal("nil context accepted")
		}
		for _, id := range ids {
			if _, err := s.Load(ctx, id); err != nil {
				t.Fatal("validation/cancellation partially deleted", err)
			}
		}
		batch := append(append([]string(nil), ids[:512]...), ids[0], "missing")
		for range 2 {
			if err := s.DeleteMany(ctx, batch); err != nil {
				t.Fatal(err)
			}
		}
		for _, id := range ids[:512] {
			if _, err := s.Load(ctx, id); !errors.Is(err, checkpoint.ErrNotFound) {
				t.Fatal(id, err)
			}
		}
		if _, err := s.Load(ctx, ids[512]); err != nil {
			t.Fatal("unrelated run deleted", err)
		}
	})
}

type lostDeletionAck struct {
	graph.Store[State]
	fault error
}

func (s *lostDeletionAck) DeleteMany(ctx context.Context, ids []string) error {
	if err := s.Store.DeleteMany(ctx, ids); err != nil {
		return err
	}
	err := s.fault
	s.fault = nil
	return err
}
