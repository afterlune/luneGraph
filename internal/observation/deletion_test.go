package observation

import (
	"context"
	"errors"
	"github.com/afterlune/luneGraph/internal/model"
	"reflect"
	"testing"
)

type batchStore struct {
	*mockStore
	ids []string
	err error
}

func (s *batchStore) DeleteMany(_ context.Context, ids []string) error {
	s.ids = append([]string(nil), ids...)
	return s.err
}

func TestObservedDeleteMany(t *testing.T) {
	for _, fault := range []error{nil, errors.New("uncertain commit")} {
		raw := &batchStore{mockStore: &mockStore{}, err: fault}
		rec := &recordObserver{}
		wrapped := WrapStore[int](raw, New(rec, "m", "session"))
		ids := []string{"a", "b", "a"}
		if err := wrapped.DeleteMany(context.Background(), ids); err != fault {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ids, raw.ids) || len(rec.events) != 2 {
			t.Fatal("batch forwarding/events")
		}
		if rec.events[0].Operation != model.OperationDelete || rec.events[0].Phase != model.PhaseStarted || rec.events[1].Phase != model.PhaseFinished || rec.events[1].Err != fault {
			t.Fatal(rec.events)
		}
	}
}
