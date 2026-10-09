package memory_test

import (
	"context"
	"github.com/afterlune/luneGraph/checkpoint/memory"
	"testing"
)

func TestDeleteManyNilStore(t *testing.T) {
	var s *memory.Store[int]
	if err := s.DeleteMany(context.Background(), nil); err == nil {
		t.Fatal("nil store accepted")
	}
}
