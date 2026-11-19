package definition

import (
	"context"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
)

func BenchmarkContinuationAdapter(b *testing.B) {
	b.Run("scalar", func(b *testing.B) { benchmarkContinuationAdapter(b, 7) })
	b.Run("struct", func(b *testing.B) {
		benchmarkContinuationAdapter(b, struct{ A, B, C int }{1, 2, 3})
	})
	b.Run("nil-interface", func(b *testing.B) { benchmarkContinuationAdapter[any](b, nil) })
}

func benchmarkContinuationAdapter[P any](b *testing.B, payload P) {
	g := New[int]("unused")
	if err := RegisterContinuation(g, "input", func([]byte) (P, error) { return payload, nil },
		func(_ context.Context, _ model.CallInfo, state int, _ P) (int, error) { return state + 1, nil }); err != nil {
		b.Fatal(err)
	}
	c := g.continuations["input"]
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		value, err := c.Decode(nil)
		if err != nil {
			b.Fatal(err)
		}
		state, err := c.Apply(ctx, model.CallInfo{}, 0, value)
		if err != nil || state != 1 {
			b.Fatalf("apply = %d, %v", state, err)
		}
	}
}
