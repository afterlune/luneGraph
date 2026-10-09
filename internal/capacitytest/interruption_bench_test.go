package capacitytest

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkCapacityInterruption(b *testing.B) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, concurrency := range []int{1, 8} {
			b.Run(fmt.Sprintf("%s/concurrency=%d", kind, concurrency), func(b *testing.B) {
				c := &interruptionController{}
				r, store := interruptionRunner(b, 8, c), openStore(b, kind)
				s := seedInterrupted(b, r, store, c, "bench", 8, concurrency)
				if err := advanceInterruptedRound(context.Background(), r, store, c, &s, 8, concurrency); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := advanceInterruptedRound(context.Background(), r, store, c, &s, 8, concurrency); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if c.active.Load() != 0 || s.checkpoint.Steps != uint64((b.N+1)*10) {
					b.Fatalf("invalid final counters: %+v", s)
				}
			})
		}
	}
}
