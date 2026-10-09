package capacitytest

import (
	"context"
	"errors"
	"fmt"
	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"testing"
)

func BenchmarkDeleteMany(b *testing.B) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, size := range []int{1, 64, 512} {
			for _, batch := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/size=%d/batch=%t", kind, size, batch), func(b *testing.B) {
					ctx := context.Background()
					store := openStore(b, kind)
					ids := make([]string, size)
					for i := range ids {
						ids[i] = fmt.Sprintf("delete-bench-%d", i)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						b.StopTimer()
						for _, id := range ids {
							s := initial(id)
							cp := graph.Checkpoint[state]{FormatVersion: graph.CheckpointFormatVersion, MachineID: "delete-bench", RunID: id, Revision: 1, Completed: true, Final: &s}
							if err := store.Create(ctx, cp); err != nil {
								b.Fatal(err)
							}
						}
						b.StartTimer()
						if batch {
							if err := store.DeleteMany(ctx, ids); err != nil {
								b.Fatal(err)
							}
						} else {
							for _, id := range ids {
								if err := store.Delete(ctx, id); err != nil {
									b.Fatal(err)
								}
							}
						}
						b.StopTimer()
						for _, id := range ids {
							if _, err := store.Load(ctx, id); !errors.Is(err, checkpoint.ErrNotFound) {
								b.Fatal("deletion not applied")
							}
						}
						b.StartTimer()
					}
				})
			}
		}
	}
}
