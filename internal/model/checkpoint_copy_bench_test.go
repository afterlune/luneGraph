package model

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

type copyReferenceState struct{ Values map[string]int }
type copyValueState [64]uint64

func BenchmarkCheckpointCopyInto(b *testing.B) {
	for _, size := range []int{1, 128, 512} {
		for _, routing := range []string{"none", "waiting", "mixed"} {
			b.Run(fmt.Sprintf("state=reference/size=%d/routing=%s", size, routing), func(b *testing.B) {
				benchmarkCopyInto(b, size, routing, copyReferenceState{Values: map[string]int{"value": 7}})
			})
		}
	}
	b.Run("state=scalar/size=512/routing=none", func(b *testing.B) { benchmarkCopyInto(b, 512, "none", 7) })
	b.Run("state=value512/size=512/routing=none", func(b *testing.B) { benchmarkCopyInto(b, 512, "none", copyValueState{7}) })
}

func benchmarkCopyInto[S any](b *testing.B, size int, routing string, state S) {
	src := Checkpoint[S]{RunID: "copy", Revision: 7, Steps: 6}
	for i := range size {
		inv := Invocation[S]{ID: "i" + strconv.Itoa(i+1), CallID: "c" + strconv.Itoa(i+1), Node: "node", State: state, Status: InvocationReady, BranchIndex: i}
		if routing == "waiting" || routing == "mixed" && i%2 == 0 {
			inv.Next = []string{"a", "b"}
			inv.Status = InvocationWaiting
			inv.Continuation = "advance"
		}
		src.Invocations = append(src.Invocations, inv)
	}
	dst := Copy(src)
	expected := Copy(src)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		CopyInto(&dst, src)
	}
	b.StopTimer()
	if !reflect.DeepEqual(dst, expected) || !reflect.DeepEqual(src, expected) {
		b.Fatal("copy changed checkpoint")
	}
	b.ReportMetric(float64(size), "invocations/op")
}
