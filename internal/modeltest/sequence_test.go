package modeltest

import (
	"fmt"
	"testing"
)

// Each fault seed has enough budget to reach its first matching callback or
// submission; mixed seeds cut rounds at different node budgets.
func seeds(parallel bool) [][]byte {
	out := [][]byte{{}, {0, 0, 0, 8, 0, 16, 0, 24, 0}, {7, 0, 0, 17, 0, 18, 0, 19, 0, 20, 0, 21, 0, 22, 0, 23, 0}}
	for _, header := range []byte{0, 7} {
		for mode := 1; mode <= 7; mode++ {
			if !parallel && mode == 5 {
				continue
			}
			out = append(out, []byte{header, byte(56 + mode), 0})
		}
	}
	// With width four, a three-node budget commits fork and two branches.
	// The second following CAS submits the last branch together with its join.
	if parallel {
		out = append(out, []byte{7, 16, 0, 59, 1}, []byte{0, 0, 0, 61, 0})
	}
	// Exercise local and global concurrency independently, not just 1/1 and 4/4.
	out = append(out, []byte{3, 60, 0}, []byte{4, 62, 0})
	return out
}

func TestExecutionModelSequences(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, kind := range []string{"memory", "sqlite"} {
			for i, data := range seeds(parallel) {
				t.Run(fmt.Sprintf("parallel=%t/%s/seed=%d", parallel, kind, i), func(t *testing.T) { runSequence(t, parallel, kind, data, i >= 3) })
			}
		}
	}
}

func FuzzLoopExecution(f *testing.F) {
	for _, seed := range seeds(false) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) { runSequence(t, false, "memory", data, false) })
}

func FuzzParallelExecution(f *testing.F) {
	for _, seed := range seeds(true) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) { runSequence(t, true, "memory", data, false) })
}
