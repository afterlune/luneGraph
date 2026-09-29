package checkpoint_test

import (
	"encoding/json"
	"fmt"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint"
)

var benchmarkEncodedCheckpoint []byte

type codecBenchmarkState struct {
	Values map[string]int
}

func BenchmarkJSONCheckpointEncoding(b *testing.B) {
	const branchCount = 32
	checkpointValue := graph.Checkpoint[codecBenchmarkState]{
		FormatVersion: graph.CheckpointFormatVersion,
		RunID:         "benchmark-run",
		MachineID:     "benchmark-machine-v1",
		Revision:      64,
		Steps:         32,
		Groups: []graph.ActivationGroup{{
			ID: "group-1", CallID: "join-call-1", Source: "fork", JoinNode: "join",
			Children: make([]string, branchCount),
		}},
		Invocations: make([]graph.Invocation[codecBenchmarkState], branchCount),
	}
	for i := range branchCount {
		checkpointValue.Groups[0].Children[i] = fmt.Sprintf("invocation-%02d", i)
		values := make(map[string]int, 16)
		for j := range 16 {
			values[fmt.Sprintf("value-%02d", j)] = i + j
		}
		checkpointValue.Invocations[i] = graph.Invocation[codecBenchmarkState]{
			ID:     checkpointValue.Groups[0].Children[i],
			CallID: fmt.Sprintf("node-call-%02d", i),
			Node:   "work",
			State:  codecBenchmarkState{Values: values},
		}
	}

	legacy, err := json.Marshal(checkpointValue)
	if err != nil {
		b.Fatal(err)
	}
	codec := checkpoint.JSON[codecBenchmarkState]{}

	b.Run("Marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			benchmarkEncodedCheckpoint, err = json.Marshal(checkpointValue)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("AppendReuse", func(b *testing.B) {
		payload := make([]byte, 0, len(legacy)+1) // Encoder.Encode writes a temporary trailing newline.
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			payload, err = codec.Append(payload[:0], checkpointValue)
			if err != nil {
				b.Fatal(err)
			}
		}
		benchmarkEncodedCheckpoint = payload
	})
}
