package checkpoint_test

import (
	"reflect"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint"
)

type codecState struct {
	Value string
}

type unsupportedState struct {
	Callback func()
}

func TestJSONCheckpointRoundTripPreservesRecoveryFields(t *testing.T) {
	final := codecState{Value: "done"}
	want := graph.Checkpoint[codecState]{
		FormatVersion:  graph.CheckpointFormatVersion,
		RunID:          "run-1",
		MachineID:      "machine-v1",
		Revision:       7,
		Steps:          12,
		NextID:         9,
		ScheduleCursor: 3,
		Completed:      true,
		Final:          &final,
		Invocations: []graph.Invocation[codecState]{
			{ID: "i1", CallID: "node-call-17", Node: "work", State: codecState{Value: "active"}, Status: graph.InvocationWaiting, Continuation: "approval", Next: []string{"next"}},
		},
		Groups:    []graph.ActivationGroup{{ID: "g1", CallID: "join-call-4", Source: "split", JoinNode: "join", Children: []string{"i1"}}},
		Terminals: []graph.Terminal[codecState]{{InvocationID: "i2", State: codecState{Value: "branch result"}}},
		Failures:  []graph.Failure{{InvocationID: "i1", Node: "work", Scope: graph.FailGroup, Message: "failed", PanicStack: "stack"}},
	}

	codec := checkpoint.JSON[codecState]{}
	data, err := codec.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JSON round trip = %+v, want %+v", got, want)
	}
}

func TestJSONRejectsMalformedAndUnsupportedData(t *testing.T) {
	codec := checkpoint.JSON[codecState]{}
	if _, err := codec.Unmarshal([]byte("{")); err == nil {
		t.Fatal("Unmarshal accepted malformed JSON")
	}

	unsupported := checkpoint.JSON[unsupportedState]{}
	state := unsupportedState{Callback: func() {}}
	if _, err := unsupported.Marshal(graph.Checkpoint[unsupportedState]{RunID: "run", Final: &state}); err == nil {
		t.Fatal("Marshal accepted a function in state")
	}
}
