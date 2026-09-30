package checkpoint_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
)

type codecState struct {
	Value  string
	Values map[string]int
}

type customMarshalerState struct{ Value string }

func (s customMarshalerState) MarshalJSON() ([]byte, error) {
	value, err := json.Marshal(s.Value)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(`{"custom":`), value...), '}'), nil
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
			{ID: "i1", CallID: "node-call-17", Node: "work", State: codecState{Value: "<active> 雪", Values: map[string]int{"z": 1, "a": 2}}, Status: graph.InvocationWaiting, Continuation: "approval", Next: []string{"next"}},
		},
		Groups:    []graph.ActivationGroup{{ID: "g1", CallID: "join-call-4", Source: "split", JoinNode: "join", Children: []string{"i1"}}},
		Terminals: []graph.Terminal[codecState]{{InvocationID: "i2", State: codecState{Value: "branch result"}}},
		Failures:  []graph.Failure{{InvocationID: "i1", Node: "work", Scope: graph.FailGroup, Message: "failed", PanicStack: "stack"}},
	}

	codec := checkpoint.JSON[codecState]{}
	data, err := codec.Append(nil, want)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, legacy) {
		t.Fatalf("JSON Append = %s, json.Marshal = %s", data, legacy)
	}
	got, err := codec.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JSON round trip = %+v, want %+v", got, want)
	}
}

func TestJSONAppendMatchesMarshalAndKeepsPrefix(t *testing.T) {
	want := graph.Checkpoint[codecState]{
		FormatVersion: graph.CheckpointFormatVersion,
		RunID:         "run-<雪>",
		MachineID:     "machine-v1",
		Revision:      2,
		Invocations: []graph.Invocation[codecState]{
			{ID: "i1", State: codecState{Value: "</script> 雪", Values: map[string]int{"z": 1, "a": 2}}, Next: []string{"a", "b"}},
		},
	}
	legacy, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	const prefix = "existing:"
	got, err := (checkpoint.JSON[codecState]{}).Append([]byte(prefix), want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:len(prefix)], []byte(prefix)) {
		t.Fatalf("Append changed destination prefix: %q", got[:len(prefix)])
	}
	if !bytes.Equal(got[len(prefix):], legacy) {
		t.Fatalf("Append JSON = %s, json.Marshal = %s", got[len(prefix):], legacy)
	}
}

func TestJSONAppendMatchesMarshalForCustomMarshaler(t *testing.T) {
	final := customMarshalerState{Value: "<custom> 雪"}
	want := graph.Checkpoint[customMarshalerState]{
		FormatVersion: graph.CheckpointFormatVersion,
		RunID:         "custom-json",
		MachineID:     "machine-v1",
		Revision:      1,
		Final:         &final,
	}
	legacy, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := (checkpoint.JSON[customMarshalerState]{}).Append(nil, want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, legacy) {
		t.Fatalf("JSON Append = %s, json.Marshal = %s", got, legacy)
	}
}

func TestJSONRejectsMalformedAndUnsupportedData(t *testing.T) {
	codec := checkpoint.JSON[codecState]{}
	if _, err := codec.Unmarshal([]byte("{")); err == nil {
		t.Fatal("Unmarshal accepted malformed JSON")
	}

	unsupported := checkpoint.JSON[unsupportedState]{}
	state := unsupportedState{Callback: func() {}}
	if _, err := unsupported.Append(nil, graph.Checkpoint[unsupportedState]{RunID: "run", Final: &state}); err == nil {
		t.Fatal("Marshal accepted a function in state")
	}
}
