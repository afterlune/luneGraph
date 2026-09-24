// Package checkpoint defines codecs and shared errors for checkpoint stores.
package checkpoint

import (
	"encoding/json"
	"errors"

	graph "lune-graph"
)

// ErrNotFound reports that a run has no stored checkpoint.
var ErrNotFound = errors.New("checkpoint not found")

// ErrCorrupt reports that stored checkpoint data cannot be decoded or does not
// match its database identity.
var ErrCorrupt = errors.New("corrupt checkpoint")

// Codec converts a complete checkpoint to and from an independent byte value.
// Implementations must preserve all fields needed by Runner.Resume.
type Codec[S any] interface {
	Marshal(graph.Checkpoint[S]) ([]byte, error)
	Unmarshal([]byte) (graph.Checkpoint[S], error)
}

// JSON uses encoding/json. S must survive a JSON round trip without losing
// state required by node, merge, or continuation callbacks.
type JSON[S any] struct{}

func (JSON[S]) Marshal(value graph.Checkpoint[S]) ([]byte, error) {
	return json.Marshal(value)
}

func (JSON[S]) Unmarshal(data []byte) (graph.Checkpoint[S], error) {
	var value graph.Checkpoint[S]
	err := json.Unmarshal(data, &value)
	return value, err
}
