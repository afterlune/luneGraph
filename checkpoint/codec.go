// Package checkpoint defines codecs and shared errors for checkpoint stores.
package checkpoint

import (
	"encoding/json"
	"errors"
)

// ErrNotFound reports that a run has no stored checkpoint.
var ErrNotFound = errors.New("checkpoint not found")

// ErrCorrupt reports that stored checkpoint data cannot be decoded or does not
// match its database identity.
var ErrCorrupt = errors.New("corrupt checkpoint")

// StateCodec encodes application state and must support concurrent calls.
// Append must append to, and not retain, dst or its returned slice. Unmarshal
// returns an independent state. SQLite owns execution metadata and callback IDs.
type StateCodec[S any] interface {
	Append(dst []byte, value S) ([]byte, error)
	Unmarshal([]byte) (S, error)
}

// JSON uses encoding/json. S must survive a JSON round trip without losing
// state required by node, merge, or continuation callbacks.
type JSON[S any] struct{}

// Append writes the value's compact JSON representation to dst without
// the trailing newline added by json.Encoder.Encode.
func (JSON[S]) Append(dst []byte, value S) ([]byte, error) {
	writer := appendWriter{dst: dst}
	if err := json.NewEncoder(&writer).Encode(value); err != nil {
		return writer.dst, err
	}
	// Encoder.Encode adds a newline. Marshal does not, and checkpoint payloads
	// retain the exact compact JSON representation used by existing databases.
	writer.dst = writer.dst[:len(writer.dst)-1]
	return writer.dst, nil
}

func (JSON[S]) Unmarshal(data []byte) (S, error) {
	var value S
	err := json.Unmarshal(data, &value)
	return value, err
}

type appendWriter struct {
	dst []byte
}

func (w *appendWriter) Write(data []byte) (int, error) {
	w.dst = append(w.dst, data...)
	return len(data), nil
}
