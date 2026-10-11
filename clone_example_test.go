package graph_test

import (
	"fmt"

	graph "github.com/afterlune/luneGraph"
)

type referenceExampleState struct {
	Values map[string][]byte
	Data   []byte
}

func copyExampleBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// This Clone is specific to referenceExampleState: copying the map alone would
// leave its mutable byte slices shared. Nil and non-nil empty values survive.
func cloneNestedReferences(s referenceExampleState) (referenceExampleState, error) {
	if s.Values != nil {
		values := make(map[string][]byte, len(s.Values))
		for key, value := range s.Values {
			values[key] = copyExampleBytes(value)
		}
		s.Values = values
	}
	s.Data = copyExampleBytes(s.Data)
	return s, nil
}

func ExampleClone() {
	original := referenceExampleState{Values: map[string][]byte{"item": {10}}, Data: []byte{20}}
	var clone graph.Clone[referenceExampleState] = cloneNestedReferences
	independent, err := clone(original)
	if err != nil {
		panic(err)
	}
	independent.Values["item"][0] = 11
	independent.Data[0] = 21
	fmt.Println("original:", original.Values["item"][0], original.Data[0])
	fmt.Println("copy:", independent.Values["item"][0], independent.Data[0])
	// Output:
	// original: 10 20
	// copy: 11 21
}
