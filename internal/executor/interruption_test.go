package executor

import (
	"errors"
	"fmt"
	"testing"
)

func TestCallbackInterruptionClassification(t *testing.T) {
	marker := ErrInterrupted
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false}, {errors.New("ordinary"), false}, {marker, true}, {fmt.Errorf("wrapped: %w", marker), true},
		{&PanicError{Value: marker}, false}, {fmt.Errorf("panic: %w", &PanicError{Value: marker}), false},
	} {
		if got := isCallbackInterruption(tc.err); got != tc.want {
			t.Fatalf("%v classified %v", tc.err, got)
		}
	}
}
