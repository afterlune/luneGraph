package executor

import (
	"context"
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
	if isJoinInterruption(marker) || !isJoinInterruption(fmt.Errorf("wrapped: %w", &joinInterruption{marker})) {
		t.Fatal("join source classification failed")
	}
	if !errors.Is(&joinInterruption{marker}, marker) {
		t.Fatal("join cause lost")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := interruptedResult(ctx, Checkpoint[int]{Revision: 7}, marker)
	if result.Status != StatusCancelled || result.Checkpoint.Revision != 7 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled=%+v %v", result, err)
	}
}
