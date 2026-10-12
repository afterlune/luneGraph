package graph_test

import (
	"context"
	"reflect"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type nilPayload struct{}

func (*nilPayload) Error() string { return "payload" }

func TestContinuationPreservesNilPayloads(t *testing.T) {
	t.Run("nil interface", func(t *testing.T) { checkContinuationPayload[any](t, nil, false) })
	t.Run("nil nonempty interface", func(t *testing.T) { checkContinuationPayload[error](t, nil, false) })
	t.Run("nil pointer", func(t *testing.T) { checkContinuationPayload[*nilPayload](t, nil, false) })
	t.Run("typed nil interface", func(t *testing.T) { checkContinuationPayload[error](t, (*nilPayload)(nil), false) })
	t.Run("json null", func(t *testing.T) { checkContinuationPayload[any](t, nil, true) })
}

func checkContinuationPayload[P any](t *testing.T, want P, jsonInput bool) {
	t.Helper()
	g := graph.New[int]("wait")
	node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.Wait(s, "input", "done"), nil
	})
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.EndExecution(s), nil
	})
	edge(t, g, "wait", "done")
	applied := 0
	apply := func(_ context.Context, _ graph.CallInfo, s int, got P) (int, error) {
		applied++
		if !reflect.DeepEqual(got, want) {
			t.Errorf("payload = %#v, want %#v", got, want)
		}
		return s + 1, nil
	}
	var err error
	if jsonInput {
		err = graph.RegisterJSONContinuation(g, "input", apply)
	} else {
		err = graph.RegisterContinuation(g, "input", func([]byte) (P, error) { return want, nil }, apply)
	}
	if err != nil {
		t.Fatal(err)
	}
	r := intRunner(t, g)
	e, _, err := r.Start(context.Background(), "nil-input", 3, nil, graph.Options{})
	if err != nil {
		t.Fatal(err)
	}
	waiting := e.Pending()[0]
	result, err := e.Advance(context.Background(), []graph.ResumeInput{{InvocationID: waiting.InvocationID, CallID: waiting.CallID, Payload: []byte("null")}}, graph.Options{})
	if err != nil || result.Status != graph.StatusCompleted || result.Final == nil || *result.Final != 4 || applied != 1 {
		t.Fatalf("result=%+v apply=%d err=%v", result, applied, err)
	}
}
