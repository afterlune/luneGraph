package graph_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint/memory"
)

func referenceContractRunner(t *testing.T, clone graph.Clone[referenceExampleState], merges *[]graph.CallInfo) *graph.Runner[referenceExampleState] {
	t.Helper()
	g := graph.New[referenceExampleState]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s referenceExampleState) (graph.Transition[referenceExampleState], error) {
		return graph.To(s, "left", "right"), nil
	})
	for i, name := range []string{"left", "right"} {
		delta := byte(i + 1)
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, s referenceExampleState) (graph.Transition[referenceExampleState], error) {
			s.Values["item"][0] += delta
			s.Data[0] += delta
			return graph.To(s, "join"), nil
		})
		edge(t, g, "fork", name)
	}
	if err := g.AddJoin(graph.JoinSpec[referenceExampleState]{Name: "join", From: "fork", OnError: graph.FailExecution,
		Merge: func(_ context.Context, call graph.CallInfo, values []referenceExampleState) (referenceExampleState, error) {
			*merges = append(*merges, call)
			if len(values) != 2 || values[0].Values["item"][0] != 11 || values[1].Values["item"][0] != 12 || values[0].Data[0] != 21 || values[1].Data[0] != 22 {
				return referenceExampleState{}, fmt.Errorf("merge inputs were not isolated: %+v", values)
			}
			out := referenceExampleState{
				Values: map[string][]byte{"item": {values[0].Values["item"][0] + values[1].Values["item"][0]}},
				Data:   []byte{values[0].Data[0] + values[1].Data[0]},
			}
			values[0].Values["item"][0] = 99
			values[0].Data[0] = 88
			if len(*merges) == 1 {
				return referenceExampleState{}, graph.Interrupt(nil)
			}
			return out, nil
		}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"left", "right"} {
		edge(t, g, name, "join")
	}
	node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s referenceExampleState) (graph.Transition[referenceExampleState], error) {
		return graph.EndExecution(s), nil
	})
	edge(t, g, "join", "done")
	r, err := g.Compile(graph.Config[referenceExampleState]{MachineID: "nested-reference-example-v1", Clone: clone})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestNestedReferenceCloneContractExample(t *testing.T) {
	t.Run("nil_and_empty", func(t *testing.T) {
		for _, original := range []referenceExampleState{
			{},
			{Values: map[string][]byte{}, Data: []byte{}},
			{Values: map[string][]byte{"nil": nil, "empty": {}, "value": {1}}, Data: []byte{2}},
		} {
			cloned, err := cloneNestedReferences(original)
			if err != nil || !reflect.DeepEqual(original, cloned) {
				t.Fatalf("copy changed values or nilness: original=%+v cloned=%+v err=%v", original, cloned, err)
			}
			if original.Values != nil {
				cloned.Values["added"] = []byte{3}
				if _, exists := original.Values["added"]; exists {
					t.Fatal("map storage is shared")
				}
			}
			if len(original.Values["value"]) != 0 {
				cloned.Values["value"][0] = 9
				if original.Values["value"][0] != 1 {
					t.Fatal("nested slice storage is shared")
				}
			}
			if len(original.Data) != 0 {
				cloned.Data[0] = 9
				if original.Data[0] != 2 {
					t.Fatal("data storage is shared")
				}
			}
		}
	})
	for _, concurrency := range []int{1, 2} {
		t.Run("concurrency="+strconv.Itoa(concurrency), func(t *testing.T) {
			ctx := context.Background()
			original := referenceExampleState{Values: map[string][]byte{"item": {10}}, Data: []byte{20}}
			var merges []graph.CallInfo // One join at a time; read only after workers drain.
			r := referenceContractRunner(t, cloneNestedReferences, &merges)
			store, err := memory.New(cloneNestedReferences)
			if err != nil {
				t.Fatal(err)
			}
			seed, err := r.Start(ctx, "references", original, graph.Options[referenceExampleState]{Store: store, MaxSteps: 1})
			if err != nil || seed.Status != graph.StatusBudget {
				t.Fatalf("seed=%+v err=%v", seed, err)
			}
			opts := graph.Options[referenceExampleState]{Store: store, MaxConcurrency: concurrency}
			out, err := r.Resume(ctx, seed.Checkpoint, nil, opts)
			if !errors.Is(err, graph.ErrInterrupted) || out.Status != graph.StatusInterrupted || len(merges) != 1 || out.Checkpoint.Revision != seed.Checkpoint.Revision+2 {
				t.Fatalf("interrupted=%+v merges=%v err=%v", out, merges, err)
			}
			if original.Values["item"][0] != 10 || original.Data[0] != 20 {
				t.Fatal("execution mutated its caller's initial state")
			}
			for _, inv := range seed.Checkpoint.Invocations {
				if inv.State.Values["item"][0] != 10 || inv.State.Data[0] != 20 {
					t.Fatalf("callbacks mutated the immutable source checkpoint: %+v", inv)
				}
			}
			joined := 0
			for _, inv := range out.Checkpoint.Invocations {
				if inv.Status == graph.InvocationJoined {
					joined++
					if inv.State.Values["item"][0] != byte(11+inv.BranchIndex) || inv.State.Data[0] != byte(21+inv.BranchIndex) {
						t.Fatalf("Merge mutated a committed input: %+v", inv)
					}
				}
			}
			if joined != 2 {
				t.Fatalf("joined inputs=%d", joined)
			}
			saved, err := store.Load(ctx, "references")
			if err != nil || !reflect.DeepEqual(saved, out.Checkpoint) {
				t.Fatalf("returned position differs from stored position: %v", err)
			}
			finished, err := r.Recover(ctx, "references", nil, opts)
			if err != nil || finished.Status != graph.StatusCompleted || finished.Checkpoint.Final == nil || len(merges) != 2 || merges[0].CallID != merges[1].CallID {
				t.Fatalf("finished=%+v merges=%v err=%v", finished, merges, err)
			}
			if finished.Checkpoint.Final.Values["item"][0] != 23 || finished.Checkpoint.Final.Data[0] != 43 {
				t.Fatalf("wrong merged result: %+v", *finished.Checkpoint.Final)
			}
		})
	}
	t.Run("shallow_negative_control", func(t *testing.T) {
		// Copying the outer map and Data still leaves Values' byte slices shared.
		shallow := func(s referenceExampleState) (referenceExampleState, error) {
			s.Values = maps.Clone(s.Values)
			s.Data = copyExampleBytes(s.Data)
			return s, nil
		}
		original := referenceExampleState{Values: map[string][]byte{"item": {10}}, Data: []byte{20}}
		var merges []graph.CallInfo
		r := referenceContractRunner(t, shallow, &merges)
		// Serial admission makes the intentional contract violation deterministic
		// without introducing a deliberate data race into the race suite.
		out, err := r.Start(context.Background(), "shallow", original, graph.Options[referenceExampleState]{MaxConcurrency: 1})
		if err == nil || out.Checkpoint.Failure == nil || original.Values["item"][0] != 13 || original.Data[0] != 20 {
			t.Fatalf("negative control did not detect shared nested storage: out=%+v original=%+v err=%v", out, original, err)
		}
	})
}
