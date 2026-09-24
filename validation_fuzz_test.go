package graph_test

import (
	"context"
	"testing"

	graph "lune-graph"
)

// FuzzCheckpointValidation exercises Resume's boundary with malformed position data.
func FuzzCheckpointValidation(f *testing.F) {
	for _, seed := range [][]byte{{}, {0}, {1, 2, 3, 4, 5}, {255, 255, 255, 255, 255}} {
		f.Add(seed)
	}
	g := graph.New[int]("wait")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "wait", Run: func(_ context.Context, state int) (graph.Transition[int], error) {
		return graph.Wait(state, "input", "wait"), nil
	}}); err != nil {
		f.Fatal(err)
	}
	if err := g.AddEdge("wait", "wait"); err != nil {
		f.Fatal(err)
	}
	if err := graph.RegisterContinuation(g, "input", func([]byte) (int, error) { return 0, nil }, func(_ context.Context, state, _ int) (int, error) { return state, nil }); err != nil {
		f.Fatal(err)
	}
	r, err := g.Compile(graph.Config[int]{MachineID: "fuzz-v1", Clone: func(state int) (int, error) { return state, nil }})
	if err != nil {
		f.Fatal(err)
	}
	base, err := r.Start(context.Background(), "fuzz-run", 0, graph.Options[int]{})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		s := base.Checkpoint
		s.Invocations = append([]graph.Invocation[int](nil), s.Invocations...)
		if len(data) > 0 {
			s.FormatVersion = uint32(data[0])
		}
		if len(data) > 1 {
			s.Invocations[0].Status = graph.InvocationStatus(data[1:2])
		}
		if len(data) > 2 {
			s.Invocations[0].BranchIndex = int(data[2])
		}
		if len(data) > 3 {
			s.Invocations[0].ID = string(data[3:])
		}
		if len(data) > 4 {
			s.ScheduleCursor = uint64(data[4])
		}
		_, _ = r.Resume(context.Background(), s, nil, graph.Options[int]{MaxSteps: 1})
	})
}
