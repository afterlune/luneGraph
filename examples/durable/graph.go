package main

import (
	"context"
	"strconv"

	graph "github.com/afterlune/luneGraph"
)

type state struct {
	Value int
}

func newRunner() (*graph.Runner[state], error) {
	g := graph.New[state]("input")
	if err := g.AddNode(graph.NodeSpec[state]{
		Name: "input",
		Run: func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
			return graph.Wait(s, "value", "finish"), nil
		},
	}); err != nil {
		return nil, err
	}
	if err := g.AddNode(graph.NodeSpec[state]{
		Name: "finish",
		Run: func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
			return graph.EndExecution(s), nil
		},
	}); err != nil {
		return nil, err
	}
	if err := g.AddEdge("input", "finish"); err != nil {
		return nil, err
	}
	if err := graph.RegisterContinuation(g, "value",
		func(payload []byte) (int, error) { return strconv.Atoi(string(payload)) },
		func(_ context.Context, _ graph.CallInfo, s state, value int) (state, error) {
			s.Value += value
			return s, nil
		},
	); err != nil {
		return nil, err
	}
	return g.Compile(graph.Config[state]{
		MachineID: "durable-example-v1",
		Clone:     func(s state) (state, error) { return s, nil },
	})
}
