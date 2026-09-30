package main

import (
	"context"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/examples/effects/internal/ledger"
)

const namespace = "counter-add-v1"

type state struct{ Delta, Value int64 }
type addEffect func(context.Context, graph.CallInfo, int64) (int64, error)

func effect(l *ledger.Ledger) addEffect {
	return func(ctx context.Context, call graph.CallInfo, delta int64) (int64, error) {
		return l.Add(ctx, ledger.Key{Namespace: namespace, RunID: call.RunID, CallID: call.CallID}, delta)
	}
}

func newRunner(add addEffect) (*graph.Runner[state], error) {
	g := graph.New[state]("add")
	if err := g.AddNode(graph.NodeSpec[state]{Name: "add", OnError: graph.FailExecution,
		Run: func(ctx context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
			value, err := add(ctx, call, s.Delta)
			if err != nil {
				return graph.Transition[state]{}, err
			}
			s.Value = value
			return graph.EndExecution(s), nil
		},
	}); err != nil {
		return nil, err
	}
	return g.Compile(graph.Config[state]{MachineID: "effects-example-v1", Clone: func(s state) (state, error) { return s, nil }})
}
