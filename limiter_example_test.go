package graph_test

import (
	"context"
	"fmt"

	graph "github.com/afterlune/luneGraph"
)

func ExampleLimiter() {
	// Express this dependency as edges. Each callback returns and releases its
	// permit before the next node needs one; no callback synchronously drives
	// another execution that requires its own occupied permit.
	g := graph.New[int]("prepare")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "prepare", Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s+1, "finish"), nil
	}}); err != nil {
		panic(err)
	}
	if err := g.AddNode(graph.NodeSpec[int]{Name: "finish", Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.EndExecution(s + 1), nil
	}}); err != nil {
		panic(err)
	}
	if err := g.AddEdge("prepare", "finish"); err != nil {
		panic(err)
	}
	runner, err := g.Compile(graph.Config[int]{MachineID: "limiter-dependency-example-v1", Clone: graph.ValueClone[int]})
	if err != nil {
		panic(err)
	}
	limiter, err := graph.NewLimiter(1)
	if err != nil {
		panic(err)
	}
	result, err := runner.Start(context.Background(), "dependency", 0, graph.Options[int]{Limiter: limiter})
	if err != nil {
		panic(err)
	}
	fmt.Println(*result.Checkpoint.Final)
	// Output: 2
}
