package capacitytest

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func chainGraph(t testing.TB, size int) *graph.Graph[state] {
	g := graph.New[state]("n0")
	for i := range size {
		addNode(t, g, "n"+strconv.Itoa(i), func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
			if err := checkIdentity(call, s); err != nil {
				return graph.Transition[state]{}, err
			}
			s.Total++
			s.Values["steps"]++
			if i+1 == size {
				return graph.EndExecution(s), nil
			}
			return graph.To(s, "n"+strconv.Itoa(i+1)), nil
		})
	}
	for i := range size - 1 {
		addEdge(t, g, "n"+strconv.Itoa(i), "n"+strconv.Itoa(i+1))
	}
	return g
}

// A complete round starts at fork and ends at boundary: width+2 node steps.
// Joins have an outgoing edge, so looping does not retain terminal history.
func fanoutRunner(t testing.TB, width, rounds, waitEvery int) *graph.Runner[state] {
	g := graph.New[state]("fork")
	targets := make([]string, width)
	for i := range width {
		targets[i] = "b" + strconv.Itoa(i)
	}
	addNode(t, g, "fork", func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
		if err := checkIdentity(call, s); err != nil {
			return graph.Transition[state]{}, err
		}
		return graph.To(s, targets...), nil
	})
	if err := g.AddJoin(graph.JoinSpec[state]{Name: "join", From: "fork", Merge: func(_ context.Context, call graph.CallInfo, values []state) (state, error) {
		if len(values) != width {
			return state{}, fmt.Errorf("join received %d/%d branches", len(values), width)
		}
		out := values[0]
		if err := checkIdentity(call, out); err != nil {
			return state{}, err
		}
		for i, s := range values {
			if s.Owner != out.Owner || s.Round != out.Round || s.Values["branch"] != i || s.Total != (s.Round+1)*width {
				return state{}, fmt.Errorf("branch %d state mixed: %+v", i, s)
			}
		}
		out.Round++
		out.Total = out.Round * width
		out.Values["sum"] = out.Round * width * (width + 1) / 2
		out.Values["branch"] = -1
		return out, nil
	}}); err != nil {
		t.Fatal(err)
	}
	for i, name := range targets {
		addNode(t, g, name, func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
			if err := checkIdentity(call, s); err != nil {
				return graph.Transition[state]{}, err
			}
			s.Total += width
			s.Values["branch"] = i
			return graph.To(s, "join"), nil
		})
		addEdge(t, g, "fork", name)
		addEdge(t, g, name, "join")
	}
	addNode(t, g, "boundary", func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
		if err := checkIdentity(call, s); err != nil {
			return graph.Transition[state]{}, err
		}
		if rounds > 0 && s.Round == rounds {
			return graph.EndExecution(s), nil
		}
		if waitEvery > 0 && s.Round%waitEvery == 0 {
			return graph.Wait(s, "advance", "fork"), nil
		}
		return graph.To(s, "fork"), nil
	})
	addEdge(t, g, "join", "boundary")
	addEdge(t, g, "boundary", "fork")
	continuation(t, g)
	return compile(t, g)
}

func waitingRunner(t testing.TB) *graph.Runner[state] {
	g := graph.New[state]("work")
	addNode(t, g, "work", func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
		if err := checkIdentity(call, s); err != nil {
			return graph.Transition[state]{}, err
		}
		s.Round++
		s.Total++
		s.Values["sum"]++
		return graph.Wait(s, "advance", "work"), nil
	})
	addEdge(t, g, "work", "work")
	continuation(t, g)
	return compile(t, g)
}
