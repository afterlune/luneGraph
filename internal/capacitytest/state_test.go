package capacitytest

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type state struct {
	Owner  string
	Round  int
	Total  int
	Values map[string]int
}

func initial(owner string) state {
	values := map[string]int{"sum": 0, "inputs": 0, "branch": -1, "steps": 0}
	for i := range 12 {
		values["pad"+strconv.Itoa(i)] = i
	}
	return state{Owner: owner, Values: values}
}

func clone(s state) (state, error) {
	values := make(map[string]int, len(s.Values))
	for k, v := range s.Values {
		values[k] = v
	}
	s.Values = values
	return s, nil
}

func checkIdentity(call graph.CallInfo, s state) error {
	if call.RunID != s.Owner || call.InvocationID == "" || call.CallID == "" {
		return fmt.Errorf("callback identity %+v disagrees with owner %q", call, s.Owner)
	}
	return nil
}

func addNode(t testing.TB, g *graph.Graph[state], name string, run graph.Node[state]) {
	t.Helper()
	if err := g.AddNode(graph.NodeSpec[state]{Name: name, Run: run}); err != nil {
		t.Fatal(err)
	}
}

func addEdge(t testing.TB, g *graph.Graph[state], from, to string) {
	t.Helper()
	if err := g.AddEdge(from, to); err != nil {
		t.Fatal(err)
	}
}

func compile(t testing.TB, g *graph.Graph[state]) *graph.Runner[state] {
	t.Helper()
	r, err := g.Compile(graph.Config[state]{MachineID: "capacity-v1", Clone: clone})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func continuation(t testing.TB, g *graph.Graph[state]) {
	t.Helper()
	if err := graph.RegisterContinuation(g, "advance", func(p []byte) (int, error) { return strconv.Atoi(string(p)) }, func(_ context.Context, call graph.CallInfo, s state, input int) (state, error) {
		if err := checkIdentity(call, s); err != nil {
			return s, err
		}
		if input != 1 {
			return s, fmt.Errorf("input = %d, want 1", input)
		}
		s.Values["inputs"]++
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func rootState(cp graph.Checkpoint[state]) (state, error) {
	for _, inv := range cp.Invocations {
		if inv.GroupID == "" {
			return inv.State, nil
		}
	}
	if cp.Final != nil {
		return *cp.Final, nil
	}
	return state{}, fmt.Errorf("checkpoint has no root state")
}
