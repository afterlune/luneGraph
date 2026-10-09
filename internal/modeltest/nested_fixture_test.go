package modeltest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func nestedRunner(t testing.TB, m nestedOracle, c *controller) *graph.Runner[nestedState] {
	t.Helper()
	g := graph.New[nestedState]("fork")
	add := func(name string, policy graph.FailureScope, body graph.Node[nestedState]) {
		t.Helper()
		if err := g.AddNode(graph.NodeSpec[nestedState]{Name: name, OnError: policy, Run: func(ctx context.Context, call graph.CallInfo, s nestedState) (graph.Transition[nestedState], error) {
			if err := c.attempt(ctx, call, "node", s); err != nil {
				s.Values["speculative"] = 1
				return graph.To(s, "fork"), err
			}
			return body(ctx, call, s)
		}}); err != nil {
			t.Fatal(err)
		}
	}
	edge := func(from, to string) {
		t.Helper()
		if err := g.AddEdge(from, to); err != nil {
			t.Fatal(err)
		}
	}
	stamp := func(s *nestedState, phase int) {
		s.Values = map[string]int{"sum": s.Sum, "phase": phase, "branch": s.Branch}
	}
	add("fork", graph.FailInvocation, func(_ context.Context, _ graph.CallInfo, s nestedState) (graph.Transition[nestedState], error) {
		s.Branch = -1
		stamp(&s, phaseRoot)
		return graph.To(s, "healthy", "nested"), nil
	})
	add("healthy", graph.FailInvocation, func(_ context.Context, _ graph.CallInfo, s nestedState) (graph.Transition[nestedState], error) {
		s.Sum += 100
		s.Branch = -1
		stamp(&s, phaseHealthy)
		return graph.To(s, "outerjoin"), nil
	})
	add("nested", graph.FailInvocation, func(_ context.Context, _ graph.CallInfo, s nestedState) (graph.Transition[nestedState], error) {
		s.Branch = -1
		stamp(&s, phaseNested)
		targets := make([]string, m.width)
		for i := range targets {
			targets[i] = fmt.Sprintf("leaf%d", i)
		}
		return graph.To(s, targets...), nil
	})
	for i := range m.width {
		index := i
		policy := graph.FailInvocation
		if m.failure == 2 {
			policy = graph.FailGroup
		}
		add(fmt.Sprintf("leaf%d", index), policy, func(_ context.Context, _ graph.CallInfo, s nestedState) (graph.Transition[nestedState], error) {
			if index == 0 && (m.failure == 1 || m.failure == 2) {
				return graph.Transition[nestedState]{}, errors.New("modeled leaf failure")
			}
			s.Sum += index + 1
			s.Branch = index
			stamp(&s, phaseLeafBase+index)
			return graph.To(s, "innerjoin"), nil
		})
	}
	merge := func(name string, nextPhase int, inner bool) graph.Merge[nestedState] {
		return func(ctx context.Context, call graph.CallInfo, values []nestedState) (nestedState, error) {
			if err := c.attempt(ctx, call, "join", values); err != nil {
				values[0].Values["speculative"] = 1
				return values[0], err
			}
			if inner && m.failure == 3 {
				return nestedState{}, errors.New("modeled inner join failure")
			}
			if len(values) == 0 {
				return nestedState{}, fmt.Errorf("%s received no branch results", name)
			}
			base := values[0].Round * m.roundWeight()
			out, err := cloneNested(values[0])
			if err != nil {
				return nestedState{}, err
			}
			out.Sum = base
			seen := make(map[int]bool, len(values))
			for _, value := range values {
				if value.Round != out.Round || value.Inputs != out.Inputs || value.Owner != out.Owner || value.Values["sum"] != value.Sum {
					return nestedState{}, fmt.Errorf("mixed nested join inputs: %+v", values)
				}
				contribution := value.Sum - base
				if inner {
					branch := value.Branch
					if branch < 0 || branch >= m.width || seen[branch] || contribution != branch+1 {
						return nestedState{}, fmt.Errorf("invalid inner branch: %+v", value)
					}
					seen[branch] = true
				} else if contribution != 100 && contribution != m.leafWeight() && !(m.failure == 1 && contribution == m.leafWeight()-1) {
					return nestedState{}, fmt.Errorf("invalid outer contribution: %+v", value)
				}
				out.Sum += contribution
			}
			if inner {
				if len(seen) != len(values) || (m.failure == 1 && len(values) != m.width-1) || (m.failure != 1 && len(values) != m.width) {
					return nestedState{}, fmt.Errorf("inner join width=%d want policy=%d: %+v", len(values), m.failure, values)
				}
				out.Sum = base
				for branch := range seen {
					out.Sum += branch + 1
				}
			}
			out.Branch = -1
			stamp(&out, nextPhase)
			if !inner {
				out.Round++
				stamp(&out, phaseBoundary)
			}
			return out, nil
		}
	}
	if err := g.AddJoin(graph.JoinSpec[nestedState]{Name: "innerjoin", From: "nested", OnError: graph.FailInvocation, Merge: merge("innerjoin", phaseInnerJoined, true)}); err != nil {
		t.Fatal(err)
	}
	add("deliver", graph.FailInvocation, func(_ context.Context, _ graph.CallInfo, s nestedState) (graph.Transition[nestedState], error) {
		s.Branch = -1
		stamp(&s, phaseDeliver)
		return graph.To(s, "outerjoin"), nil
	})
	if err := g.AddJoin(graph.JoinSpec[nestedState]{Name: "outerjoin", From: "fork", Merge: merge("outerjoin", phaseBoundary, false)}); err != nil {
		t.Fatal(err)
	}
	add("boundary", graph.FailInvocation, func(_ context.Context, _ graph.CallInfo, s nestedState) (graph.Transition[nestedState], error) {
		if s.Round == nestedRounds {
			return graph.EndExecution(s), nil
		}
		return graph.Wait(s, "advance", "fork"), nil
	})
	for _, pair := range [][2]string{{"fork", "healthy"}, {"fork", "nested"}, {"healthy", "outerjoin"}, {"nested", "innerjoin"}, {"innerjoin", "deliver"}, {"outerjoin", "boundary"}, {"boundary", "fork"}} {
		edge(pair[0], pair[1])
	}
	for i := range m.width {
		edge("nested", fmt.Sprintf("leaf%d", i))
		edge(fmt.Sprintf("leaf%d", i), "innerjoin")
	}
	edge("deliver", "outerjoin")
	if err := graph.RegisterContinuation(g, "advance", func(data []byte) (int, error) { return strconv.Atoi(string(data)) }, func(ctx context.Context, call graph.CallInfo, s nestedState, input int) (nestedState, error) {
		if err := c.attempt(ctx, call, "apply", s); err != nil {
			s.Values["speculative"] = 1
			return s, err
		}
		if input != 1 {
			return s, fmt.Errorf("unexpected nested continuation input %d", input)
		}
		s.Inputs++
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[nestedState]{MachineID: "nested-model-v1", Clone: cloneNested})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func nestedSeeds(parallel bool) [][]byte {
	var seeds [][]byte
	for widthBit := byte(0); widthBit <= 1; widthBit++ {
		for failure := byte(0); failure < 4; failure++ {
			header := widthBit | failure<<3
			if parallel {
				header |= 2 | 4
			}
			seeds = append(seeds, []byte{header, 56, 0}) // normal, budget 8, first matching event
		}
	}
	for mode := byte(1); mode <= 7; mode++ {
		seeds = append(seeds, []byte{0, 56 + mode, 0})
	}
	// The second join callback is the outer join for a serial width-two run.
	seeds = append(seeds, []byte{0, 61, 1})
	return seeds
}

func decodeNested(data []byte) (nestedOracle, int, int, []instruction) {
	header := byte(0)
	if len(data) > 0 {
		header, data = data[0], data[1:]
	}
	width, concurrency, capacity := 2, 1, 1
	if header&1 != 0 {
		width = 4
	}
	if header&2 != 0 {
		concurrency = 4
	}
	if header&4 != 0 {
		capacity = 4
	}
	oracle := nestedOracle{width: width, failure: int((header >> 3) % 4)}
	var ops []instruction
	for len(data) > 0 && len(ops) < 64 {
		a, b := data[0], byte(0)
		data = data[1:]
		if len(data) > 0 {
			b, data = data[0], data[1:]
		}
		ops = append(ops, instruction{mode: int(a % 8), target: 1 + int(b%4), budget: 1 + int((a/8)%8)})
	}
	return oracle, concurrency, capacity, ops
}
