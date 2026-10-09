package modeltest

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func runner(t testing.TB, m model, c *controller) *graph.Runner[state] {
	t.Helper()
	entry := "work"
	if m.width > 0 {
		entry = "fork"
	}
	g := graph.New[state](entry)
	add := func(name string, body graph.Node[state]) {
		t.Helper()
		err := g.AddNode(graph.NodeSpec[state]{Name: name, Run: func(ctx context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
			if err := c.attempt(ctx, call, "node", s); err != nil {
				s.Values["speculative"] = 1
				return graph.To(s, entry), err
			}
			return body(ctx, call, s)
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	edge := func(from, to string) {
		t.Helper()
		if err := g.AddEdge(from, to); err != nil {
			t.Fatal(err)
		}
	}
	finish := func(s state) (graph.Transition[state], error) {
		if s.Round == rounds {
			return graph.EndExecution(s), nil
		}
		return graph.Wait(s, "advance", entry), nil
	}
	if m.width == 0 {
		add("work", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
			s.Round++
			s.Sum++
			s.Values["sum"] = s.Sum
			return finish(s)
		})
		edge("work", "work")
	} else {
		targets := make([]string, m.width)
		for i := range targets {
			targets[i] = fmt.Sprintf("b%d", i)
		}
		add("fork", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
			return graph.To(s, targets...), nil
		})
		for i, name := range targets {
			add(name, func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
				s.Sum += i + 1
				s.Branch = i
				s.Values["sum"] = s.Sum
				return graph.To(s, "join"), nil
			})
		}
		if err := g.AddJoin(graph.JoinSpec[state]{Name: "join", From: "fork", Merge: func(ctx context.Context, call graph.CallInfo, values []state) (state, error) {
			if err := c.attempt(ctx, call, "join", values); err != nil {
				values[0].Values["speculative"] = 1
				return values[0], err
			}
			if len(values) != m.width {
				return state{}, fmt.Errorf("join width=%d", len(values))
			}
			out, _ := clone(values[0])
			out.Sum = 0
			base := values[0].Round * m.weight()
			for i, s := range values {
				if err := m.checkState(s); err != nil {
					return state{}, err
				}
				if s.Round != out.Round || s.Branch != i || s.Inputs != out.Inputs {
					return state{}, fmt.Errorf("mixed branches: %+v", values)
				}
				out.Sum += s.Sum - base
			}
			out.Sum += base
			out.Round++
			out.Branch = -1
			out.Values["sum"] = out.Sum
			return out, nil
		}}); err != nil {
			t.Fatal(err)
		}
		add("boundary", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) { return finish(s) })
		for _, name := range targets {
			edge("fork", name)
			edge(name, "join")
		}
		edge("join", "boundary")
		edge("boundary", "fork")
	}
	if err := graph.RegisterContinuation(g, "advance", func(b []byte) (int, error) { return strconv.Atoi(string(b)) }, func(ctx context.Context, call graph.CallInfo, s state, input int) (state, error) {
		if err := c.attempt(ctx, call, "apply", s); err != nil {
			s.Values["speculative"] = 1
			return s, err
		}
		if input != 1 {
			return s, fmt.Errorf("input=%d", input)
		}
		s.Inputs += input
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
	r, err := g.Compile(graph.Config[state]{MachineID: "model-v1", Clone: clone})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
