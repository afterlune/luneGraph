package modeltest

import (
	"fmt"
	"maps"
	"reflect"
	"strings"

	graph "github.com/afterlune/luneGraph"
)

const rounds = 4

type state struct {
	Owner  string
	Round  int
	Sum    int
	Inputs int
	Branch int
	Values map[string]int
}

func clone(s state) (state, error) { s.Values = maps.Clone(s.Values); return s, nil }

// The oracle knows business arithmetic and public commit boundaries, not
// scheduling order, callback allocation, or the executor's implementation.
type model struct{ width int }

func (m model) weight() int {
	if m.width == 0 {
		return 1
	}
	return m.width * (m.width + 1) / 2
}

func (m model) roundSteps() int {
	if m.width == 0 {
		return 1
	}
	return m.width + 2
}

func (m model) checkState(s state) error {
	if s.Owner != "model-run" || s.Round < 0 || s.Round > rounds || s.Inputs < 0 || s.Inputs > rounds-1 || s.Inputs < s.Round-1 || s.Inputs > s.Round {
		return fmt.Errorf("invalid ownership/round/input: %+v", s)
	}
	want := s.Round * m.weight()
	if s.Branch != -1 {
		if s.Branch < 0 || s.Branch >= m.width {
			return fmt.Errorf("invalid branch: %+v", s)
		}
		want += s.Branch + 1
	}
	if s.Sum != want || !reflect.DeepEqual(s.Values, map[string]int{"sum": want}) {
		return fmt.Errorf("state differs from arithmetic model: %+v want sum=%d", s, want)
	}
	return nil
}

func (m model) check(cp graph.Checkpoint[state]) error {
	if cp.RunID != "model-run" || cp.MachineID != "model-v1" || cp.FormatVersion != graph.CheckpointFormatVersion || cp.Failure != nil || cp.HadLocalFailures || len(cp.Groups) > 1 || len(cp.Invocations) > m.width+1 {
		return fmt.Errorf("invalid checkpoint boundary: %+v", cp)
	}
	for _, inv := range cp.Invocations {
		if err := m.checkState(inv.State); err != nil {
			return err
		}
		if inv.Status != graph.InvocationReady && inv.Status != graph.InvocationWaiting && inv.Status != graph.InvocationGroup && inv.Status != graph.InvocationJoined {
			return fmt.Errorf("unexpected invocation status: %+v", inv)
		}
		s := inv.State
		switch {
		case inv.Node == "work" && m.width == 0:
			want := s.Round
			if inv.Status == graph.InvocationWaiting {
				want--
			}
			if s.Branch != -1 || s.Inputs != want {
				return fmt.Errorf("loop phase differs from model: %+v", inv)
			}
		case inv.Node == "fork" && m.width > 0:
			if s.Branch != -1 || s.Inputs != s.Round {
				return fmt.Errorf("fork phase differs from model: %+v", inv)
			}
		case inv.Node == "boundary" && m.width > 0:
			if s.Branch != -1 || s.Inputs != s.Round-1 {
				return fmt.Errorf("boundary phase differs from model: %+v", inv)
			}
		case strings.HasPrefix(inv.Node, "b") && m.width > 0:
			if s.Branch != -1 || s.Inputs != s.Round || inv.Status != graph.InvocationReady {
				return fmt.Errorf("branch entry differs from model: %+v", inv)
			}
		case inv.Node == "join" && m.width > 0:
			if s.Branch != inv.BranchIndex || s.Inputs != s.Round || inv.Status != graph.InvocationJoined {
				return fmt.Errorf("branch result differs from model: %+v", inv)
			}
		default:
			return fmt.Errorf("unexpected model node: %+v", inv)
		}
	}
	if len(cp.Groups) == 1 {
		g := cp.Groups[0]
		if m.width == 0 || g.Source != "fork" || g.JoinNode != "join" || len(g.Children) != m.width {
			return fmt.Errorf("invalid fanout shape: %+v", g)
		}
		joined := 0
		parentRound := -1
		for _, inv := range cp.Invocations {
			if inv.ID == g.ParentID {
				parentRound = inv.State.Round
			}
			if inv.Status == graph.InvocationJoined {
				joined++
			}
		}
		if parentRound < 0 || cp.Steps != uint64(parentRound*m.roundSteps()+1+joined) {
			return fmt.Errorf("partial fanout step count differs from model: %+v", cp)
		}
	} else if len(cp.Invocations) == 1 && cp.Invocations[0].Status == graph.InvocationReady {
		s := cp.Invocations[0].State
		want := s.Round * m.roundSteps()
		if cp.Invocations[0].Node == "boundary" {
			want--
		}
		if cp.Steps != uint64(want) {
			return fmt.Errorf("ready phase steps differ from model: %+v", cp)
		}
	}
	var boundary *state
	if cp.Completed {
		if cp.Final == nil || len(cp.Invocations) != 0 || len(cp.Groups) != 0 {
			return fmt.Errorf("invalid final topology: %+v", cp)
		}
		boundary = cp.Final
		if boundary.Round != rounds {
			return fmt.Errorf("premature completion: %+v", cp)
		}
	} else if len(cp.Invocations) == 1 && cp.Invocations[0].Status == graph.InvocationWaiting {
		boundary = &cp.Invocations[0].State
		if len(cp.Groups) != 0 {
			return fmt.Errorf("waiting with a live group")
		}
	}
	if boundary != nil {
		if err := m.checkState(*boundary); err != nil {
			return err
		}
		if boundary.Branch != -1 || boundary.Inputs != boundary.Round-1 || cp.Steps != uint64(boundary.Round*m.roundSteps()) || cp.Revision != 1+cp.Steps+uint64(boundary.Inputs) {
			return fmt.Errorf("boundary differs from model: %+v", cp)
		}
	}
	return nil
}
