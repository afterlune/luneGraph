package modeltest

import (
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"

	graph "github.com/afterlune/luneGraph"
)

const nestedRounds = 4

const (
	phaseRoot = iota
	phaseHealthy
	phaseNested
	phaseLeafBase    = 10
	phaseInnerJoined = 20
	phaseDeliver     = 21
	phaseBoundary    = 22
)

type nestedState struct {
	Owner  string
	Round  int
	Inputs int
	Sum    int
	Branch int
	Values map[string]int
}

func cloneNested(s nestedState) (nestedState, error) {
	s.Values = maps.Clone(s.Values)
	return s, nil
}

type nestedOracle struct {
	width   int
	failure int // 0 none, 1 leaf FailInvocation, 2 leaf FailGroup, 3 inner join FailInvocation
}

func (m nestedOracle) leafWeight() int { return m.width * (m.width + 1) / 2 }
func (m nestedOracle) roundWeight() int {
	weight := 100
	if m.failure == 0 {
		weight += m.leafWeight()
	}
	if m.failure == 1 {
		weight += m.leafWeight() - 1
	}
	return weight
}

func (m nestedOracle) checkState(s nestedState, phase int) error {
	if s.Owner != "model-run" || s.Round < 0 || s.Round > nestedRounds || s.Inputs < 0 || s.Inputs > nestedRounds-1 {
		return fmt.Errorf("invalid nested owner/round/input: %+v", s)
	}
	base := s.Round * m.roundWeight()
	wantBranch, wantSum := -1, base
	switch {
	case phase == phaseRoot || phase == phaseNested:
	case phase == phaseHealthy:
		wantSum += 100
	case phase >= phaseLeafBase && phase < phaseLeafBase+m.width:
		wantBranch = phase - phaseLeafBase
		wantSum += wantBranch + 1
	case phase == phaseInnerJoined:
		wantSum += m.leafWeight()
		if m.failure == 1 {
			wantSum--
		}
	case phase == phaseDeliver:
		wantSum += m.leafWeight()
		if m.failure == 1 {
			wantSum--
		}
	case phase == phaseBoundary:
		wantSum = s.Round * m.roundWeight()
	default:
		return fmt.Errorf("unknown nested phase %d", phase)
	}
	if s.Branch != wantBranch || s.Sum != wantSum || !reflect.DeepEqual(s.Values, map[string]int{"sum": wantSum, "phase": phase, "branch": wantBranch}) {
		return fmt.Errorf("nested state differs from oracle: %+v phase=%d want branch=%d sum=%d", s, phase, wantBranch, wantSum)
	}
	if phase == phaseBoundary {
		if s.Round == 0 || s.Inputs < s.Round-1 || s.Inputs > s.Round {
			return fmt.Errorf("invalid boundary input position: %+v", s)
		}
	} else if s.Inputs != s.Round {
		return fmt.Errorf("phase input position differs from round: %+v phase=%d", s, phase)
	}
	return nil
}

func (m nestedOracle) check(cp graph.Checkpoint[nestedState]) error {
	if cp.RunID != "model-run" || cp.MachineID != "nested-model-v1" || cp.FormatVersion != graph.CheckpointFormatVersion || cp.Failure != nil || (cp.HadLocalFailures && m.failure == 0) {
		return fmt.Errorf("invalid nested checkpoint header/failure: %+v", cp)
	}
	if len(cp.Invocations) > m.width+3 || len(cp.Groups) > 2 {
		return fmt.Errorf("nested checkpoint exceeds graph bounds: %+v", cp)
	}
	invocations := make(map[string]graph.Invocation[nestedState], len(cp.Invocations))
	for _, inv := range cp.Invocations {
		if inv.ID == "" || invocations[inv.ID].ID != "" {
			return fmt.Errorf("missing or duplicate invocation: %+v", inv)
		}
		invocations[inv.ID] = inv
		phase := -1
		switch {
		case inv.Node == "fork":
			phase = inv.State.Values["phase"]
			if phase != phaseRoot && phase != phaseBoundary {
				return fmt.Errorf("fork state is not at a round boundary: %+v", inv)
			}
		case inv.Node == "healthy":
			phase = phaseRoot
		case inv.Node == "nested":
			phase = phaseRoot
			if inv.Status == graph.InvocationGroup || inv.Status == graph.InvocationFailed {
				phase = phaseNested
			}
		case strings.HasPrefix(inv.Node, "leaf"):
			i, err := strconv.Atoi(strings.TrimPrefix(inv.Node, "leaf"))
			if err != nil || i < 0 || i >= m.width {
				return fmt.Errorf("unexpected leaf invocation: %+v", inv)
			}
			phase = phaseNested
			if inv.Status == graph.InvocationJoined {
				phase = phaseLeafBase + i
			}
		case inv.Node == "innerjoin":
			if inv.Status != graph.InvocationJoined {
				return fmt.Errorf("inner join child is not joined: %+v", inv)
			}
			phase = phaseLeafBase + inv.BranchIndex
		case inv.Node == "outerjoin":
			if inv.Status != graph.InvocationJoined {
				return fmt.Errorf("outer join child is not joined: %+v", inv)
			}
			if inv.State.Values["phase"] == phaseHealthy {
				phase = phaseHealthy
			} else {
				phase = phaseDeliver
			}
		case inv.Node == "deliver":
			phase = phaseInnerJoined
		case inv.Node == "boundary":
			phase = phaseBoundary
		default:
			return fmt.Errorf("unexpected nested invocation: %+v", inv)
		}
		if inv.Status == graph.InvocationFailed {
			if inv.Node == "nested" || strings.HasPrefix(inv.Node, "leaf") {
				phase = phaseNested
			} else {
				return fmt.Errorf("failure outside modeled local scope: %+v", inv)
			}
		}
		if err := m.checkState(inv.State, phase); err != nil {
			return err
		}
		if inv.Status != graph.InvocationReady && inv.Status != graph.InvocationWaiting && inv.Status != graph.InvocationGroup && inv.Status != graph.InvocationJoined && inv.Status != graph.InvocationFailed {
			return fmt.Errorf("unexpected nested invocation status: %+v", inv)
		}
		if inv.Status == graph.InvocationWaiting && (inv.Node != "boundary" || inv.Continuation != "advance") {
			return fmt.Errorf("unexpected nested wait: %+v", inv)
		}
	}
	groups := make(map[string]graph.ActivationGroup, len(cp.Groups))
	for _, group := range cp.Groups {
		if group.ID == "" || groups[group.ID].ID != "" {
			return fmt.Errorf("missing or duplicate activation group: %+v", group)
		}
		groups[group.ID] = group
		parent, ok := invocations[group.ParentID]
		if !ok || parent.ChildGroupID != group.ID {
			return fmt.Errorf("group parent mismatch: %+v", group)
		}
		wantParent, wantPhase := "fork", phaseRoot
		if group.Source == "nested" {
			wantParent, wantPhase = "nested", phaseNested
		}
		if parent.Node != wantParent || parent.Status != graph.InvocationGroup || parent.State.Values["phase"] != wantPhase {
			return fmt.Errorf("group attached to wrong parent: group=%+v parent=%+v", group, parent)
		}
		wantChildren, wantJoin := 2, "outerjoin"
		if group.Source == "nested" {
			wantChildren, wantJoin = m.width, "innerjoin"
		} else if group.Source != "fork" {
			return fmt.Errorf("unknown activation source: %+v", group)
		}
		if group.JoinNode != wantJoin || len(group.Children) != wantChildren {
			return fmt.Errorf("invalid nested group shape: %+v", group)
		}
		seen := make(map[string]bool, len(group.Children))
		for i, id := range group.Children {
			child, exists := invocations[id]
			if !exists || child.GroupID != group.ID || seen[id] {
				return fmt.Errorf("invalid group child %q in %+v", id, group)
			}
			wantNode := fmt.Sprintf("leaf%d", i)
			if group.Source == "fork" {
				if i == 0 {
					wantNode = "healthy"
				} else {
					wantNode = "nested"
					if child.State.Values["phase"] == phaseInnerJoined {
						wantNode = "deliver"
					}
				}
				if child.Status == graph.InvocationJoined {
					wantNode = "outerjoin"
				}
			} else if child.Status == graph.InvocationJoined {
				wantNode = "innerjoin"
			}
			if child.Node != wantNode || child.BranchIndex != i {
				return fmt.Errorf("activation child order differs: got=%+v want node=%s index=%d", child, wantNode, i)
			}
			seen[id] = true
		}
	}
	for _, inv := range cp.Invocations {
		if inv.GroupID != "" {
			group, exists := groups[inv.GroupID]
			if !exists || !slicesContains(group.Children, inv.ID) {
				return fmt.Errorf("invocation group membership mismatch: %+v", inv)
			}
		}
		if inv.ChildGroupID != "" {
			if _, exists := groups[inv.ChildGroupID]; !exists {
				return fmt.Errorf("missing child group: %+v", inv)
			}
		}
	}
	if cp.Completed {
		if len(cp.Invocations) != 0 || len(cp.Groups) != 0 || cp.Final == nil || cp.Final.Round != nestedRounds {
			return fmt.Errorf("invalid nested terminal topology: %+v", cp)
		}
		want := nestedRounds * m.roundWeight()
		if cp.HadLocalFailures != (m.failure != 0) {
			return fmt.Errorf("terminal failure flag differs: %+v", cp)
		}
		if cp.Final.Sum != want {
			return fmt.Errorf("nested final sum=%d want=%d", cp.Final.Sum, want)
		}
		return m.checkState(*cp.Final, phaseBoundary)
	}
	if len(cp.Groups) > 0 {
		outer := 0
		for _, group := range cp.Groups {
			if group.Source == "fork" {
				outer++
			}
		}
		if outer != 1 {
			return fmt.Errorf("nested checkpoint must retain its outer activation: %+v", cp.Groups)
		}
	}
	return nil
}

func slicesContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
