package executor

import (
	"fmt"
	"slices"
	"strings"

	"github.com/afterlune/luneGraph/internal/diagram"
)

// Runner is an immutable compiled machine safe to share across executions.
type Runner[S any] struct {
	id            string
	entry         string
	entryIndex    int
	clone         Clone[S]
	nodes         []CompiledNode[S]
	edgeTargets   []string
	edges         []CompiledEdge
	joins         []CompiledJoin[S]
	continuations []CompiledContinuation[S]
	nodeIndex     map[string]int
	joinIndex     map[string]int
	contIndex     map[string]int
}

func New[S any](machine Machine[S]) (*Runner[S], error) {
	nodeIndex := make(map[string]int, len(machine.Nodes))
	for i := range machine.Nodes {
		nodeIndex[machine.Nodes[i].Spec.Name] = i
	}
	joinIndex := make(map[string]int, len(machine.Joins))
	for i := range machine.Joins {
		joinIndex[machine.Joins[i].Spec.Name] = i
	}
	contIndex := make(map[string]int, len(machine.Continuations))
	for i := range machine.Continuations {
		contIndex[machine.Continuations[i].Key] = i
	}

	nodes := machine.Nodes
	for i := range nodes {
		nodes[i].Index = i
		if nodes[i].ReturnTarget != "" {
			if idx, ok := nodeIndex[nodes[i].ReturnTarget]; ok {
				nodes[i].ReturnNodeIndex = idx
			} else {
				nodes[i].ReturnNodeIndex = -1
			}
			if idx, ok := joinIndex[nodes[i].ReturnTarget]; ok {
				nodes[i].ReturnJoinIndex = idx
			} else {
				nodes[i].ReturnJoinIndex = -1
			}
		} else {
			nodes[i].ReturnNodeIndex = -1
			nodes[i].ReturnJoinIndex = -1
		}
	}

	edges := machine.Edges
	if len(edges) != len(machine.EdgeTargets) {
		edges = make([]CompiledEdge, len(machine.EdgeTargets))
		for i, target := range machine.EdgeTargets {
			nIdx := -1
			if idx, ok := nodeIndex[target]; ok {
				nIdx = idx
			}
			jIdx := -1
			if idx, ok := joinIndex[target]; ok {
				jIdx = idx
			}
			edges[i] = CompiledEdge{
				Target:    target,
				NodeIndex: nIdx,
				JoinIndex: jIdx,
			}
		}
	}

	entryIdx := machine.EntryIndex
	if entryIdx < 0 && machine.Entry != "" {
		if idx, ok := nodeIndex[machine.Entry]; ok {
			entryIdx = idx
		}
	}

	return &Runner[S]{
		id:            machine.ID,
		entry:         machine.Entry,
		entryIndex:    entryIdx,
		clone:         machine.Clone,
		nodes:         nodes,
		edgeTargets:   machine.EdgeTargets,
		edges:         edges,
		joins:         machine.Joins,
		continuations: machine.Continuations,
		nodeIndex:     nodeIndex,
		joinIndex:     joinIndex,
		contIndex:     contIndex,
	}, nil
}

func (r *Runner[S]) node(name string) *CompiledNode[S] {
	if r == nil {
		return nil
	}
	if idx, ok := r.nodeIndex[name]; ok {
		return &r.nodes[idx]
	}
	return nil
}

func (r *Runner[S]) join(name string) *CompiledJoin[S] {
	if r == nil {
		return nil
	}
	if idx, ok := r.joinIndex[name]; ok {
		return &r.joins[idx]
	}
	return nil
}

func (r *Runner[S]) continuation(key string) *Continuation[S] {
	if r == nil {
		return nil
	}
	if idx, ok := r.contIndex[key]; ok {
		return &r.continuations[idx].Cont
	}
	return nil
}

func (r *Runner[S]) resolveEdge(node *CompiledNode[S], from string, target string) (CompiledEdge, error) {
	if target == "" {
		return CompiledEdge{}, fmt.Errorf("node %q selected no target", from)
	}
	if node == nil || node.Spec.Name != from {
		node = r.node(from)
		if node == nil {
			return CompiledEdge{}, fmt.Errorf("node %q is unknown", from)
		}
	}
	edges := r.edges[node.EdgeStart:node.EdgeEnd]
	for i := range edges {
		if edges[i].Target == target {
			return edges[i], nil
		}
	}
	return CompiledEdge{}, fmt.Errorf("node %q selected unknown edge to %q", from, target)
}

func (r *Runner[S]) checkTarget(from string, target string) error {
	_, err := r.resolveEdge(nil, from, target)
	return err
}

func (r *Runner[S]) checkTargets(from string, targets []string) error {
	if len(targets) == 0 {
		return fmt.Errorf("node %q selected no target", from)
	}
	node := r.node(from)
	if node == nil {
		return fmt.Errorf("node %q is unknown", from)
	}
	allowed := r.edgeTargets[node.EdgeStart:node.EdgeEnd]
	if len(targets) == 1 {
		target := targets[0]
		if !containsTarget(allowed, target) {
			return fmt.Errorf("node %q selected unknown edge to %q", from, target)
		}
		return nil
	}
	if len(targets) <= 8 {
		for i, target := range targets {
			if !containsTarget(allowed, target) {
				return fmt.Errorf("node %q selected unknown edge to %q", from, target)
			}
			if slices.Contains(targets[:i], target) {
				return fmt.Errorf("node %q selected duplicate target %q", from, target)
			}
		}
		return nil
	}
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if !containsTarget(allowed, target) {
			return fmt.Errorf("node %q selected unknown edge to %q", from, target)
		}
		if _, ok := seen[target]; ok {
			return fmt.Errorf("node %q selected duplicate target %q", from, target)
		}
		seen[target] = struct{}{}
	}
	return nil
}

func containsTarget(allowed []string, target string) bool {
	for i := range allowed {
		if allowed[i] == target {
			return true
		}
	}
	return false
}

// ExportMermaid renders the compiled machine topology as a Mermaid flowchart.
func (r *Runner[S]) ExportMermaid() string {
	if r == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("flowchart TD\n")
	indent := "    "
	var names diagram.Names
	id := names.ID
	if r.entry != "" {
		fmt.Fprintf(&sb, "%s%s([\"%s (entry)\"])\n", indent, id(r.entry), diagram.Label(r.entry))
	}
	for i := range r.nodes {
		name := r.nodes[i].Spec.Name
		if name == r.entry {
			continue
		}
		fmt.Fprintf(&sb, "%s%s[\"%s\"]\n", indent, id(name), diagram.Label(name))
	}
	for i := range r.joins {
		name := r.joins[i].Spec.Name
		fmt.Fprintf(&sb, "%s%s{{\"%s (join)\"}}\n", indent, id(name), diagram.Label(name))
	}
	for i := range r.nodes {
		from := r.nodes[i].Spec.Name
		for _, to := range r.edgeTargets[r.nodes[i].EdgeStart:r.nodes[i].EdgeEnd] {
			fmt.Fprintf(&sb, "%s%s --> %s\n", indent, id(from), id(to))
		}
	}
	for i := range r.joins {
		if r.joins[i].NextTarget != "" {
			fmt.Fprintf(&sb, "%s%s --> %s\n", indent, id(r.joins[i].Spec.Name), id(r.joins[i].NextTarget))
		}
	}
	for i := range r.nodes {
		if r.nodes[i].ReturnTarget != "" {
			fmt.Fprintf(&sb, "%s%s -.->|return| %s\n", indent, id(r.nodes[i].Spec.Name), id(r.nodes[i].ReturnTarget))
		}
	}
	return sb.String()
}
