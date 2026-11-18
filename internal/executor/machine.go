package executor

import (
	"fmt"
	"slices"
	"strings"

	"github.com/afterlune/luneGraph/internal/diagram"
)

// Runner is an immutable compiled graph safe to share across executions.
type Runner[S any] struct {
	id            string
	entry         string
	entryIndex    int
	clone         Clone[S]
	nodes         []CompiledNode[S]
	edges         []CompiledEdge
	joins         []CompiledJoin[S]
	continuations []CompiledContinuation[S]
	nodeIndex     map[string]int
	joinIndex     map[string]int
	contIndex     map[string]int
}

func New[S any](compiled CompiledGraph[S]) *Runner[S] {
	return &Runner[S]{
		id:            compiled.ID,
		entry:         compiled.Entry,
		entryIndex:    compiled.EntryIndex,
		clone:         compiled.Clone,
		nodes:         compiled.Nodes,
		edges:         compiled.Edges,
		joins:         compiled.Joins,
		continuations: compiled.Continuations,
		nodeIndex:     compiled.NodeIndex,
		joinIndex:     compiled.JoinIndex,
		contIndex:     compiled.ContinuationIndex,
	}
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
	if i := findEdge(edges, target); i >= 0 {
		return edges[i], nil
	}
	return CompiledEdge{}, fmt.Errorf("node %q selected unknown edge to %q", from, target)
}

func (r *Runner[S]) checkTargets(from string, targets []string) error {
	if len(targets) == 0 {
		return fmt.Errorf("node %q selected no target", from)
	}
	node := r.node(from)
	if node == nil {
		return fmt.Errorf("node %q is unknown", from)
	}
	allowed := r.edges[node.EdgeStart:node.EdgeEnd]
	if len(targets) == 1 {
		target := targets[0]
		if findEdge(allowed, target) < 0 {
			return fmt.Errorf("node %q selected unknown edge to %q", from, target)
		}
		return nil
	}
	if len(targets) <= 8 {
		for i, target := range targets {
			if findEdge(allowed, target) < 0 {
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
		if findEdge(allowed, target) < 0 {
			return fmt.Errorf("node %q selected unknown edge to %q", from, target)
		}
		if _, ok := seen[target]; ok {
			return fmt.Errorf("node %q selected duplicate target %q", from, target)
		}
		seen[target] = struct{}{}
	}
	return nil
}

func findEdge(allowed []CompiledEdge, target string) int {
	for i := range allowed {
		if allowed[i].Target == target {
			return i
		}
	}
	return -1
}

// ExportMermaid renders the compiled graph topology as a Mermaid flowchart.
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
		for _, to := range r.edges[r.nodes[i].EdgeStart:r.nodes[i].EdgeEnd] {
			fmt.Fprintf(&sb, "%s%s --> %s\n", indent, id(from), id(to.Target))
		}
	}
	for i := range r.joins {
		if r.joins[i].NextEdge.Target != "" {
			fmt.Fprintf(&sb, "%s%s --> %s\n", indent, id(r.joins[i].Spec.Name), id(r.joins[i].NextEdge.Target))
		}
	}
	for i := range r.nodes {
		if r.nodes[i].ReturnEdge.Target != "" {
			fmt.Fprintf(&sb, "%s%s -.->|return| %s\n", indent, id(r.nodes[i].Spec.Name), id(r.nodes[i].ReturnEdge.Target))
		}
	}
	return sb.String()
}
