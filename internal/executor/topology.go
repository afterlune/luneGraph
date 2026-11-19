package executor

import (
	"fmt"
	"slices"
	"strings"

	"github.com/afterlune/luneGraph/internal/diagram"
)

// Runner is an immutable compiled graph safe to share across executions.
type Runner[S any] struct {
	definitionID  string
	entry         string
	clone         Clone[S]
	nodes         []CompiledNode[S]
	joins         []CompiledJoin[S]
	continuations []CompiledContinuation[S]
	nodeByName    map[string]*CompiledNode[S]
	joinByName    map[string]*CompiledJoin[S]
	contByKey     map[string]*Continuation[S]
}

func New[S any](compiled CompiledGraph[S]) *Runner[S] {
	r := &Runner[S]{
		definitionID:  compiled.ID,
		entry:         compiled.Entry,
		clone:         compiled.Clone,
		nodes:         compiled.Nodes,
		joins:         compiled.Joins,
		continuations: compiled.Continuations,
		nodeByName:    compiled.NodeByName,
		joinByName:    compiled.JoinByName,
		contByKey:     compiled.ContByKey,
	}
	if r.nodeByName == nil && len(r.nodes) > 0 {
		r.nodeByName = make(map[string]*CompiledNode[S], len(r.nodes))
		for i := range r.nodes {
			r.nodeByName[r.nodes[i].Spec.Name] = &r.nodes[i]
		}
	}
	if r.joinByName == nil && len(r.joins) > 0 {
		r.joinByName = make(map[string]*CompiledJoin[S], len(r.joins))
		for i := range r.joins {
			r.joinByName[r.joins[i].Spec.Name] = &r.joins[i]
		}
	}
	if r.contByKey == nil && len(r.continuations) > 0 {
		r.contByKey = make(map[string]*Continuation[S], len(r.continuations))
		for i := range r.continuations {
			r.contByKey[r.continuations[i].Key] = &r.continuations[i].Cont
		}
	}
	return r
}

func (r *Runner[S]) node(name string) *CompiledNode[S] {
	if r == nil {
		return nil
	}
	return r.nodeByName[name]
}

func (r *Runner[S]) join(name string) *CompiledJoin[S] {
	if r == nil {
		return nil
	}
	return r.joinByName[name]
}

func (r *Runner[S]) continuation(key string) *Continuation[S] {
	if r == nil {
		return nil
	}
	return r.contByKey[key]
}

func (r *Runner[S]) resolveEdge(node *CompiledNode[S], from string, target string) (CompiledEdge[S], error) {
	if target == "" {
		return CompiledEdge[S]{}, fmt.Errorf("node %q selected no target", from)
	}
	if node == nil || node.Spec.Name != from {
		node = r.node(from)
		if node == nil {
			return CompiledEdge[S]{}, fmt.Errorf("node %q is unknown", from)
		}
	}
	if i := findEdge(node.Edges, target); i >= 0 {
		return node.Edges[i], nil
	}
	return CompiledEdge[S]{}, fmt.Errorf("node %q selected unknown edge to %q", from, target)
}

func (r *Runner[S]) checkTargets(from string, targets []string) error {
	if len(targets) == 0 {
		return fmt.Errorf("node %q selected no target", from)
	}
	node := r.node(from)
	if node == nil {
		return fmt.Errorf("node %q is unknown", from)
	}
	allowed := node.Edges
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

func findEdge[S any](allowed []CompiledEdge[S], target string) int {
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
		for _, to := range r.nodes[i].Edges {
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
