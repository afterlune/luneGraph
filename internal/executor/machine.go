package executor

import (
	"fmt"
	"github.com/afterlune/luneGraph/internal/diagram"
	"sort"
	"strings"
)

// Runner is an immutable compiled machine safe to share across executions.
type Runner[S any] struct {
	id            string
	entry         string
	clone         Clone[S]
	nodes         map[string]NodeSpec[S]
	joins         map[string]JoinSpec[S]
	joinBySource  map[string]string
	edges         map[string]map[string]struct{}
	continuations map[string]Continuation[S]
	returnTargets map[string]string
}

func New[S any](machine Machine[S]) (*Runner[S], error) {
	return &Runner[S]{
		id:            machine.ID,
		entry:         machine.Entry,
		clone:         machine.Clone,
		nodes:         machine.Nodes,
		joins:         machine.Joins,
		joinBySource:  machine.JoinBySource,
		edges:         machine.Edges,
		continuations: machine.Continuations,
		returnTargets: machine.ReturnTargets,
	}, nil
}

func (r *Runner[S]) checkTargets(from string, targets []string) error {
	if len(targets) == 0 {
		return fmt.Errorf("node %q selected no target", from)
	}
	if len(targets) == 1 {
		target := targets[0]
		if _, ok := r.edges[from][target]; !ok {
			return fmt.Errorf("node %q selected unknown edge to %q", from, target)
		}
		return nil
	}
	if len(targets) <= 8 {
		for i, target := range targets {
			if _, ok := r.edges[from][target]; !ok {
				return fmt.Errorf("node %q selected unknown edge to %q", from, target)
			}
			for j := 0; j < i; j++ {
				if targets[j] == target {
					return fmt.Errorf("node %q selected duplicate target %q", from, target)
				}
			}
		}
		return nil
	}
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if _, ok := r.edges[from][target]; !ok {
			return fmt.Errorf("node %q selected unknown edge to %q", from, target)
		}
		if _, ok := seen[target]; ok {
			return fmt.Errorf("node %q selected duplicate target %q", from, target)
		}
		seen[target] = struct{}{}
	}
	return nil
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
	for _, name := range sortedKeys(r.nodes) {
		if name == r.entry {
			continue
		}
		fmt.Fprintf(&sb, "%s%s[\"%s\"]\n", indent, id(name), diagram.Label(name))
	}
	for _, name := range sortedKeys(r.joins) {
		fmt.Fprintf(&sb, "%s%s{{\"%s (join)\"}}\n", indent, id(name), diagram.Label(name))
	}
	for _, from := range sortedKeys(r.edges) {
		for _, to := range sortedKeys(r.edges[from]) {
			fmt.Fprintf(&sb, "%s%s --> %s\n", indent, id(from), id(to))
		}
	}
	for _, from := range sortedKeys(r.returnTargets) {
		to := r.returnTargets[from]
		if to != "" {
			fmt.Fprintf(&sb, "%s%s -.->|return| %s\n", indent, id(from), id(to))
		}
	}
	return sb.String()
}

func sortedKeys[V any](items map[string]V) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
