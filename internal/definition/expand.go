package definition

import (
	"context"
	"fmt"
	"sort"

	"github.com/afterlune/luneGraph/internal/model"
)

type component[S any] struct {
	graph    *Graph[S]
	prefix   string
	children map[string]*component[S]
}

func expand[S any](root *Graph[S]) (model.Machine[S], error) {
	rootComponent, err := buildComponent(root, "", make(map[*Graph[S]]bool))
	if err != nil {
		return model.Machine[S]{}, err
	}
	entry, err := componentEntry(rootComponent)
	if err != nil {
		return model.Machine[S]{}, err
	}
	machine := model.Machine[S]{
		Entry:         entry,
		Nodes:         make(map[string]model.NodeSpec[S]),
		Joins:         make(map[string]model.JoinSpec[S]),
		JoinBySource:  make(map[string]string),
		Edges:         make(map[string]map[string]struct{}),
		Continuations: make(map[string]model.Continuation[S]),
		ReturnTargets: make(map[string]string),
	}
	vertices := make(map[string]string)
	continuationNames := make(map[string]string)
	if err := flatten(rootComponent, "", false, &machine, vertices, continuationNames); err != nil {
		return model.Machine[S]{}, err
	}
	return machine, nil
}

func buildComponent[S any](g *Graph[S], prefix string, active map[*Graph[S]]bool) (*component[S], error) {
	if g == nil {
		return nil, fmt.Errorf("subgraph at %q is nil", prefix)
	}
	if active[g] {
		return nil, fmt.Errorf("recursive subgraph reference at %q", prefix)
	}
	active[g] = true
	defer delete(active, g)

	c := &component[S]{graph: g, prefix: prefix, children: make(map[string]*component[S], len(g.subgraphs))}
	for _, name := range sortedKeys(g.subgraphs) {
		child := g.subgraphs[name]
		if child == nil {
			return nil, fmt.Errorf("subgraph %q is nil", name)
		}
		built, err := buildComponent(child, qualify(prefix, name), active)
		if err != nil {
			return nil, err
		}
		c.children[name] = built
	}
	return c, nil
}

func componentEntry[S any](c *component[S]) (string, error) {
	if _, ok := c.graph.nodes[c.graph.entry]; ok {
		return qualify(c.prefix, c.graph.entry), nil
	}
	if _, ok := c.graph.joins[c.graph.entry]; ok {
		return "", fmt.Errorf("entry %q in graph %q is a join", c.graph.entry, c.prefix)
	}
	if child, ok := c.children[c.graph.entry]; ok {
		return componentEntry(child)
	}
	return "", fmt.Errorf("entry vertex %q in graph %q is not registered", c.graph.entry, c.prefix)
}

func flatten[S any](c *component[S], returnTarget string, mounted bool, machine *model.Machine[S], vertices, continuationNames map[string]string) error {
	targets := make(map[string]string, len(c.graph.nodes)+len(c.graph.joins)+len(c.children))
	for _, name := range sortedKeys(c.graph.nodes) {
		targets[name] = qualify(c.prefix, name)
	}
	for _, name := range sortedKeys(c.graph.joins) {
		targets[name] = qualify(c.prefix, name)
	}
	for _, name := range sortedKeys(c.children) {
		entry, err := componentEntry(c.children[name])
		if err != nil {
			return err
		}
		targets[name] = entry
	}

	continuationTargets := make(map[string]string, len(c.graph.continuations))
	for _, key := range sortedKeys(c.graph.continuations) {
		continuationTargets[key] = qualify(c.prefix, key)
	}

	for _, name := range sortedKeys(c.graph.nodes) {
		qualified := qualify(c.prefix, name)
		if previous, exists := vertices[qualified]; exists {
			return fmt.Errorf("flattened vertex name %q collides between %s and node %q", qualified, previous, name)
		}
		vertices[qualified] = fmt.Sprintf("node %q", name)
		spec := c.graph.nodes[name]
		spec.Name = qualified
		spec.Run = rewriteNode(spec.Run, targets, continuationTargets)
		machine.Nodes[qualified] = spec
		if mounted {
			machine.ReturnTargets[qualified] = returnTarget
		}
	}

	for _, name := range sortedKeys(c.graph.joins) {
		spec := c.graph.joins[name]
		if _, isNode := c.graph.nodes[spec.From]; !isNode {
			return fmt.Errorf("join %q has unknown fan-out source %q", name, spec.From)
		}
		qualified := qualify(c.prefix, name)
		if previous, exists := vertices[qualified]; exists {
			return fmt.Errorf("flattened vertex name %q collides between %s and join %q", qualified, previous, name)
		}
		vertices[qualified] = fmt.Sprintf("join %q", name)
		if _, exists := machine.JoinBySource[targets[spec.From]]; exists {
			return fmt.Errorf("fan-out %q has more than one join", targets[spec.From])
		}
		spec.Name = qualified
		spec.From = targets[spec.From]
		machine.Joins[qualified] = spec
		machine.JoinBySource[spec.From] = qualified
	}

	for _, key := range sortedKeys(c.graph.continuations) {
		qualified := continuationTargets[key]
		if previous, exists := continuationNames[qualified]; exists {
			return fmt.Errorf("flattened continuation name %q collides with %s", qualified, previous)
		}
		continuationNames[qualified] = fmt.Sprintf("continuation %q", key)
		machine.Continuations[qualified] = c.graph.continuations[key]
	}

	for _, from := range sortedKeys(c.graph.edges) {
		if _, isSubgraph := c.children[from]; isSubgraph {
			if len(c.graph.edges[from]) > 1 {
				return fmt.Errorf("subgraph %q must have at most one outgoing edge", from)
			}
			continue
		}
		if _, isJoin := c.graph.joins[from]; isJoin && len(c.graph.edges[from]) > 1 {
			return fmt.Errorf("join %q must have at most one outgoing edge", from)
		}
		qualifiedFrom, ok := targets[from]
		if !ok {
			return fmt.Errorf("unknown edge source %q in graph %q", from, c.prefix)
		}
		for _, to := range sortedKeys(c.graph.edges[from]) {
			qualifiedTo, ok := targets[to]
			if !ok {
				return fmt.Errorf("unknown edge target %q in graph %q", to, c.prefix)
			}
			if machine.Edges[qualifiedFrom] == nil {
				machine.Edges[qualifiedFrom] = make(map[string]struct{})
			}
			machine.Edges[qualifiedFrom][qualifiedTo] = struct{}{}
		}
	}

	for _, name := range sortedKeys(c.children) {
		child := c.children[name]
		var next string
		if outgoing := c.graph.edges[name]; len(outgoing) == 1 {
			local := firstKey(outgoing)
			mapped, exists := targets[local]
			if !exists {
				return fmt.Errorf("subgraph %q returns to unknown vertex %q", name, local)
			}
			next = mapped
		}
		if err := flatten(child, next, true, machine, vertices, continuationNames); err != nil {
			return err
		}
	}
	return nil
}

func rewriteNode[S any](run model.Node[S], targets, continuations map[string]string) model.Node[S] {
	return func(ctx context.Context, call model.CallInfo, state S) (model.Transition[S], error) {
		transition, err := run(ctx, call, state)
		if err != nil {
			return transition, err
		}
		if transition.Action != model.ActionContinue && transition.Action != model.ActionWait {
			return transition, nil
		}
		if len(transition.Targets) != 0 {
			transition.Targets = append([]string(nil), transition.Targets...)
			for index, target := range transition.Targets {
				if mapped, ok := targets[target]; ok {
					transition.Targets[index] = mapped
				} else {
					transition.Targets[index] = ""
				}
			}
		}
		if transition.Action == model.ActionWait {
			if mapped, ok := continuations[transition.Continuation]; ok {
				transition.Continuation = mapped
			} else {
				transition.Continuation = ""
			}
		}
		return transition, nil
	}
}

func qualify(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

func sortedKeys[V any](items map[string]V) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func firstKey(items map[string]struct{}) string {
	for key := range items {
		return key
	}
	return ""
}
