package definition

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/afterlune/luneGraph/internal/model"
)

type rawNode[S any] struct {
	spec         model.NodeSpec[S]
	returnTarget string
	mounted      bool
}

type flatBuilder[S any] struct {
	nodes             []rawNode[S]
	joins             []model.JoinSpec[S]
	continuations     []model.CompiledContinuation[S]
	edgesByFrom       map[string][]string
	joinBySource      map[string]string
	vertexNames       map[string]string
	continuationNames map[string]string
}

func expand[S any](root *Graph[S]) (model.CompiledGraph[S], error) {
	if root == nil {
		return model.CompiledGraph[S]{}, fmt.Errorf("subgraph at %q is nil", "")
	}
	rootEntry, err := root.resolveEntry("", make(map[*Graph[S]]bool))
	if err != nil {
		return model.CompiledGraph[S]{}, err
	}
	b := flatBuilder[S]{
		edgesByFrom:       make(map[string][]string),
		joinBySource:      make(map[string]string),
		vertexNames:       make(map[string]string),
		continuationNames: make(map[string]string),
	}
	if err := root.flatten("", "", false, make(map[*Graph[S]]bool), &b); err != nil {
		return model.CompiledGraph[S]{}, err
	}

	slices.SortFunc(b.nodes, func(x, y rawNode[S]) int {
		return strings.Compare(x.spec.Name, y.spec.Name)
	})
	slices.SortFunc(b.joins, func(x, y model.JoinSpec[S]) int {
		return strings.Compare(x.Name, y.Name)
	})
	slices.SortFunc(b.continuations, func(x, y model.CompiledContinuation[S]) int {
		return strings.Compare(x.Key, y.Key)
	})

	compiled := model.CompiledGraph[S]{
		Entry:         rootEntry,
		Nodes:         make([]model.CompiledNode[S], len(b.nodes)),
		Joins:         make([]model.CompiledJoin[S], len(b.joins)),
		Continuations: b.continuations,
		NodeByName:    make(map[string]*model.CompiledNode[S], len(b.nodes)),
		JoinByName:    make(map[string]*model.CompiledJoin[S], len(b.joins)),
		ContByKey:     make(map[string]*model.Continuation[S], len(b.continuations)),
	}
	for i := range b.nodes {
		compiled.Nodes[i] = model.CompiledNode[S]{
			Spec:    b.nodes[i].spec,
			Mounted: b.nodes[i].mounted,
		}
		compiled.NodeByName[b.nodes[i].spec.Name] = &compiled.Nodes[i]
	}
	for i := range b.joins {
		compiled.Joins[i] = model.CompiledJoin[S]{Spec: b.joins[i]}
		compiled.JoinByName[b.joins[i].Name] = &compiled.Joins[i]
	}
	for i := range compiled.Continuations {
		compiled.ContByKey[compiled.Continuations[i].Key] = &compiled.Continuations[i].Cont
	}

	for i := range compiled.Joins {
		name := compiled.Joins[i].Spec.Name
		if targets := b.edgesByFrom[name]; len(targets) > 0 {
			slices.Sort(targets)
			compiled.Joins[i].NextEdge = compiled.ResolveTarget(targets[0])
		}
	}
	for i := range compiled.Nodes {
		name := compiled.Nodes[i].Spec.Name
		if jName, ok := b.joinBySource[name]; ok {
			compiled.Nodes[i].FanOutJoin = compiled.JoinByName[jName]
		}
		compiled.Nodes[i].ReturnEdge = compiled.ResolveTarget(b.nodes[i].returnTarget)
		if targets := b.edgesByFrom[name]; len(targets) > 0 {
			slices.Sort(targets)
			targets = slices.Compact(targets)
			edges := make([]model.CompiledEdge[S], len(targets))
			for j, target := range targets {
				edges[j] = compiled.ResolveTarget(target)
			}
			compiled.Nodes[i].Edges = edges
		}
	}
	return compiled, nil
}

func (g *Graph[S]) resolveEntry(prefix string, active map[*Graph[S]]bool) (string, error) {
	if g == nil {
		return "", fmt.Errorf("subgraph at %q is nil", prefix)
	}
	if active[g] {
		return "", fmt.Errorf("recursive subgraph reference at %q", prefix)
	}
	if _, ok := g.nodes[g.entry]; ok {
		return qualify(prefix, g.entry), nil
	}
	if _, ok := g.joins[g.entry]; ok {
		return "", fmt.Errorf("entry %q in graph %q is a join", g.entry, prefix)
	}
	if child, ok := g.subgraphs[g.entry]; ok {
		if child == nil {
			return "", fmt.Errorf("subgraph %q is nil", g.entry)
		}
		active[g] = true
		defer delete(active, g)
		return child.resolveEntry(qualify(prefix, g.entry), active)
	}
	return "", fmt.Errorf("entry vertex %q in graph %q is not registered", g.entry, prefix)
}

func (g *Graph[S]) flatten(prefix, returnTarget string, mounted bool, active map[*Graph[S]]bool, b *flatBuilder[S]) error {
	if g == nil {
		return fmt.Errorf("subgraph at %q is nil", prefix)
	}
	if active[g] {
		return fmt.Errorf("recursive subgraph reference at %q", prefix)
	}
	active[g] = true
	defer delete(active, g)

	targets := make(map[string]string, len(g.nodes)+len(g.joins)+len(g.subgraphs))
	for _, name := range sortedKeys(g.nodes) {
		targets[name] = qualify(prefix, name)
	}
	for _, name := range sortedKeys(g.joins) {
		targets[name] = qualify(prefix, name)
	}
	for _, name := range sortedKeys(g.subgraphs) {
		child := g.subgraphs[name]
		if child == nil {
			return fmt.Errorf("subgraph %q is nil", name)
		}
		entry, err := child.resolveEntry(qualify(prefix, name), active)
		if err != nil {
			return err
		}
		targets[name] = entry
	}

	continuationTargets := make(map[string]string, len(g.continuations))
	for _, key := range sortedKeys(g.continuations) {
		continuationTargets[key] = qualify(prefix, key)
	}

	needsRewrite := prefix != "" || len(g.subgraphs) != 0
	for _, name := range sortedKeys(g.nodes) {
		qualified := targets[name]
		if prev, exists := b.vertexNames[qualified]; exists {
			return fmt.Errorf("flattened vertex name %q collides between %s and node %q", qualified, prev, name)
		}
		b.vertexNames[qualified] = fmt.Sprintf("node %q", name)
		spec := g.nodes[name]
		spec.Name = qualified
		if needsRewrite {
			spec.Run = rewriteNode(spec.Run, targets, continuationTargets)
		}
		b.nodes = append(b.nodes, rawNode[S]{
			spec:         spec,
			returnTarget: returnTarget,
			mounted:      mounted,
		})
	}

	for _, name := range sortedKeys(g.joins) {
		spec := g.joins[name]
		if _, isNode := g.nodes[spec.FanOutSource]; !isNode {
			return fmt.Errorf("join %q has unknown fan-out source %q", name, spec.FanOutSource)
		}
		qualified := targets[name]
		if prev, exists := b.vertexNames[qualified]; exists {
			return fmt.Errorf("flattened vertex name %q collides between %s and join %q", qualified, prev, name)
		}
		b.vertexNames[qualified] = fmt.Sprintf("join %q", name)
		qualifiedSource := targets[spec.FanOutSource]
		if prev, exists := b.joinBySource[qualifiedSource]; exists {
			return fmt.Errorf("fan-out %q has more than one join (%s, %s)", qualifiedSource, prev, qualified)
		}
		spec.Name = qualified
		spec.FanOutSource = qualifiedSource
		b.joins = append(b.joins, spec)
		b.joinBySource[qualifiedSource] = qualified
	}

	for _, key := range sortedKeys(g.continuations) {
		qualified := continuationTargets[key]
		if prev, exists := b.continuationNames[qualified]; exists {
			return fmt.Errorf("flattened continuation name %q collides with %s", qualified, prev)
		}
		b.continuationNames[qualified] = fmt.Sprintf("continuation %q", key)
		b.continuations = append(b.continuations, model.CompiledContinuation[S]{
			Key:  qualified,
			Cont: g.continuations[key],
		})
	}

	for _, from := range sortedKeys(g.edges) {
		if _, isSubgraph := g.subgraphs[from]; isSubgraph {
			if len(g.edges[from]) > 1 {
				return fmt.Errorf("mounted graph %q may have at most one outgoing edge", from)
			}
			continue
		}
		if _, isJoin := g.joins[from]; isJoin && len(g.edges[from]) > 1 {
			return fmt.Errorf("join %q must have at most one outgoing edge", from)
		}
		qualifiedFrom, ok := targets[from]
		if !ok {
			return fmt.Errorf("unknown edge source %q in graph %q", from, prefix)
		}
		for _, to := range sortedKeys(g.edges[from]) {
			qualifiedTo, ok := targets[to]
			if !ok {
				return fmt.Errorf("unknown edge target %q in graph %q", to, prefix)
			}
			b.edgesByFrom[qualifiedFrom] = append(b.edgesByFrom[qualifiedFrom], qualifiedTo)
		}
	}

	for _, name := range sortedKeys(g.subgraphs) {
		child := g.subgraphs[name]
		var next string
		if outgoing := g.edges[name]; len(outgoing) == 1 {
			local := firstKey(outgoing)
			mapped, exists := targets[local]
			if !exists {
				return fmt.Errorf("unknown edge target %q in graph %q", local, prefix)
			}
			next = mapped
		}
		if err := child.flatten(qualify(prefix, name), next, true, active, b); err != nil {
			return err
		}
	}
	return nil
}

func rewriteNode[S any](run model.NodeFunc[S], targets, continuations map[string]string) model.NodeFunc[S] {
	return func(ctx context.Context, call model.CallInfo, state S) (model.Transition[S], error) {
		transition, err := run(ctx, call, state)
		if err != nil {
			return transition, err
		}
		return model.RemapTransition(transition, targets, continuations), nil
	}
}

func qualify(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

func firstKey(items map[string]struct{}) string {
	for key := range items {
		return key
	}
	return ""
}
