package definition

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/afterlune/luneGraph/internal/model"
)

type childMount struct {
	name string
	idx  int
}

type componentNode[S any] struct {
	graph        *Graph[S]
	prefix       string
	parentIdx    int
	mountStart   int
	mountCount   int
	returnTarget string
	mounted      bool
}

func expand[S any](root *Graph[S]) (model.Machine[S], error) {
	if root == nil {
		return model.Machine[S]{}, fmt.Errorf("subgraph at %q is nil", "")
	}

	arena := []componentNode[S]{
		{graph: root, prefix: "", parentIdx: -1},
	}
	var mounts []childMount

	for i := 0; i < len(arena); i++ {
		g := arena[i].graph
		if g == nil {
			return model.Machine[S]{}, fmt.Errorf("subgraph at %q is nil", arena[i].prefix)
		}
		subList := make([]subgraphMount[S], len(g.subgraphs))
		copy(subList, g.subgraphs)
		slices.SortFunc(subList, func(a, b subgraphMount[S]) int {
			return strings.Compare(a.name, b.name)
		})

		arena[i].mountStart = len(mounts)
		arena[i].mountCount = len(subList)

		for _, sub := range subList {
			if sub.graph == nil {
				return model.Machine[S]{}, fmt.Errorf("subgraph %q is nil", sub.name)
			}
			childPrefix := qualify(arena[i].prefix, sub.name)
			for p := i; p != -1; p = arena[p].parentIdx {
				if arena[p].graph == sub.graph {
					return model.Machine[S]{}, fmt.Errorf("recursive subgraph reference at %q", childPrefix)
				}
			}
			childIdx := len(arena)
			mounts = append(mounts, childMount{name: sub.name, idx: childIdx})
			arena = append(arena, componentNode[S]{
				graph:     sub.graph,
				prefix:    childPrefix,
				parentIdx: i,
			})
		}
	}

	rootEntry, err := componentEntry(arena, mounts, 0)
	if err != nil {
		return model.Machine[S]{}, err
	}

	type rawNode struct {
		spec         model.NodeSpec[S]
		mounted      bool
		returnTarget string
	}
	var allNodes []rawNode
	var allJoins []model.JoinSpec[S]
	var allContinuations []model.CompiledContinuation[S]
	type rawEdge struct {
		from string
		to   string
	}
	var allEdges []rawEdge

	vertices := make(map[string]string)
	continuationNames := make(map[string]string)
	joinBySource := make(map[string]string)

	for i := 0; i < len(arena); i++ {
		c := &arena[i]

		targets := make(map[string]string, len(c.graph.nodes)+len(c.graph.joins)+c.mountCount)
		for _, n := range c.graph.nodes {
			targets[n.Name] = qualify(c.prefix, n.Name)
		}
		for _, j := range c.graph.joins {
			targets[j.Name] = qualify(c.prefix, j.Name)
		}
		for m := c.mountStart; m < c.mountStart+c.mountCount; m++ {
			entry, err := componentEntry(arena, mounts, mounts[m].idx)
			if err != nil {
				return model.Machine[S]{}, err
			}
			targets[mounts[m].name] = entry
		}

		continuationTargets := make(map[string]string, len(c.graph.continuations))
		for _, cont := range c.graph.continuations {
			continuationTargets[cont.Key] = qualify(c.prefix, cont.Key)
		}

		sortedNodes := make([]model.NodeSpec[S], len(c.graph.nodes))
		copy(sortedNodes, c.graph.nodes)
		slices.SortFunc(sortedNodes, func(a, b model.NodeSpec[S]) int { return strings.Compare(a.Name, b.Name) })

		needsRewrite := c.prefix != "" || c.mountCount != 0
		for _, nodeSpec := range sortedNodes {
			qualified := qualify(c.prefix, nodeSpec.Name)
			if prev, exists := vertices[qualified]; exists {
				return model.Machine[S]{}, fmt.Errorf("flattened vertex name %q collides between %s and node %q", qualified, prev, nodeSpec.Name)
			}
			vertices[qualified] = fmt.Sprintf("node %q", nodeSpec.Name)

			run := nodeSpec.Run
			if needsRewrite {
				run = rewriteNode(nodeSpec.Run, targets, continuationTargets)
			}
			nodeSpec.Name = qualified
			nodeSpec.Run = run
			allNodes = append(allNodes, rawNode{
				spec:         nodeSpec,
				mounted:      c.mounted,
				returnTarget: c.returnTarget,
			})
		}

		sortedJoins := make([]model.JoinSpec[S], len(c.graph.joins))
		copy(sortedJoins, c.graph.joins)
		slices.SortFunc(sortedJoins, func(a, b model.JoinSpec[S]) int { return strings.Compare(a.Name, b.Name) })

		for _, joinSpec := range sortedJoins {
			if !c.graph.isNode(joinSpec.From) {
				return model.Machine[S]{}, fmt.Errorf("join %q has unknown fan-out source %q", joinSpec.Name, joinSpec.From)
			}
			qualified := qualify(c.prefix, joinSpec.Name)
			if prev, exists := vertices[qualified]; exists {
				return model.Machine[S]{}, fmt.Errorf("flattened vertex name %q collides between %s and join %q", qualified, prev, joinSpec.Name)
			}
			vertices[qualified] = fmt.Sprintf("join %q", joinSpec.Name)

			sourceTarget := targets[joinSpec.From]
			if _, exists := joinBySource[sourceTarget]; exists {
				return model.Machine[S]{}, fmt.Errorf("fan-out %q has more than one join", sourceTarget)
			}
			joinBySource[sourceTarget] = qualified
			joinSpec.Name = qualified
			joinSpec.From = sourceTarget
			allJoins = append(allJoins, joinSpec)
		}

		sortedConts := make([]model.CompiledContinuation[S], len(c.graph.continuations))
		copy(sortedConts, c.graph.continuations)
		slices.SortFunc(sortedConts, func(a, b model.CompiledContinuation[S]) int { return strings.Compare(a.Key, b.Key) })

		for _, cont := range sortedConts {
			qualified := continuationTargets[cont.Key]
			if prev, exists := continuationNames[qualified]; exists {
				return model.Machine[S]{}, fmt.Errorf("flattened continuation name %q collides with %s", qualified, prev)
			}
			continuationNames[qualified] = fmt.Sprintf("continuation %q", cont.Key)
			allContinuations = append(allContinuations, model.CompiledContinuation[S]{
				Key:  qualified,
				Cont: cont.Cont,
			})
		}

		childOutgoing := make(map[string][]string)
		joinOutgoing := make(map[string][]string)
		for _, e := range c.graph.edges {
			if c.graph.isSubgraph(e.from) {
				childOutgoing[e.from] = append(childOutgoing[e.from], e.to)
			} else if c.graph.isJoin(e.from) {
				joinOutgoing[e.from] = append(joinOutgoing[e.from], e.to)
			}
		}

		for m := c.mountStart; m < c.mountStart+c.mountCount; m++ {
			subName := mounts[m].name
			outs := childOutgoing[subName]
			if len(outs) > 1 {
				return model.Machine[S]{}, fmt.Errorf("subgraph %q must have at most one outgoing edge", subName)
			}
			childComp := &arena[mounts[m].idx]
			childComp.mounted = true
			if len(outs) == 1 {
				local := outs[0]
				mapped, exists := targets[local]
				if !exists {
					return model.Machine[S]{}, fmt.Errorf("subgraph %q returns to unknown vertex %q", subName, local)
				}
				childComp.returnTarget = mapped
			} else {
				childComp.returnTarget = ""
			}
		}

		for _, j := range c.graph.joins {
			if len(joinOutgoing[j.Name]) > 1 {
				return model.Machine[S]{}, fmt.Errorf("join %q must have at most one outgoing edge", j.Name)
			}
		}

		for _, e := range c.graph.edges {
			if c.graph.isSubgraph(e.from) {
				continue
			}
			qFrom, ok := targets[e.from]
			if !ok {
				return model.Machine[S]{}, fmt.Errorf("unknown edge source %q in graph %q", e.from, c.prefix)
			}
			qTo, ok := targets[e.to]
			if !ok {
				return model.Machine[S]{}, fmt.Errorf("unknown edge target %q in graph %q", e.to, c.prefix)
			}
			allEdges = append(allEdges, rawEdge{from: qFrom, to: qTo})
		}
	}

	slices.SortFunc(allNodes, func(a, b rawNode) int {
		return strings.Compare(a.spec.Name, b.spec.Name)
	})
	slices.SortFunc(allJoins, func(a, b model.JoinSpec[S]) int {
		return strings.Compare(a.Name, b.Name)
	})
	slices.SortFunc(allContinuations, func(a, b model.CompiledContinuation[S]) int {
		return strings.Compare(a.Key, b.Key)
	})

	nodeIndices := make(map[string]int, len(allNodes))
	for i := range allNodes {
		nodeIndices[allNodes[i].spec.Name] = i
	}
	joinIndices := make(map[string]int, len(allJoins))
	for i := range allJoins {
		joinIndices[allJoins[i].Name] = i
	}

	edgesByFrom := make(map[string][]string)
	for _, e := range allEdges {
		edgesByFrom[e.from] = append(edgesByFrom[e.from], e.to)
	}
	for from := range edgesByFrom {
		targets := edgesByFrom[from]
		slices.Sort(targets)
		edgesByFrom[from] = slices.Compact(targets)
	}

	compiledJoins := make([]model.CompiledJoin[S], len(allJoins))
	for i := range allJoins {
		j := allJoins[i]
		nextTarget := ""
		nextNodeIdx := -1
		nextJoinIdx := -1
		if targets, ok := edgesByFrom[j.Name]; ok && len(targets) > 0 {
			nextTarget = targets[0]
			if idx, exists := nodeIndices[nextTarget]; exists {
				nextNodeIdx = idx
			}
			if idx, exists := joinIndices[nextTarget]; exists {
				nextJoinIdx = idx
			}
		}
		compiledJoins[i] = model.CompiledJoin[S]{
			Spec:          j,
			NextTarget:    nextTarget,
			NextNodeIndex: nextNodeIdx,
			NextJoinIndex: nextJoinIdx,
		}
	}

	compiledNodes := make([]model.CompiledNode[S], len(allNodes))
	var edgeTargets []string
	var compiledEdges []model.CompiledEdge

	for i := range allNodes {
		n := allNodes[i]
		start := len(edgeTargets)
		targets := edgesByFrom[n.spec.Name]
		edgeTargets = append(edgeTargets, targets...)
		for _, target := range targets {
			nIdx := -1
			if idx, exists := nodeIndices[target]; exists {
				nIdx = idx
			}
			jIdx := -1
			if idx, exists := joinIndices[target]; exists {
				jIdx = idx
			}
			compiledEdges = append(compiledEdges, model.CompiledEdge{
				Target:    target,
				NodeIndex: nIdx,
				JoinIndex: jIdx,
			})
		}
		end := len(edgeTargets)

		joinIdx := -1
		if jName, ok := joinBySource[n.spec.Name]; ok {
			if idx, exists := joinIndices[jName]; exists {
				joinIdx = idx
			}
		}

		retNodeIdx := -1
		retJoinIdx := -1
		if n.returnTarget != "" {
			if idx, exists := nodeIndices[n.returnTarget]; exists {
				retNodeIdx = idx
			}
			if idx, exists := joinIndices[n.returnTarget]; exists {
				retJoinIdx = idx
			}
		}

		compiledNodes[i] = model.CompiledNode[S]{
			Spec:            n.spec,
			Index:           i,
			JoinIndex:       joinIdx,
			ReturnTarget:    n.returnTarget,
			ReturnNodeIndex: retNodeIdx,
			ReturnJoinIndex: retJoinIdx,
			Mounted:         n.mounted,
			EdgeStart:       start,
			EdgeEnd:         end,
		}
	}

	entryIdx := -1
	if idx, exists := nodeIndices[rootEntry]; exists {
		entryIdx = idx
	}

	return model.Machine[S]{
		Entry:         rootEntry,
		EntryIndex:    entryIdx,
		Nodes:         compiledNodes,
		EdgeTargets:   edgeTargets,
		Edges:         compiledEdges,
		Joins:         compiledJoins,
		Continuations: allContinuations,
	}, nil
}

func componentEntry[S any](arena []componentNode[S], mounts []childMount, compIdx int) (string, error) {
	curr := compIdx
	for {
		c := &arena[curr]
		if c.graph.isNode(c.graph.entry) {
			return qualify(c.prefix, c.graph.entry), nil
		}
		if c.graph.isJoin(c.graph.entry) {
			return "", fmt.Errorf("entry %q in graph %q is a join", c.graph.entry, c.prefix)
		}
		childComp := -1
		for m := c.mountStart; m < c.mountStart+c.mountCount; m++ {
			if mounts[m].name == c.graph.entry {
				childComp = mounts[m].idx
				break
			}
		}
		if childComp >= 0 {
			curr = childComp
			continue
		}
		return "", fmt.Errorf("entry vertex %q in graph %q is not registered", c.graph.entry, c.prefix)
	}
}

func rewriteNode[S any](run model.Node[S], targets, continuations map[string]string) model.Node[S] {
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
