package definition

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/afterlune/luneGraph/internal/model"
)

func expand[S any](g *Graph[S]) (model.Machine[S], error) {
	if g == nil {
		return model.Machine[S]{}, fmt.Errorf("subgraph at %q is nil", "")
	}
	if g.hasRecursiveMount {
		return model.Machine[S]{}, fmt.Errorf("recursive subgraph reference at %q", g.recursiveMountName)
	}

	for _, m := range g.mounts {
		if m.EntryIdx < 0 || m.ChildStart < 0 || m.ChildStart > m.ChildEnd {
			return model.Machine[S]{}, fmt.Errorf("subgraph %q is nil", m.Name)
		}
	}

	entryIdx := g.entryVertexIdx()
	if entryIdx < 0 || entryIdx >= len(g.vertices) {
		return model.Machine[S]{}, fmt.Errorf("entry vertex %q in graph %q is not registered", g.entry, "")
	}
	if g.vertices[entryIdx].Kind == VertexJoin {
		return model.Machine[S]{}, fmt.Errorf("entry %q in graph %q is a join", g.entry, "")
	}
	rootEntry := g.vertices[entryIdx].Name

	// Validate mount edges
	mountOutgoing := make(map[string][]string)
	for _, me := range g.mountEdges {
		mountOutgoing[me.from] = append(mountOutgoing[me.from], me.to)
		if len(mountOutgoing[me.from]) > 1 {
			return model.Machine[S]{}, fmt.Errorf("mounted graph %q may have at most one outgoing edge", me.from)
		}
	}

	// Validate edges
	for _, e := range g.edges {
		if e.From < 0 || e.From >= len(g.vertices) {
			return model.Machine[S]{}, fmt.Errorf("unknown edge source in graph %q", "")
		}
		if e.To < 0 || e.To >= len(g.vertices) {
			return model.Machine[S]{}, fmt.Errorf("unknown edge target in graph %q", "")
		}
	}

	// Validate joins and count outgoing edges
	joinBySource := make(map[string]string)
	joinOutgoing := make(map[int]int)
	for _, e := range g.edges {
		if g.vertices[e.From].Kind == VertexJoin {
			joinOutgoing[e.From]++
			if joinOutgoing[e.From] > 1 {
				return model.Machine[S]{}, fmt.Errorf("join %q must have at most one outgoing edge", g.vertices[e.From].Name)
			}
		}
	}

	// Vertex name collision check
	vertexNames := make(map[string]string)
	for _, v := range g.vertices {
		if prev, exists := vertexNames[v.Name]; exists {
			return model.Machine[S]{}, fmt.Errorf("flattened vertex name %q collides between %s and vertex %q", v.Name, prev, v.Name)
		}
		vertexNames[v.Name] = v.Name
	}

	for _, v := range g.vertices {
		if v.Kind == VertexJoin {
			fromIdx := g.findVertexIdx(v.Join.From)
			if fromIdx < 0 || g.vertices[fromIdx].Kind != VertexNode {
				return model.Machine[S]{}, fmt.Errorf("join %q has unknown fan-out source %q", v.Join.Name, v.Join.From)
			}
			if prev, exists := joinBySource[v.Join.From]; exists {
				return model.Machine[S]{}, fmt.Errorf("fan-out %q has more than one join (%s, %s)", v.Join.From, prev, v.Join.Name)
			}
			joinBySource[v.Join.From] = v.Join.Name
		}
	}

	// Continuation name collision check
	continuationNames := make(map[string]string)
	for _, c := range g.continuations {
		if prev, exists := continuationNames[c.Key]; exists {
			return model.Machine[S]{}, fmt.Errorf("flattened continuation name %q collides with %s", c.Key, prev)
		}
		continuationNames[c.Key] = fmt.Sprintf("continuation %q", c.Key)
	}

	// Build target maps for scopes
	type scopeTargets struct {
		targets       map[string]string
		continuations map[string]string
	}
	scopeMaps := make(map[string]*scopeTargets)
	getScope := func(scope string) *scopeTargets {
		st, ok := scopeMaps[scope]
		if !ok {
			st = &scopeTargets{
				targets:       make(map[string]string),
				continuations: make(map[string]string),
			}
			scopeMaps[scope] = st
		}
		return st
	}

	for _, v := range g.vertices {
		st := getScope(v.Scope)
		localName := v.Name
		if v.Scope != "" {
			localName = strings.TrimPrefix(v.Name, v.Scope+"/")
		}
		st.targets[localName] = v.Name
	}

	for _, m := range g.mounts {
		if m.EntryIdx >= 0 && m.EntryIdx < len(g.vertices) {
			entryName := g.vertices[m.EntryIdx].Name
			parentScope := ""
			localMountName := m.Name
			if idx := strings.LastIndex(m.Name, "/"); idx >= 0 {
				parentScope = m.Name[:idx]
				localMountName = m.Name[idx+1:]
			}
			st := getScope(parentScope)
			st.targets[localMountName] = entryName
		}
	}

	for _, c := range g.continuations {
		for scope, st := range scopeMaps {
			if scope == "" {
				if !strings.Contains(c.Key, "/") {
					st.continuations[c.Key] = c.Key
				}
			} else if strings.HasPrefix(c.Key, scope+"/") {
				localKey := strings.TrimPrefix(c.Key, scope+"/")
				st.continuations[localKey] = c.Key
			}
		}
	}

	type rawNode struct {
		spec         model.NodeSpec[S]
		returnTarget string
		mounted      bool
	}
	var allNodes []rawNode
	var allJoins []model.JoinSpec[S]

	for _, v := range g.vertices {
		if v.Kind == VertexNode {
			run := v.Node.Run
			if st, ok := scopeMaps[v.Scope]; ok && (len(st.targets) > 0 || len(st.continuations) > 0) {
				run = rewriteNode(v.Node.Run, st.targets, st.continuations)
			}
			allNodes = append(allNodes, rawNode{
				spec: model.NodeSpec[S]{
					Name:    v.Name,
					Run:     run,
					OnError: v.Node.OnError,
					Timeout: v.Node.Timeout,
				},
				returnTarget: v.ReturnTarget,
				mounted:      v.Mounted,
			})
		} else {
			allJoins = append(allJoins, v.Join)
		}
	}

	slices.SortFunc(allNodes, func(a, b rawNode) int {
		return strings.Compare(a.spec.Name, b.spec.Name)
	})
	slices.SortFunc(allJoins, func(a, b model.JoinSpec[S]) int {
		return strings.Compare(a.Name, b.Name)
	})

	allContinuations := make([]model.CompiledContinuation[S], len(g.continuations))
	copy(allContinuations, g.continuations)
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
	for _, e := range g.edges {
		fromName := g.vertices[e.From].Name
		toName := g.vertices[e.To].Name
		edgesByFrom[fromName] = append(edgesByFrom[fromName], toName)
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
		edgeStart := len(compiledEdges)
		targets := edgesByFrom[n.spec.Name]
		for _, target := range targets {
			edgeTargets = append(edgeTargets, target)
			nodeIdx := -1
			joinIdx := -1
			if idx, exists := nodeIndices[target]; exists {
				nodeIdx = idx
			}
			if idx, exists := joinIndices[target]; exists {
				joinIdx = idx
			}
			compiledEdges = append(compiledEdges, model.CompiledEdge{
				Target:    target,
				NodeIndex: nodeIdx,
				JoinIndex: joinIdx,
			})
		}
		edgeEnd := len(compiledEdges)

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
			EdgeStart:       edgeStart,
			EdgeEnd:         edgeEnd,
		}
	}

	entryIdx = -1
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
