// Package definition builds immutable graph machines for the executor.
package definition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/afterlune/luneGraph/internal/diagram"
	"github.com/afterlune/luneGraph/internal/executor"
	"github.com/afterlune/luneGraph/internal/model"
)

var nextGraphID atomic.Uint64

// VertexKind identifies whether a vertex in the flat vector is a Node or a Join.
type VertexKind uint8

const (
	VertexNode VertexKind = iota
	VertexJoin
)

// Vertex stores one node or join in the flat vector.
type Vertex[S any] struct {
	Kind         VertexKind
	Name         string
	Node         model.NodeSpec[S]
	Join         model.JoinSpec[S]
	Scope        string
	ReturnTarget string
	Mounted      bool
}

// Edge represents a directed transition between two vertices by integer index.
type Edge struct {
	From int
	To   int
}

// MountInfo tracks the index range and entry of a flattened subgraph.
type MountInfo struct {
	Name         string
	EntryIdx     int
	ChildStart   int
	ChildEnd     int
	ReturnTarget string
}

type edgeSpec struct {
	from string
	to   string
}

// Graph is a mutable builder backed by a single flat vector of vertices and index-based edges.
type Graph[S any] struct {
	id                 uint64
	entry              string
	entryIdx           int
	vertices           []Vertex[S]
	edges              []Edge
	mountEdges         []edgeSpec
	continuations      []model.CompiledContinuation[S]
	mounts             []MountInfo
	ancestorIDs        []uint64
	hasRecursiveMount  bool
	recursiveMountName string
}

// New creates a graph builder with the given entry vertex name.
func New[S any](entry string) *Graph[S] {
	return &Graph[S]{
		id:       nextGraphID.Add(1),
		entry:    entry,
		entryIdx: -1,
	}
}

func (g *Graph[S]) hasAncestor(id uint64) bool {
	return slices.Contains(g.ancestorIDs, id)
}

func (g *Graph[S]) addAncestor(id uint64) {
	if !g.hasAncestor(id) {
		g.ancestorIDs = append(g.ancestorIDs, id)
	}
}

func (g *Graph[S]) entryVertexIdx() int {
	if g == nil || g.entry == "" {
		return -1
	}
	for i := range g.vertices {
		if g.vertices[i].Name == g.entry {
			return i
		}
	}
	for _, m := range g.mounts {
		if m.Name == g.entry {
			return m.EntryIdx
		}
	}
	return -1
}

func (g *Graph[S]) isNode(name string) bool {
	for i := range g.vertices {
		if g.vertices[i].Name == name && g.vertices[i].Kind == VertexNode {
			return true
		}
	}
	return false
}

func (g *Graph[S]) isJoin(name string) bool {
	for i := range g.vertices {
		if g.vertices[i].Name == name && g.vertices[i].Kind == VertexJoin {
			return true
		}
	}
	return false
}

func (g *Graph[S]) isSubgraph(name string) bool {
	for i := range g.mounts {
		if g.mounts[i].Name == name {
			return true
		}
	}
	return false
}

func (g *Graph[S]) isRootNode(name string) bool {
	for i := range g.vertices {
		if g.vertices[i].Name == name && !g.vertices[i].Mounted && g.vertices[i].Kind == VertexNode {
			return true
		}
	}
	return false
}

func (g *Graph[S]) isRootJoin(name string) bool {
	for i := range g.vertices {
		if g.vertices[i].Name == name && !g.vertices[i].Mounted && g.vertices[i].Kind == VertexJoin {
			return true
		}
	}
	return false
}

func (g *Graph[S]) occupied(name string) bool {
	return g.isRootNode(name) || g.isRootJoin(name) || g.isSubgraph(name)
}

func (g *Graph[S]) findVertexIdx(name string) int {
	for i := range g.vertices {
		if g.vertices[i].Name == name {
			return i
		}
	}
	return -1
}

func (g *Graph[S]) findMountIdx(name string) int {
	for i := range g.mounts {
		if g.mounts[i].Name == name {
			return i
		}
	}
	return -1
}

// AddNode registers a named node into the flat vertex vector.
func (g *Graph[S]) AddNode(spec model.NodeSpec[S]) error {
	if g == nil {
		return errors.New("graph is nil")
	}
	if !model.ValidName(spec.Name) {
		return errors.New("node name must be non-empty and have no surrounding whitespace")
	}
	if spec.Run == nil {
		return fmt.Errorf("node %q has no function", spec.Name)
	}
	if !model.ValidScope(spec.OnError) {
		return fmt.Errorf("node %q has invalid failure scope", spec.Name)
	}
	if spec.Timeout < 0 {
		return fmt.Errorf("node %q timeout must not be negative", spec.Name)
	}
	if g.occupied(spec.Name) {
		return fmt.Errorf("duplicate vertex %q", spec.Name)
	}
	idx := len(g.vertices)
	if spec.Name == g.entry {
		g.entryIdx = idx
	}
	g.vertices = append(g.vertices, Vertex[S]{
		Kind: VertexNode,
		Name: spec.Name,
		Node: spec,
	})
	return nil
}

// AddJoin registers a join into the flat vertex vector for one fan-out source.
func (g *Graph[S]) AddJoin(spec model.JoinSpec[S]) error {
	if g == nil {
		return errors.New("graph is nil")
	}
	if !model.ValidName(spec.Name) || !model.ValidName(spec.From) {
		return errors.New("join name and source must be non-empty and have no surrounding whitespace")
	}
	if spec.Merge == nil {
		return fmt.Errorf("join %q has no merge function", spec.Name)
	}
	if !model.ValidScope(spec.OnError) {
		return fmt.Errorf("join %q has invalid failure scope", spec.Name)
	}
	if spec.Timeout < 0 {
		return fmt.Errorf("join %q timeout must not be negative", spec.Name)
	}
	if spec.Quorum < 0 {
		return fmt.Errorf("join %q quorum must not be negative", spec.Name)
	}
	if g.occupied(spec.Name) {
		return fmt.Errorf("duplicate vertex %q", spec.Name)
	}
	idx := len(g.vertices)
	if spec.Name == g.entry {
		g.entryIdx = idx
	}
	g.vertices = append(g.vertices, Vertex[S]{
		Kind: VertexJoin,
		Name: spec.Name,
		Join: spec,
	})
	return nil
}

// AddSubgraph flattens child into the parent graph's single vertex vector with prefix names.
func (g *Graph[S]) AddSubgraph(name string, child *Graph[S]) error {
	if g == nil {
		return errors.New("graph is nil")
	}
	if !model.ValidName(name) {
		return errors.New("subgraph name must be non-empty and have no surrounding whitespace")
	}
	if child == nil {
		return fmt.Errorf("subgraph %q is nil", name)
	}
	if g.occupied(name) {
		return fmt.Errorf("duplicate vertex %q", name)
	}

	// Recursive subgraph cycle detection
	if child == g || g.hasAncestor(child.id) || child.hasRecursiveMount {
		g.hasRecursiveMount = true
		g.recursiveMountName = name
		child.hasRecursiveMount = true
		child.recursiveMountName = name
		return nil
	}
	child.addAncestor(g.id)
	for _, anc := range g.ancestorIDs {
		child.addAncestor(anc)
	}

	baseIdx := len(g.vertices)
	origEntryIdx := child.entryVertexIdx()
	childEntryIdx := -1
	if origEntryIdx >= 0 {
		childEntryIdx = baseIdx + origEntryIdx
	}

	for _, v := range child.vertices {
		qName := qualify(name, v.Name)
		newV := v
		newV.Name = qName
		newV.Mounted = true
		if newV.Scope == "" {
			newV.Scope = name
		} else {
			newV.Scope = qualify(name, newV.Scope)
		}
		if newV.ReturnTarget != "" {
			newV.ReturnTarget = qualify(name, newV.ReturnTarget)
		}
		if newV.Kind == VertexJoin {
			if newV.Join.From != "" {
				newV.Join.From = qualify(name, newV.Join.From)
			}
			newV.Join.Name = qName
		}
		g.vertices = append(g.vertices, newV)
	}

	for _, e := range child.edges {
		g.edges = append(g.edges, Edge{
			From: baseIdx + e.From,
			To:   baseIdx + e.To,
		})
	}

	for _, c := range child.continuations {
		qKey := qualify(name, c.Key)
		newC := c
		newC.Key = qKey
		g.continuations = append(g.continuations, newC)
	}

	for _, m := range child.mounts {
		retTarget := m.ReturnTarget
		if retTarget != "" {
			retTarget = qualify(name, retTarget)
		}
		childMountEntry := -1
		if m.EntryIdx >= 0 {
			childMountEntry = baseIdx + m.EntryIdx
		}
		g.mounts = append(g.mounts, MountInfo{
			Name:         qualify(name, m.Name),
			EntryIdx:     childMountEntry,
			ChildStart:   baseIdx + m.ChildStart,
			ChildEnd:     baseIdx + m.ChildEnd,
			ReturnTarget: retTarget,
		})
	}

	g.mounts = append(g.mounts, MountInfo{
		Name:       name,
		EntryIdx:   childEntryIdx,
		ChildStart: baseIdx,
		ChildEnd:   len(g.vertices),
	})

	if name == g.entry {
		g.entryIdx = childEntryIdx
	}
	return nil
}

// AddEdge declares a legal transition using integer indices into the vertex vector.
func (g *Graph[S]) AddEdge(from, to string) error {
	if g == nil {
		return errors.New("graph is nil")
	}
	if !g.occupied(from) {
		return fmt.Errorf("unknown source %q", from)
	}
	if !g.occupied(to) {
		return fmt.Errorf("unknown target %q", to)
	}

	for _, me := range g.mountEdges {
		if me.from == from && me.to == to {
			return fmt.Errorf("duplicate edge %q -> %q", from, to)
		}
	}

	if g.isSubgraph(from) {
		g.mountEdges = append(g.mountEdges, edgeSpec{from: from, to: to})
		resolvedTo := to
		if toMountIdx := g.findMountIdx(to); toMountIdx >= 0 {
			entryIdx := g.mounts[toMountIdx].EntryIdx
			if entryIdx >= 0 && entryIdx < len(g.vertices) {
				resolvedTo = g.vertices[entryIdx].Name
			}
		}
		for i := range g.mounts {
			if g.mounts[i].Name == from {
				g.mounts[i].ReturnTarget = resolvedTo
				for v := g.mounts[i].ChildStart; v < g.mounts[i].ChildEnd; v++ {
					if g.vertices[v].ReturnTarget == "" {
						g.vertices[v].ReturnTarget = resolvedTo
					}
				}
				break
			}
		}
		return nil
	}

	fromIdx := g.findVertexIdx(from)
	toIdx := g.findVertexIdx(to)

	if toIdx < 0 && g.isSubgraph(to) {
		for _, m := range g.mounts {
			if m.Name == to {
				toIdx = m.EntryIdx
				break
			}
		}
	}

	if fromIdx < 0 {
		return fmt.Errorf("unknown source %q", from)
	}
	if toIdx < 0 {
		return fmt.Errorf("unknown target %q", to)
	}

	for _, e := range g.edges {
		if e.From == fromIdx && e.To == toIdx {
			return fmt.Errorf("duplicate edge %q -> %q", from, to)
		}
	}
	g.edges = append(g.edges, Edge{From: fromIdx, To: toIdx})
	return nil
}

// RegisterContinuation keeps the external payload as bytes and makes each
// handler's decoded value statically typed.
func RegisterContinuation[S, P any](g *Graph[S], key string, decode func([]byte) (P, error), apply func(context.Context, model.CallInfo, S, P) (S, error)) error {
	if g == nil {
		return errors.New("graph is nil")
	}
	if !model.ValidName(key) || decode == nil || apply == nil {
		return errors.New("continuation needs a valid key, decoder and handler")
	}
	for _, c := range g.continuations {
		if c.Key == key {
			return fmt.Errorf("duplicate continuation %q", key)
		}
	}
	g.continuations = append(g.continuations, model.CompiledContinuation[S]{
		Key: key,
		Cont: model.Continuation[S]{
			Decode: func(payload []byte) (any, error) {
				value, err := decode(payload)
				return continuationValue[P]{value: value}, err
			},
			Apply: func(ctx context.Context, call model.CallInfo, state S, value any) (S, error) {
				return apply(ctx, call, state, value.(continuationValue[P]).value)
			},
		},
	})
	return nil
}

type continuationValue[P any] struct{ value P }

// RegisterJSONContinuation registers a continuation whose payload is JSON-decoded into type P.
func RegisterJSONContinuation[S, P any](g *Graph[S], key string, apply func(context.Context, model.CallInfo, S, P) (S, error)) error {
	return RegisterContinuation(g, key, func(payload []byte) (P, error) {
		var value P
		err := json.Unmarshal(payload, &value)
		return value, err
	}, apply)
}

// Compile validates and copies the builder into a concurrent-safe Runner.
func (g *Graph[S]) Compile(config model.Config[S]) (*executor.Runner[S], error) {
	if g == nil {
		return nil, errors.New("graph is nil")
	}
	if !model.ValidName(config.MachineID) {
		return nil, errors.New("machine ID must be non-empty and have no surrounding whitespace")
	}
	if config.Clone == nil {
		return nil, errors.New("Clone is required")
	}
	definition, err := expand(g)
	if err != nil {
		return nil, err
	}
	for i := range definition.Joins {
		join := definition.Joins[i].Spec
		if join.Quorum > 0 && join.CanMerge != nil {
			return nil, fmt.Errorf("join %q cannot configure both quorum and can-merge", join.Name)
		}
	}
	definition.ID = config.MachineID
	definition.Clone = config.Clone
	return executor.New(definition)
}

// ExportMermaid renders the builder topology directly from flat vectors.
func (g *Graph[S]) ExportMermaid() (string, error) {
	if g == nil {
		return "", errors.New("graph is nil")
	}
	if g.hasRecursiveMount {
		return "", fmt.Errorf("recursive subgraph reference at %q", g.recursiveMountName)
	}

	var sb strings.Builder
	sb.WriteString("flowchart TD\n")
	var names diagram.Names

	id := func(name string) string { return names.ID(name) }

	// Collect entry names
	entryNames := make(map[string]bool)
	entryName := g.entry
	if g.entryIdx >= 0 && g.entryIdx < len(g.vertices) {
		entryName = g.vertices[g.entryIdx].Name
	}
	if entryName != "" {
		entryNames[entryName] = true
	}
	for _, m := range g.mounts {
		if m.EntryIdx >= 0 && m.EntryIdx < len(g.vertices) {
			entryNames[g.vertices[m.EntryIdx].Name] = true
		}
	}

	// Group vertices by mount scope
	rootVertices := make([]Vertex[S], 0, len(g.vertices))
	mountMap := make(map[string][]Vertex[S])

	for _, v := range g.vertices {
		if v.Scope == "" {
			rootVertices = append(rootVertices, v)
		} else {
			mountMap[v.Scope] = append(mountMap[v.Scope], v)
		}
	}

	slices.SortFunc(rootVertices, func(a, b Vertex[S]) int {
		return strings.Compare(a.Name, b.Name)
	})

	// Emit root entry first
	if entryName != "" {
		fmt.Fprintf(&sb, "    %s([\"%s (entry)\"])\n", id(entryName), diagram.Label(entryName))
	}

	// Emit other root vertices
	for _, v := range rootVertices {
		if v.Name == entryName {
			continue
		}
		if v.Kind == VertexNode {
			fmt.Fprintf(&sb, "    %s[\"%s\"]\n", id(v.Name), diagram.Label(v.Name))
		} else {
			fmt.Fprintf(&sb, "    %s{{\"%s (join)\"}}\n", id(v.Name), diagram.Label(v.Name))
		}
	}

	// Sort mount scopes deterministically
	mountScopes := make([]string, 0, len(mountMap))
	for scope := range mountMap {
		mountScopes = append(mountScopes, scope)
	}
	slices.Sort(mountScopes)

	for _, scope := range mountScopes {
		subID := id("subgraph:" + scope)
		fmt.Fprintf(&sb, "    subgraph %s [\"%s\"]\n", subID, diagram.Label(scope))
		verts := mountMap[scope]
		slices.SortFunc(verts, func(a, b Vertex[S]) int {
			return strings.Compare(a.Name, b.Name)
		})
		for _, v := range verts {
			labelName := v.Name
			if v.Scope != "" {
				labelName = strings.TrimPrefix(v.Name, v.Scope+"/")
			}
			if entryNames[v.Name] {
				fmt.Fprintf(&sb, "        %s([\"%s (entry)\"])\n", id(v.Name), diagram.Label(labelName))
			} else if v.Kind == VertexNode {
				fmt.Fprintf(&sb, "        %s[\"%s\"]\n", id(v.Name), diagram.Label(labelName))
			} else {
				fmt.Fprintf(&sb, "        %s{{\"%s (join)\"}}\n", id(v.Name), diagram.Label(labelName))
			}
		}
		fmt.Fprintf(&sb, "    end\n")
	}

	// Emit edges
	type edgeDisplay struct {
		from string
		to   string
	}
	displayEdges := make([]edgeDisplay, 0, len(g.edges))
	for _, e := range g.edges {
		displayEdges = append(displayEdges, edgeDisplay{
			from: g.vertices[e.From].Name,
			to:   g.vertices[e.To].Name,
		})
	}
	slices.SortFunc(displayEdges, func(a, b edgeDisplay) int {
		if c := strings.Compare(a.from, b.from); c != 0 {
			return c
		}
		return strings.Compare(a.to, b.to)
	})

	for _, e := range displayEdges {
		fmt.Fprintf(&sb, "    %s --> %s\n", id(e.from), id(e.to))
	}

	return sb.String(), nil
}
