// Package definition builds immutable graph machines for the executor.
package definition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/afterlune/luneGraph/internal/diagram"
	"github.com/afterlune/luneGraph/internal/executor"
	"github.com/afterlune/luneGraph/internal/model"
)

type subgraphMount[S any] struct {
	name  string
	graph *Graph[S]
}

type edgeSpec struct {
	from string
	to   string
}

// Graph is a mutable builder. Compile copies its definition into a Runner.
type Graph[S any] struct {
	entry         string
	nodes         []model.NodeSpec[S]
	joins         []model.JoinSpec[S]
	subgraphs     []subgraphMount[S]
	edges         []edgeSpec
	continuations []model.CompiledContinuation[S]
}

// New creates a graph builder with the given entry vertex name.
func New[S any](entry string) *Graph[S] {
	return &Graph[S]{entry: entry}
}

func (g *Graph[S]) isNode(name string) bool {
	for i := range g.nodes {
		if g.nodes[i].Name == name {
			return true
		}
	}
	return false
}

func (g *Graph[S]) isJoin(name string) bool {
	for i := range g.joins {
		if g.joins[i].Name == name {
			return true
		}
	}
	return false
}

func (g *Graph[S]) isSubgraph(name string) bool {
	for i := range g.subgraphs {
		if g.subgraphs[i].name == name {
			return true
		}
	}
	return false
}

func (g *Graph[S]) occupied(name string) bool {
	return g.isNode(name) || g.isJoin(name) || g.isSubgraph(name)
}

// AddNode registers a named node.
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
	g.nodes = append(g.nodes, spec)
	return nil
}

// AddJoin registers a join for one fan-out source.
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
	g.joins = append(g.joins, spec)
	return nil
}

// AddSubgraph mounts child at name. The child must use the same state type.
// A mounted graph may have zero or one outgoing edge from its mount vertex;
// that edge is the destination of a Return transition.
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
	g.subgraphs = append(g.subgraphs, subgraphMount[S]{name: name, graph: child})
	return nil
}

// AddEdge declares a legal transition between registered vertices.
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
	for _, edge := range g.edges {
		if edge.from == from && edge.to == to {
			return fmt.Errorf("duplicate edge %q -> %q", from, to)
		}
	}
	g.edges = append(g.edges, edgeSpec{from: from, to: to})
	return nil
}

// RegisterContinuation keeps the external payload as bytes and makes each
// handler's decoded value statically typed. Decode errors leave it waiting.
// Apply receives a CallInfo whose CallID remains stable across recovery replays.
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

// continuationValue preserves nil interface payloads through type erasure.
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

// ExportMermaid renders the builder topology and rejects recursive mounts.
func (g *Graph[S]) ExportMermaid() (string, error) {
	if g == nil {
		return "", errors.New("graph is nil")
	}
	var sb strings.Builder
	sb.WriteString("flowchart TD\n")
	var names diagram.Names

	type mermaidFrame struct {
		g         *Graph[S]
		path      string
		indent    string
		subIdx    int
		subgraphs []subgraphMount[S]
	}

	ancestors := []*Graph[S]{g}
	subList := make([]subgraphMount[S], len(g.subgraphs))
	copy(subList, g.subgraphs)
	slices.SortFunc(subList, func(a, b subgraphMount[S]) int { return strings.Compare(a.name, b.name) })

	stack := []mermaidFrame{
		{g: g, path: "", indent: "    ", subgraphs: subList},
	}

	for len(stack) > 0 {
		frame := &stack[len(stack)-1]
		if frame.subIdx == 0 {
			// Emit entry, nodes, joins for frame.g
			id := func(name string) string { return names.ID(fmt.Sprintf("%s%d:%s", frame.path, len(name), name)) }
			if frame.g.entry != "" {
				fmt.Fprintf(&sb, "%s%s([\"%s (entry)\"])\n", frame.indent, id(frame.g.entry), diagram.Label(frame.g.entry))
			}
			nodes := make([]model.NodeSpec[S], len(frame.g.nodes))
			copy(nodes, frame.g.nodes)
			slices.SortFunc(nodes, func(a, b model.NodeSpec[S]) int { return strings.Compare(a.Name, b.Name) })
			for _, node := range nodes {
				if node.Name != frame.g.entry {
					fmt.Fprintf(&sb, "%s%s[\"%s\"]\n", frame.indent, id(node.Name), diagram.Label(node.Name))
				}
			}
			joins := make([]model.JoinSpec[S], len(frame.g.joins))
			copy(joins, frame.g.joins)
			slices.SortFunc(joins, func(a, b model.JoinSpec[S]) int { return strings.Compare(a.Name, b.Name) })
			for _, join := range joins {
				fmt.Fprintf(&sb, "%s%s{{\"%s (join)\"}}\n", frame.indent, id(join.Name), diagram.Label(join.Name))
			}
		}

		if frame.subIdx < len(frame.subgraphs) {
			sub := frame.subgraphs[frame.subIdx]
			frame.subIdx++
			childPath := fmt.Sprintf("%s%d:%s", frame.path, len(sub.name), sub.name)
			// Cycle check
			for _, anc := range ancestors {
				if anc == sub.graph {
					return "", fmt.Errorf("recursive subgraph reference at %q", childPath)
				}
			}
			id := names.ID(childPath)
			fmt.Fprintf(&sb, "%ssubgraph %s [\"%s\"]\n", frame.indent, id, diagram.Label(sub.name))
			ancestors = append(ancestors, sub.graph)
			childSubList := make([]subgraphMount[S], len(sub.graph.subgraphs))
			copy(childSubList, sub.graph.subgraphs)
			slices.SortFunc(childSubList, func(a, b subgraphMount[S]) int { return strings.Compare(a.name, b.name) })
			stack = append(stack, mermaidFrame{
				g:         sub.graph,
				path:      childPath,
				indent:    frame.indent + "    ",
				subgraphs: childSubList,
			})
			continue
		}

		// Finished all subgraphs of frame.g, now emit edges
		id := func(name string) string { return names.ID(fmt.Sprintf("%s%d:%s", frame.path, len(name), name)) }
		edges := make([]edgeSpec, len(frame.g.edges))
		copy(edges, frame.g.edges)
		slices.SortFunc(edges, func(a, b edgeSpec) int {
			if c := strings.Compare(a.from, b.from); c != 0 {
				return c
			}
			return strings.Compare(a.to, b.to)
		})
		for _, e := range edges {
			fmt.Fprintf(&sb, "%s%s --> %s\n", frame.indent, id(e.from), id(e.to))
		}

		if len(stack) > 1 {
			// Close subgraph block
			indent := frame.indent[:len(frame.indent)-4]
			fmt.Fprintf(&sb, "%send\n", indent)
		}

		ancestors = ancestors[:len(ancestors)-1]
		stack = stack[:len(stack)-1]
	}

	return sb.String(), nil
}
