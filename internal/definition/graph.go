// Package definition builds immutable graph machines for the executor.
package definition

import (
	"context"
	"errors"
	"fmt"

	"lune-graph/internal/executor"
	"lune-graph/internal/model"
)

// Graph is a mutable builder. Compile copies its definition into a Runner.
type Graph[S any] struct {
	entry         string
	nodes         map[string]model.NodeSpec[S]
	joins         map[string]model.JoinSpec[S]
	edges         map[string]map[string]struct{}
	continuations map[string]model.Continuation[S]
}

// New creates a graph builder with the given entry node name.
func New[S any](entry string) *Graph[S] {
	return &Graph[S]{entry: entry, nodes: make(map[string]model.NodeSpec[S]), joins: make(map[string]model.JoinSpec[S]), edges: make(map[string]map[string]struct{}), continuations: make(map[string]model.Continuation[S])}
}

func (g *Graph[S]) occupied(name string) bool {
	_, node := g.nodes[name]
	_, join := g.joins[name]
	return node || join
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
	if g.occupied(spec.Name) {
		return fmt.Errorf("duplicate vertex %q", spec.Name)
	}
	if g.nodes == nil {
		g.nodes = make(map[string]model.NodeSpec[S])
	}
	g.nodes[spec.Name] = spec
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
	if g.occupied(spec.Name) {
		return fmt.Errorf("duplicate vertex %q", spec.Name)
	}
	if g.joins == nil {
		g.joins = make(map[string]model.JoinSpec[S])
	}
	g.joins[spec.Name] = spec
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
	if g.edges == nil {
		g.edges = make(map[string]map[string]struct{})
	}
	if g.edges[from] == nil {
		g.edges[from] = make(map[string]struct{})
	}
	if _, exists := g.edges[from][to]; exists {
		return fmt.Errorf("duplicate edge %q -> %q", from, to)
	}
	g.edges[from][to] = struct{}{}
	return nil
}

// RegisterContinuation keeps the external payload as bytes and makes each
// handler's decoded value statically typed. Decode errors leave it waiting.
func RegisterContinuation[S, P any](g *Graph[S], key string, decode func([]byte) (P, error), apply func(context.Context, S, P) (S, error)) error {
	if g == nil {
		return errors.New("graph is nil")
	}
	if !model.ValidName(key) || decode == nil || apply == nil {
		return errors.New("continuation needs a valid key, decoder and handler")
	}
	if g.continuations == nil {
		g.continuations = make(map[string]model.Continuation[S])
	}
	if _, exists := g.continuations[key]; exists {
		return fmt.Errorf("duplicate continuation %q", key)
	}
	g.continuations[key] = model.Continuation[S]{
		Decode: func(payload []byte) (any, error) { return decode(payload) },
		Apply:  func(ctx context.Context, state S, value any) (S, error) { return apply(ctx, state, value.(P)) },
	}
	return nil
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
	if _, exists := g.nodes[g.entry]; !exists {
		return nil, fmt.Errorf("entry node %q is not registered", g.entry)
	}

	nodes := make(map[string]model.NodeSpec[S], len(g.nodes))
	for name, spec := range g.nodes {
		nodes[name] = spec
	}
	joins := make(map[string]model.JoinSpec[S], len(g.joins))
	joinBySource := make(map[string]string, len(g.joins))
	for name, spec := range g.joins {
		if _, exists := g.nodes[spec.From]; !exists {
			return nil, fmt.Errorf("join %q has unknown fan-out source %q", name, spec.From)
		}
		if other := joinBySource[spec.From]; other != "" {
			return nil, fmt.Errorf("fan-out %q has joins %q and %q", spec.From, other, name)
		}
		if len(g.edges[name]) > 1 {
			return nil, fmt.Errorf("join %q must have at most one outgoing edge", name)
		}
		joins[name] = spec
		joinBySource[spec.From] = name
	}
	edges := make(map[string]map[string]struct{}, len(g.edges))
	for from, targets := range g.edges {
		edges[from] = make(map[string]struct{}, len(targets))
		for to := range targets {
			edges[from][to] = struct{}{}
		}
	}
	continuations := make(map[string]model.Continuation[S], len(g.continuations))
	for key, continuation := range g.continuations {
		continuations[key] = continuation
	}

	return executor.New(model.Machine[S]{
		ID:            config.MachineID,
		Entry:         g.entry,
		Clone:         config.Clone,
		Nodes:         nodes,
		Joins:         joins,
		JoinBySource:  joinBySource,
		Edges:         edges,
		Continuations: continuations,
	})
}
