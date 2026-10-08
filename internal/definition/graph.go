// Package definition builds immutable graph machines for the executor.
package definition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/afterlune/luneGraph/internal/executor"
	"github.com/afterlune/luneGraph/internal/model"
)

// Graph is a mutable builder. Compile copies its definition into a Runner.
type Graph[S any] struct {
	entry         string
	nodes         map[string]model.NodeSpec[S]
	joins         map[string]model.JoinSpec[S]
	subgraphs     map[string]*Graph[S]
	edges         map[string]map[string]struct{}
	continuations map[string]model.Continuation[S]
}

// New creates a graph builder with the given entry vertex name.
func New[S any](entry string) *Graph[S] {
	return &Graph[S]{entry: entry, nodes: make(map[string]model.NodeSpec[S]), joins: make(map[string]model.JoinSpec[S]), subgraphs: make(map[string]*Graph[S]), edges: make(map[string]map[string]struct{}), continuations: make(map[string]model.Continuation[S])}
}

func (g *Graph[S]) occupied(name string) bool {
	_, node := g.nodes[name]
	_, join := g.joins[name]
	_, subgraph := g.subgraphs[name]
	return node || join || subgraph
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
	if g.subgraphs == nil {
		g.subgraphs = make(map[string]*Graph[S])
	}
	g.subgraphs[name] = child
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
// Apply receives a stable CallInfo that remains the same if recovery replays
// the application.
func RegisterContinuation[S, P any](g *Graph[S], key string, decode func([]byte) (P, error), apply func(context.Context, model.CallInfo, S, P) (S, error)) error {
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
		Apply: func(ctx context.Context, call model.CallInfo, state S, value any) (S, error) {
			return apply(ctx, call, state, value.(P))
		},
	}
	return nil
}

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
	definition.ID = config.MachineID
	definition.Clone = config.Clone
	return executor.New(definition)
}

// ExportMermaid generates a Mermaid flowchart representation of the graph definition.
func (g *Graph[S]) ExportMermaid() string {
	if g == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("flowchart TD\n")
	renderGraphMermaid(&sb, g, "    ")
	return sb.String()
}

func mermaidID(name string) string {
	return strings.ReplaceAll(strings.ReplaceAll(name, "/", "_"), "-", "_")
}

func renderGraphMermaid[S any](sb *strings.Builder, g *Graph[S], indent string) {
	if g.entry != "" {
		fmt.Fprintf(sb, "%s%s([\"%s (entry)\"])\n", indent, mermaidID(g.entry), g.entry)
	}
	for _, name := range sortedKeys(g.nodes) {
		if name == g.entry {
			continue
		}
		fmt.Fprintf(sb, "%s%s[\"%s\"]\n", indent, mermaidID(name), name)
	}
	for _, name := range sortedKeys(g.joins) {
		fmt.Fprintf(sb, "%s%s{{\"%s (join)\"}}\n", indent, mermaidID(name), name)
	}
	for _, name := range sortedKeys(g.subgraphs) {
		sub := g.subgraphs[name]
		fmt.Fprintf(sb, "%ssubgraph %s [\"%s\"]\n", indent, mermaidID(name), name)
		renderGraphMermaid(sb, sub, indent+"    ")
		fmt.Fprintf(sb, "%send\n", indent)
	}
	for _, from := range sortedKeys(g.edges) {
		for _, to := range sortedKeys(g.edges[from]) {
			fmt.Fprintf(sb, "%s%s --> %s\n", indent, mermaidID(from), mermaidID(to))
		}
	}
}
