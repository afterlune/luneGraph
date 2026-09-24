package executor

import "fmt"

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
	}, nil
}

func (r *Runner[S]) checkTargets(from string, targets []string) error {
	if len(targets) == 0 {
		return fmt.Errorf("node %q selected no target", from)
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
