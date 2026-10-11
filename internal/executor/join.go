package executor

import (
	"fmt"
)

// resultChange builds an isolated candidate containing only touched topology.
func (r *Runner[S]) resultChange(l *live[S], out workResult[S]) (Change[S], error, error) {
	id := out.ref.id
	scope := FailInvocation
	node := ""
	if out.ref.join {
		g := l.groups[id]
		id = g.ParentID
		node = g.JoinNode
		if node != "" {
			scope = r.joins[node].OnError
		}
	} else {
		inv, _ := l.invocation(id)
		node = inv.Node
		scope = r.nodes[node].OnError
	}
	// Caller applies its per-invocation override before invoking this method.
	if l.failureOverride != nil {
		scope = *l.failureOverride
	}
	before := l.candidate(id, out.err != nil && scope == FailGroup)
	if out.ref.join {
		g := l.groups[out.ref.id]
		seenI := make(map[string]bool)
		seenG := make(map[string]bool)
		for _, v := range before.Invocations {
			seenI[v.ID] = true
		}
		for _, v := range before.Groups {
			seenG[v.ID] = true
		}
		if !seenG[g.ID] {
			before.Groups = append(before.Groups, g)
		}
		for _, child := range g.Children {
			if !seenI[child] {
				inv, _ := l.invocation(child)
				before.Invocations = append(before.Invocations, inv)
			}
		}
	}
	l.beforeInv, l.beforeGroups = before.Invocations, before.Groups
	after := before
	clear(l.afterInv)
	clear(l.afterGroups)
	after.Invocations = append(l.afterInv[:0], before.Invocations...)
	after.Groups = append(l.afterGroups[:0], before.Groups...)
	index := newInvocationIndex(after)
	var terminalErr error
	if out.ref.join {
		g := l.groups[out.ref.id]
		remove := make(map[string]bool, len(g.Children))
		for _, child := range g.Children {
			remove[child] = true
		}
		removeInvocations(&index, &after, remove)
		kept := after.Groups[:0]
		for _, group := range after.Groups {
			if group.ID != g.ID {
				kept = append(kept, group)
			}
		}
		clear(after.Groups[len(kept):])
		after.Groups = kept
		_, parent := indexedInvocation(&index, &after, id)
		parent.ChildGroupID = ""
		if out.err != nil {
			terminalErr = r.recordFailure(&after, &index, id, node, scope, out.err)
		} else if g.JoinNode == "" {
			parent.Status = InvocationEnded
			parent.CallID = ""
		} else {
			parent.State = out.state
			next := ""
			for target := range r.edges[g.JoinNode] {
				next = target
			}
			if next == "" {
				parent.Status = InvocationEnded
				parent.CallID = ""
			} else if err := r.setTarget(&after, parent, next); err != nil {
				return Change[S]{}, nil, &TransitionError{InvocationID: id, Node: node, Cause: err}
			}
		}
	} else {
		after.Steps++
		if out.err != nil {
			terminalErr = r.recordFailure(&after, &index, id, node, scope, out.err)
		} else if _, err := r.applyTransition(&after, &index, id, out.transition); err != nil {
			return Change[S]{}, nil, err
		}
	}
	if terminalErr != nil && after.Failure == nil {
		return Change[S]{}, nil, fmt.Errorf("record failure: %w", terminalErr)
	}
	l.afterInv = after.Invocations
	l.afterGroups = after.Groups
	return l.localChange(before, after), terminalErr, nil
}
