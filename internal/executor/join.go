package executor

import (
	"fmt"
)

// resultChange builds an isolated candidate containing only touched topology.
func (r *Runner[S]) resultChange(l *live[S], out workResult[S], maxInvocations int) (Change[S], error, error) {
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
	joinGroupID := ""
	if out.ref.join {
		joinGroupID = out.ref.id
	}
	before, after := l.workspace.prepare(l, id, out.err != nil && scope == FailGroup, joinGroupID)
	index := newInvocationIndex(after)
	var terminalErr error
	if out.ref.join {
		g := l.groups[out.ref.id]
		remove := make(map[string]bool, len(g.Branches))
		for _, branch := range g.Branches {
			remove[branch.InvocationID] = true
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
		} else if _, err := r.applyTransition(&after, &index, id, out.transition, maxInvocations); err != nil {
			return Change[S]{}, nil, err
		}
	}
	if terminalErr != nil && after.Failure == nil {
		return Change[S]{}, nil, fmt.Errorf("record failure: %w", terminalErr)
	}
	return l.workspace.change(before, after), terminalErr, nil
}
