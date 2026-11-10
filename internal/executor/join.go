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
		lg, _, _ := l.group(id, out.ref.slot)
		g := lg.group
		id = g.ParentID
		node = g.JoinNode
		if lg.join != nil {
			scope = lg.join.Spec.OnError
		} else if node != "" {
			if j := r.join(node); j != nil {
				scope = j.Spec.OnError
			}
		}
	} else {
		inv, _, _ := l.invocation(id, out.ref.slot)
		if inv != nil {
			node = inv.Node
		}
		if out.node != nil && out.node.Spec.Name == node {
			scope = out.node.Spec.OnError
		} else if n := r.node(node); n != nil {
			scope = n.Spec.OnError
		}
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
		g, _ := l.getGroup(out.ref.id)
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
			if j := r.join(g.JoinNode); j != nil {
				next = j.NextTarget
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
		} else {
			if _, err := r.applyTransition(&after, &index, id, out.transition, invocationBudget{retained: l.invocationCount(), maximum: maxInvocations}); err != nil {
				return Change[S]{}, nil, err
			}
			if !after.Completed {
				_, inv := indexedInvocation(&index, &after, id)
				if err := clearEntryTimeout(&after, inv); err != nil {
					return Change[S]{}, nil, err
				}
			}
		}
	}
	if terminalErr != nil && after.Failure == nil {
		return Change[S]{}, nil, fmt.Errorf("record failure: %w", terminalErr)
	}
	return l.workspace.change(before, after), terminalErr, nil
}
