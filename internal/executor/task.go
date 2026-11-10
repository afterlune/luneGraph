package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/afterlune/luneGraph/internal/observation"
)

type workResult[S any] struct {
	ref        taskRef
	callID     string
	node       *CompiledNode[S]
	transition Transition[S]
	state      S
	err        error
}
type workerTask[S any] struct {
	ctx          context.Context
	ref          taskRef
	call         CallInfo
	node         *CompiledNode[S]
	spec         NodeSpec[S]
	join         JoinSpec[S]
	timeout      time.Duration
	state        S
	branches     []BranchResult[S]
	ready        bool
	preflightErr error
	obs          *observation.Session
	revision     uint64
}

func (r *Runner[S]) executeTask(t workerTask[S]) workResult[S] {
	out := workResult[S]{ref: t.ref, callID: t.call.CallID, node: t.node}
	if t.ref.join {
		if t.join.Merge != nil {
			out.state, out.err = r.observedMerge(t.ctx, t.obs, t.revision, t.call, t.join, t.state, t.branches)
		}
	} else if t.obs == nil {
		out.transition, out.err = r.runNode(t.ctx, t.call, t.spec, t.state)
	} else {
		out.transition, out.err = r.observedNode(t.ctx, t.obs, t.revision, t.call, t.spec, t.state)
	}
	return out
}

func (r *Runner[S]) prepareTask(ctx context.Context, l *live[S], ref taskRef, obs *observation.Session) (workerTask[S], error) {
	t := workerTask[S]{ctx: ctx, ref: ref, obs: obs, revision: l.header.Revision, ready: true}
	if ref.join {
		lg, _, ok := l.group(ref.id, ref.slot)
		if !ok {
			return t, invalidCheckpoint("join group %q missing", ref.id)
		}
		g := lg.group
		parent, _, exists := l.invocation(g.ParentID, -1)
		if !exists {
			return t, invalidCheckpoint("join group %q parent %q missing", ref.id, g.ParentID)
		}
		if lg.join != nil {
			t.join = lg.join.Spec
		} else if j := r.join(g.JoinNode); j != nil {
			t.join = j.Spec
		}
		t.timeout = t.join.Timeout
		t.call = CallInfo{RunID: l.header.RunID, InvocationID: parent.ID, CallID: g.CallID, Node: g.JoinNode, Step: l.header.Steps, BranchIndex: parent.BranchIndex}
		if t.join.CanMerge != nil && lg.remaining > 0 {
			canMergeParent, err := r.cloneState(parent.State, parent.ID)
			if err != nil {
				return t, fmt.Errorf("clone speculative join parent: %w", err)
			}
			canMergeBranches := make([]BranchResult[S], 0, len(g.Branches))
			for branchIndex, branch := range g.Branches {
				child, _, exists := l.invocation(branch.InvocationID, -1)
				if !exists {
					return t, invalidCheckpoint("join group %q has missing branch %q", ref.id, branch.InvocationID)
				}
				state, err := r.cloneState(child.State, child.ID)
				if err != nil {
					return t, fmt.Errorf("clone speculative join branch %q: %w", child.ID, err)
				}
				canMergeBranches = append(canMergeBranches, BranchResult[S]{Index: branchIndex, Target: branch.Target, Node: child.Node, Status: child.Status, State: state, Error: branch.Error})
			}
			ready, err := canMerge(t.join, t.call, canMergeParent, canMergeBranches)
			if err != nil {
				t.preflightErr = err
				return t, nil
			}
			if !ready {
				t.ready = false
				return t, nil
			}
		}
		parentState, err := r.cloneState(parent.State, parent.ID)
		if err != nil {
			return t, fmt.Errorf("clone join parent: %w", err)
		}
		t.state = parentState
		t.branches = make([]BranchResult[S], 0, len(g.Branches))
		for branchIndex, branch := range g.Branches {
			id := branch.InvocationID
			child, _, exists := l.invocation(id, -1)
			if !exists {
				return t, invalidCheckpoint("join group %q has missing branch %q", ref.id, id)
			}
			res := BranchResult[S]{
				Index:  branchIndex,
				Target: branch.Target,
				Node:   child.Node,
				Status: child.Status,
				Error:  branch.Error,
			}
			if child.Status == InvocationJoined {
				value, err := r.cloneState(child.State, id)
				if err != nil {
					return t, fmt.Errorf("clone join input: %w", err)
				}
				res.State = value
			}
			t.branches = append(t.branches, res)
		}
	} else {
		v, _, ok := l.invocation(ref.id, ref.slot)
		if !ok {
			return t, invalidCheckpoint("node invocation %q missing", ref.id)
		}
		state, err := r.cloneState(v.State, v.ID)
		if err != nil {
			return t, fmt.Errorf("clone node state: %w", err)
		}
		t.state = state
		var n *CompiledNode[S]
		if uint(ref.nodeIdx) < uint(len(r.nodes)) && r.nodes[ref.nodeIdx].Spec.Name == v.Node {
			n = &r.nodes[ref.nodeIdx]
		} else {
			n = r.node(v.Node)
		}
		if n != nil {
			t.node = n
			t.spec = n.Spec
		}
		if v.GroupID != "" {
			if activation, _, ok := l.group(v.GroupID, -1); ok && v.BranchIndex >= 0 && v.BranchIndex < len(activation.group.Branches) {
				t.timeout = activation.group.Branches[v.BranchIndex].EntryTimeout
			}
		}
		if t.timeout <= 0 {
			t.timeout = t.spec.Timeout
		}
		t.call = CallInfo{RunID: l.header.RunID, InvocationID: v.ID, CallID: v.CallID, Node: v.Node, Step: l.header.Steps, BranchIndex: v.BranchIndex}
	}
	return t, nil
}
