package executor

// candidateWorkspace owns reusable, speculative topology slices. They never
// alias the slices that store the accepted position. User states are borrowed
// read-only until callback results transfer ownership through a Change.
type candidateWorkspace[S any] struct {
	beforeInv, afterInv       []Invocation[S]
	beforeGroups, afterGroups []ActivationGroup
	membership                map[string]bool
	groupsBefore              map[string]ActivationGroup
}

type candidateMembers struct {
	invocations    map[string]bool
	groups         map[string]bool
	invocationHint int
}

func (w *candidateWorkspace[S]) addInvocation(l *live[S], cp *Checkpoint[S], id string, m *candidateMembers) {
	if m.invocations != nil {
		if m.invocations[id] {
			return
		}
	} else {
		for _, v := range cp.Invocations {
			if v.ID == id {
				return
			}
		}
		if len(cp.Invocations) >= invocationIndexThreshold {
			// Collection and diffing are sequential phases. Reuse their ID set
			// rather than allocating a second map for a wide join.
			if w.membership == nil {
				w.membership = make(map[string]bool, max(len(cp.Invocations)+1, m.invocationHint))
			}
			clear(w.membership)
			m.invocations = w.membership
			for _, v := range cp.Invocations {
				m.invocations[v.ID] = true
			}
		}
	}
	v, ok := l.invocation(id)
	if !ok {
		return
	}
	if m.invocations != nil {
		m.invocations[id] = true
	}
	cp.Invocations = append(cp.Invocations, v)
}

func (w *candidateWorkspace[S]) addGroup(l *live[S], cp *Checkpoint[S], id string, children bool, m *candidateMembers) {
	if m.groups != nil {
		if m.groups[id] {
			return
		}
	} else {
		for _, g := range cp.Groups {
			if g.ID == id {
				return
			}
		}
		if len(cp.Groups) >= invocationIndexThreshold {
			m.groups = make(map[string]bool, len(cp.Groups)+1)
			for _, g := range cp.Groups {
				m.groups[g.ID] = true
			}
		}
	}
	g, ok := l.groups[id]
	if !ok {
		return
	}
	if m.groups != nil {
		m.groups[id] = true
	}
	cp.Groups = append(cp.Groups, g)
	if children {
		w.addInvocation(l, cp, g.ParentID, m)
		for _, branch := range g.Branches {
			child := branch.InvocationID
			w.addInvocation(l, cp, child, m)
			descendant, exists := l.invocation(child)
			if nested := descendant.ChildGroupID; exists && nested != "" {
				w.addGroup(l, cp, nested, true, m)
			}
		}
	}
}

func (w *candidateWorkspace[S]) collect(l *live[S], id string, subtree bool, joinGroupID string) Checkpoint[S] {
	cp := l.header
	clear(w.beforeInv)
	clear(w.beforeGroups)
	cp.Invocations = w.beforeInv[:0]
	cp.Groups = w.beforeGroups[:0]
	var members candidateMembers
	if joinGroupID != "" {
		// A pending join already knows its local child count. Reserve that
		// set when it crosses the indexing threshold, not the whole run.
		members.invocationHint = len(l.groups[joinGroupID].Branches) + 1
	}
	w.addInvocation(l, &cp, id, &members)
	if v, ok := l.invocation(id); ok && v.GroupID != "" {
		w.addGroup(l, &cp, v.GroupID, subtree, &members)
		if subtree {
			// FailGroup marks this activation's parent failed. Include its outer
			// branch metadata without collecting unrelated outer descendants.
			g := l.groups[v.GroupID]
			if parent, exists := l.invocation(g.ParentID); exists && parent.GroupID != "" {
				w.addGroup(l, &cp, parent.GroupID, false, &members)
			}
		}
	}
	if joinGroupID != "" {
		w.addGroup(l, &cp, joinGroupID, true, &members)
	}
	w.beforeInv = cp.Invocations
	w.beforeGroups = cp.Groups
	return cp
}

// prepare creates independent structural working slices for one outcome.
func (w *candidateWorkspace[S]) prepare(l *live[S], id string, subtree bool, joinGroupID string) (Checkpoint[S], Checkpoint[S]) {
	before := w.collect(l, id, subtree, joinGroupID)
	after := before
	clear(w.afterInv)
	clear(w.afterGroups)
	after.Invocations = append(w.afterInv[:0], before.Invocations...)
	after.Groups = append(w.afterGroups[:0], before.Groups...)
	w.afterInv, w.afterGroups = after.Invocations, after.Groups
	return before, after
}

func (w *candidateWorkspace[S]) change(before, after Checkpoint[S]) Change[S] {
	w.afterInv, w.afterGroups = after.Invocations, after.Groups
	c := Change[S]{Header: after.Header(), Final: after.Final, UpsertInvocations: after.Invocations}
	if len(before.Invocations)+len(after.Invocations) < 16 {
		for _, v := range before.Invocations {
			found := false
			for _, next := range after.Invocations {
				if v.ID == next.ID {
					found = true
					break
				}
			}
			if !found {
				c.DeleteInvocations = append(c.DeleteInvocations, v.ID)
			}
		}
	} else {
		if w.membership == nil {
			w.membership = make(map[string]bool, len(before.Invocations))
		}
		clear(w.membership)
		for _, v := range before.Invocations {
			w.membership[v.ID] = true
		}
		for _, v := range after.Invocations {
			delete(w.membership, v.ID)
		}
		for id := range w.membership {
			c.DeleteInvocations = append(c.DeleteInvocations, id)
		}
	}
	if len(before.Groups)+len(after.Groups) < 16 {
		for _, old := range before.Groups {
			found := false
			for _, next := range after.Groups {
				if old.ID == next.ID {
					found = true
					break
				}
			}
			if !found {
				c.DeleteGroups = append(c.DeleteGroups, old.ID)
			}
		}
		for _, next := range after.Groups {
			found := false
			for _, old := range before.Groups {
				if next.ID == old.ID {
					if !sameActivationGroup(next, old) {
						c.UpsertGroups = append(c.UpsertGroups, next)
					}
					found = true
					break
				}
			}
			if !found {
				c.UpsertGroups = append(c.UpsertGroups, next)
			}
		}
	} else {
		if w.groupsBefore == nil {
			w.groupsBefore = make(map[string]ActivationGroup, len(before.Groups))
		}
		clear(w.groupsBefore)
		for _, g := range before.Groups {
			w.groupsBefore[g.ID] = g
		}
		for _, g := range after.Groups {
			if old, exists := w.groupsBefore[g.ID]; exists {
				if !sameActivationGroup(g, old) {
					c.UpsertGroups = append(c.UpsertGroups, g)
				}
				delete(w.groupsBefore, g.ID)
			} else {
				c.UpsertGroups = append(c.UpsertGroups, g)
			}
		}
		for id := range w.groupsBefore {
			c.DeleteGroups = append(c.DeleteGroups, id)
		}
	}
	clear(w.membership)
	return c
}

func sameActivationGroup(a, b ActivationGroup) bool {
	if a.ID != b.ID || a.CallID != b.CallID || a.Source != b.Source || a.ParentID != b.ParentID || a.JoinNode != b.JoinNode || len(a.Branches) != len(b.Branches) {
		return false
	}
	if len(a.Branches) == 0 || &a.Branches[0] == &b.Branches[0] {
		return true
	}
	for i := range a.Branches {
		if a.Branches[i] != b.Branches[i] {
			return false
		}
	}
	return true
}
