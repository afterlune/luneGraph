package executor

// groupProgress belongs to one drive or input batch. Terminal children never
// become runnable again within their activation, so a confirmed prefix remains
// valid across checkpoint buffer swaps. No state or invocation pointers escape.
type groupProgress struct {
	oneID     string
	onePrefix int
	// many holds a snapshot while topology is reconciled; order owns the
	// current prefixes during ordinary readiness checks.
	many  map[string]groupPrefix
	order []groupCursor
}

type groupCursor struct {
	id     string
	prefix int
}

type groupPrefix struct {
	prefix int
	live   bool
}

func (p *groupProgress) sync(groups []ActivationGroup) {
	if len(groups) == 0 {
		*p = groupProgress{}
		return
	}
	if len(groups) == 1 {
		id := groups[0].ID
		prefix := 0
		if p.many != nil {
			for _, cursor := range p.order {
				if cursor.id == id {
					prefix = cursor.prefix
					break
				}
			}
		} else if p.oneID == id {
			prefix = p.onePrefix
		}
		*p = groupProgress{oneID: id, onePrefix: prefix}
		return
	}
	if p.many == nil {
		p.many = make(map[string]groupPrefix, len(groups))
		if p.oneID != "" {
			p.many[p.oneID] = groupPrefix{prefix: p.onePrefix}
		}
		p.oneID, p.onePrefix = "", 0
	}
	// Settling or failing groups normally removes entries without reordering
	// survivors. Compact their records directly, avoiding a map rebuild for
	// every nested join. Unexpected replacement/reordering takes the ID path.
	if len(groups) < len(p.order) {
		next := 0
		for _, cursor := range p.order {
			if next < len(groups) && cursor.id == groups[next].ID {
				next++
			}
		}
		if next == len(groups) {
			next = 0
			for _, cursor := range p.order {
				if next < len(groups) && cursor.id == groups[next].ID {
					p.order[next] = cursor
					next++
				} else {
					delete(p.many, cursor.id)
				}
			}
			clear(p.order[next:])
			p.order = p.order[:next]
			return
		}
	}
	// Most node results change only child status. Comparing IDs avoids map
	// writes when topology is unchanged, including across buffer exchanges.
	unchanged := len(p.order) == len(groups)
	if unchanged {
		for i, group := range groups {
			if p.order[i].id != group.ID {
				unchanged = false
				break
			}
		}
	}
	if unchanged {
		return
	}
	for _, cursor := range p.order {
		p.many[cursor.id] = groupPrefix{prefix: cursor.prefix}
	}
	for id, entry := range p.many {
		entry.live = false
		p.many[id] = entry
	}
	for _, group := range groups {
		entry := p.many[group.ID]
		entry.live = true
		p.many[group.ID] = entry
	}
	for id, entry := range p.many {
		if !entry.live {
			delete(p.many, id)
		}
	}
	clear(p.order)
	if cap(p.order) < len(groups) {
		p.order = make([]groupCursor, len(groups))
	} else {
		p.order = p.order[:len(groups)]
	}
	for i, group := range groups {
		p.order[i] = groupCursor{id: group.ID, prefix: p.many[group.ID].prefix}
	}
}

func readyGroup[S any](s *Checkpoint[S], index *invocationIndex, progress *groupProgress) int {
	if len(s.Groups) < 2 || len(progress.order) != len(s.Groups) {
		progress.sync(s.Groups)
	}
	for i, group := range s.Groups {
		prefix := progress.onePrefix
		if progress.many != nil {
			// Check identity while traversing, rather than scanning all groups
			// a second time on every result. A changed topology is reconciled
			// before reading its prefix; unvisited groups are never consulted.
			if progress.order[i].id != group.ID {
				progress.sync(s.Groups)
			}
			prefix = progress.order[i].prefix
		}
		for prefix < len(group.Children) {
			_, child := indexedInvocation(index, s, group.Children[prefix])
			if child == nil || (child.Status != InvocationJoined && child.Status != InvocationEnded && child.Status != InvocationFailed) {
				break
			}
			prefix++
		}
		if progress.many == nil {
			progress.onePrefix = prefix
		} else {
			progress.order[i].prefix = prefix
		}
		if prefix == len(group.Children) {
			return i
		}
	}
	return -1
}
