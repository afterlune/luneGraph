package executor

import (
	"context"
	"sort"
)

// selectReady preserves numeric-ID round robin, even when a checkpoint's
// invocation slice is unordered or the cursor's invocation has been removed.
// Large sets search their transient numeric order and stop at the first match.
// Sparse sets may still scan every entry; small sets avoid index allocations.
func selectReady[S any](s Checkpoint[S], index *invocationIndex, running map[string]context.CancelFunc, cursor uint64) (*Invocation[S], uint64) {
	if index == nil || index.positions == nil {
		return scanReady(s, running, cursor)
	}
	start := sort.Search(len(index.order), func(i int) bool { return index.order[i].number > cursor })
	for _, entries := range [][]invocationOrder{index.order[start:], index.order[:start]} {
		for _, entry := range entries {
			inv := &s.Invocations[entry.position]
			if inv.Status != InvocationReady {
				continue
			}
			if _, active := running[inv.ID]; !active {
				return inv, entry.number
			}
		}
	}
	return nil, 0
}

func scanReady[S any](s Checkpoint[S], running map[string]context.CancelFunc, cursor uint64) (*Invocation[S], uint64) {
	var after, wrapped *Invocation[S]
	var afterNumber, wrappedNumber uint64
	for i := range s.Invocations {
		inv := &s.Invocations[i]
		if inv.Status != InvocationReady {
			continue
		}
		if _, active := running[inv.ID]; active {
			continue
		}
		number := invocationNumber(inv.ID)
		if number > cursor {
			if after == nil || number < afterNumber {
				after, afterNumber = inv, number
			}
		} else if wrapped == nil || number < wrappedNumber {
			wrapped, wrappedNumber = inv, number
		}
	}
	if after != nil {
		return after, afterNumber
	}
	return wrapped, wrappedNumber
}
