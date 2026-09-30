package executor

import (
	"cmp"
	"slices"
)

// invocationIndex maps invocation IDs to positions in one working checkpoint.
// Its numeric ordering is transient; positions remain valid across checkpoint
// buffer swaps. Neither index stores application state or invocation pointers.
// Short invocation lists use a linear scan because building a map costs more.
const invocationIndexThreshold = 8

type invocationIndex struct {
	positions    map[string]int
	order        []invocationOrder
	capacityHint int
}

type invocationOrder struct {
	id       string
	number   uint64
	position int
}

func newInvocationIndex[S any](checkpoint Checkpoint[S]) invocationIndex {
	index := invocationIndex{}
	reserveInvocationIndex(&index, &checkpoint, 0)
	return index
}

func indexedInvocation[S any](index *invocationIndex, checkpoint *Checkpoint[S], id string) (int, *Invocation[S]) {
	if index != nil && index.positions != nil {
		position, ok := index.positions[id]
		if !ok || position < 0 || position >= len(checkpoint.Invocations) || checkpoint.Invocations[position].ID != id {
			return -1, nil
		}
		return position, &checkpoint.Invocations[position]
	}
	for position := range checkpoint.Invocations {
		if checkpoint.Invocations[position].ID == id {
			return position, &checkpoint.Invocations[position]
		}
	}
	return -1, nil
}

func reserveInvocationIndex[S any](index *invocationIndex, checkpoint *Checkpoint[S], additional int) {
	expected := len(checkpoint.Invocations) + additional
	if expected <= invocationIndexThreshold || (index.positions != nil && index.capacityHint >= expected) {
		return
	}
	if index.positions == nil {
		index.order = make([]invocationOrder, len(checkpoint.Invocations), expected)
		for i, inv := range checkpoint.Invocations {
			index.order[i] = invocationOrder{id: inv.ID, number: invocationNumber(inv.ID), position: i}
		}
		slices.SortFunc(index.order, func(a, b invocationOrder) int { return cmp.Compare(a.number, b.number) })
	} else {
		index.order = slices.Grow(index.order, expected-len(index.order))
	}
	positions := make(map[string]int, expected)
	for i := range checkpoint.Invocations {
		positions[checkpoint.Invocations[i].ID] = i
	}
	index.positions = positions
	index.capacityHint = expected
}

func appendInvocation[S any](index *invocationIndex, checkpoint *Checkpoint[S], invocation Invocation[S]) *Invocation[S] {
	reserveInvocationIndex(index, checkpoint, 1)
	position := len(checkpoint.Invocations)
	checkpoint.Invocations = append(checkpoint.Invocations, invocation)
	if index.positions != nil {
		index.positions[invocation.ID] = position
		entry := invocationOrder{id: invocation.ID, number: invocationNumber(invocation.ID), position: position}
		// Allocated IDs grow monotonically; insert earlier IDs in numeric order
		// as well when this helper is used to build an unordered set.
		at := len(index.order)
		if at != 0 && index.order[at-1].number > entry.number {
			at, _ = slices.BinarySearchFunc(index.order, entry.number, func(a invocationOrder, number uint64) int { return cmp.Compare(a.number, number) })
		}
		index.order = slices.Insert(index.order, at, entry)
	}
	return &checkpoint.Invocations[position]
}

func removeInvocations[S any](index *invocationIndex, checkpoint *Checkpoint[S], remove map[string]bool) {
	positions := index.positions
	clear(positions)
	invocations := checkpoint.Invocations
	write := 0
	for read := range invocations {
		invocation := invocations[read]
		if remove[invocation.ID] {
			continue
		}
		invocations[write] = invocation
		if positions != nil {
			positions[invocation.ID] = write
		}
		write++
	}
	clear(invocations[write:])
	checkpoint.Invocations = invocations[:write]
	if write <= invocationIndexThreshold {
		clearInvocationIndex(index)
	} else if positions == nil {
		reserveInvocationIndex(index, checkpoint, 0)
	} else {
		kept := 0
		for _, entry := range index.order {
			position, exists := positions[entry.id]
			if !exists {
				continue
			}
			entry.position = position
			index.order[kept] = entry
			kept++
		}
		clear(index.order[kept:])
		index.order = index.order[:kept]
	}
}

func clearInvocationIndex(index *invocationIndex) {
	clear(index.positions)
	clear(index.order)
	index.positions = nil
	index.order = nil
	index.capacityHint = 0
}
