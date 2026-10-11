package executor

// invocationIndex maps invocation IDs to positions in one working checkpoint.
// Positions refer to the current candidate slice. The index owns no state.
// Short invocation lists use a linear scan because building a map costs more.
const invocationIndexThreshold = 8

type invocationIndex struct{ positions map[string]int }

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
	if expected <= invocationIndexThreshold || index.positions != nil {
		return
	}
	positions := make(map[string]int, expected)
	for i := range checkpoint.Invocations {
		positions[checkpoint.Invocations[i].ID] = i
	}
	index.positions = positions
}

func appendInvocation[S any](index *invocationIndex, checkpoint *Checkpoint[S], invocation Invocation[S]) *Invocation[S] {
	reserveInvocationIndex(index, checkpoint, 1)
	position := len(checkpoint.Invocations)
	checkpoint.Invocations = append(checkpoint.Invocations, invocation)
	if index.positions != nil {
		index.positions[invocation.ID] = position

	}
	return &checkpoint.Invocations[position]
}

func removeInvocations[S any](index *invocationIndex, checkpoint *Checkpoint[S], remove map[string]bool) {
	positions := index.positions
	invocations := checkpoint.Invocations
	write := 0
	for read := range invocations {
		invocation := invocations[read]
		if remove[invocation.ID] {
			if positions != nil {
				delete(positions, invocation.ID)
			}
			continue
		}
		invocations[write] = invocation
		if positions != nil && write != read {
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
	}
}

func clearInvocationIndex(index *invocationIndex) {
	clear(index.positions)
	index.positions = nil
}
