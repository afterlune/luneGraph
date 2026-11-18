package executor

// invocationIndex maps invocation IDs to positions in one candidate position.
// Positions refer to the current candidate slice. The index owns no state.
// Short invocation lists use a linear scan because building a map costs more.
const invocationIndexThreshold = 8

type invocationIndex struct{ positions map[string]int }

func newInvocationIndex[S any](position candidatePosition[S]) invocationIndex {
	index := invocationIndex{}
	reserveInvocationIndex(&index, &position, 0)
	return index
}

func indexedInvocation[S any](index *invocationIndex, position *candidatePosition[S], id string) (int, *Invocation[S]) {
	if index != nil && index.positions != nil {
		offset, ok := index.positions[id]
		if !ok || offset < 0 || offset >= len(position.Invocations) || position.Invocations[offset].ID != id {
			return -1, nil
		}
		return offset, &position.Invocations[offset]
	}
	for offset := range position.Invocations {
		if position.Invocations[offset].ID == id {
			return offset, &position.Invocations[offset]
		}
	}
	return -1, nil
}

func reserveInvocationIndex[S any](index *invocationIndex, position *candidatePosition[S], additional int) {
	expected := len(position.Invocations) + additional
	if expected <= invocationIndexThreshold || index.positions != nil {
		return
	}
	positions := make(map[string]int, expected)
	for i := range position.Invocations {
		positions[position.Invocations[i].ID] = i
	}
	index.positions = positions
}

func appendInvocation[S any](index *invocationIndex, position *candidatePosition[S], invocation Invocation[S]) *Invocation[S] {
	reserveInvocationIndex(index, position, 1)
	offset := len(position.Invocations)
	position.Invocations = append(position.Invocations, invocation)
	if index.positions != nil {
		index.positions[invocation.ID] = offset

	}
	return &position.Invocations[offset]
}

func removeInvocations[S any](index *invocationIndex, position *candidatePosition[S], remove map[string]bool) {
	positions := index.positions
	invocations := position.Invocations
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
	position.Invocations = invocations[:write]
	if write <= invocationIndexThreshold {
		clearInvocationIndex(index)
	} else if positions == nil {
		reserveInvocationIndex(index, position, 0)
	}
}

func clearInvocationIndex(index *invocationIndex) {
	clear(index.positions)
	index.positions = nil
}
