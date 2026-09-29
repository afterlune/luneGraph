package executor

// invocationIndex maps invocation IDs to positions in one working checkpoint.
// It keys by each canonical ID's numeric part and stores positions rather than
// pointers because checkpoint buffers may swap.
// Short invocation lists use a linear scan because building a map costs more.
const invocationIndexThreshold = 8

type invocationIndex struct {
	positions    map[uint64]int
	capacityHint int
}

func newInvocationIndex[S any](checkpoint Checkpoint[S]) invocationIndex {
	index := invocationIndex{}
	if len(checkpoint.Invocations) <= invocationIndexThreshold {
		return index
	}
	index.positions = make(map[uint64]int, len(checkpoint.Invocations))
	index.capacityHint = len(checkpoint.Invocations)
	for i := range checkpoint.Invocations {
		index.positions[invocationNumber(checkpoint.Invocations[i].ID)] = i
	}
	return index
}

func indexedInvocation[S any](index *invocationIndex, checkpoint *Checkpoint[S], id string) (int, *Invocation[S]) {
	if index != nil && index.positions != nil {
		position, ok := index.positions[invocationNumber(id)]
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
	positions := make(map[uint64]int, expected)
	for i := range checkpoint.Invocations {
		positions[invocationNumber(checkpoint.Invocations[i].ID)] = i
	}
	index.positions = positions
	index.capacityHint = expected
}

func appendInvocation[S any](index *invocationIndex, checkpoint *Checkpoint[S], invocation Invocation[S]) *Invocation[S] {
	reserveInvocationIndex(index, checkpoint, 1)
	position := len(checkpoint.Invocations)
	checkpoint.Invocations = append(checkpoint.Invocations, invocation)
	if index.positions != nil {
		index.positions[invocationNumber(invocation.ID)] = position
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
			positions[invocationNumber(invocation.ID)] = write
		}
		write++
	}
	clear(invocations[write:])
	checkpoint.Invocations = invocations[:write]
	if write <= invocationIndexThreshold {
		index.positions = nil
		index.capacityHint = 0
	} else if positions == nil {
		reserveInvocationIndex(index, checkpoint, 0)
	}
}

func clearInvocationIndex(index *invocationIndex) {
	clear(index.positions)
	index.positions = nil
	index.capacityHint = 0
}
