package model

// Copy returns a structural copy of a checkpoint. State values remain shared.
func Copy[S any](s Checkpoint[S]) Checkpoint[S] {
	var out Checkpoint[S]
	CopyInto(&out, s)
	return out
}

// CopyInto copies s into dst, reusing dst's structural slices when possible.
// State values retain Copy's shallow-copy behavior. dst must not share its
// slice storage with s; execution code uses separate alternating checkpoints
// to maintain this ownership boundary.
func CopyInto[S any](dst *Checkpoint[S], s Checkpoint[S]) {
	previous := *dst
	out := s
	out.Invocations = copyInvocations(previous.Invocations, s.Invocations)
	out.Groups = copyGroups(previous.Groups, s.Groups)
	out.Terminals = copySlice(previous.Terminals, s.Terminals)
	out.Failures = copySlice(previous.Failures, s.Failures)
	if s.Final != nil {
		value := *s.Final
		out.Final = &value
	}
	*dst = out
}

func copyInvocations[S any](dst, src []Invocation[S]) []Invocation[S] {
	if len(src) == 0 {
		clear(dst)
		return nil
	}
	old := dst
	dst = resizeSlice(dst, len(src))
	if len(src) > 8 {
		return copyInvocationRuns(dst, old, src)
	}
	for i, invocation := range src {
		var next []string
		if i < len(old) {
			next = old[i].Next
		}
		invocation.Next = copySlice(next, invocation.Next)
		dst[i] = invocation
	}
	return dst
}

// copyInvocationRuns batches nil-route entries without allocating metadata.
func copyInvocationRuns[S any](dst, old, src []Invocation[S]) []Invocation[S] {
	start := 0
	for i := range src {
		if src[i].Next == nil {
			// Preserve cleanup of the destination's obsolete route buffer.
			if i < len(old) {
				clear(old[i].Next)
			}
			continue
		}
		if start < i {
			// Exclude i so its old route buffer survives until the copy below.
			copy(dst[start:i], src[start:i])
		}
		var next []string
		if i < len(old) {
			next = old[i].Next
		}
		invocation := src[i]
		invocation.Next = copySlice(next, invocation.Next)
		dst[i] = invocation
		start = i + 1
	}
	copy(dst[start:], src[start:])
	return dst
}

func copyGroups(dst, src []ActivationGroup) []ActivationGroup {
	if len(src) == 0 {
		clear(dst)
		return nil
	}
	old := dst
	dst = resizeSlice(dst, len(src))
	for i, activation := range src {
		var children []string
		if i < len(old) {
			children = old[i].Children
		}
		activation.Children = copySlice(children, activation.Children)
		dst[i] = activation
	}
	return dst
}

func copySlice[T any](dst, src []T) []T {
	if len(src) == 0 {
		clear(dst)
		return nil
	}
	dst = resizeSlice(dst, len(src))
	copy(dst, src)
	return dst
}

func resizeSlice[T any](dst []T, size int) []T {
	if cap(dst) < size {
		return make([]T, size)
	}
	if size < len(dst) {
		clear(dst[size:])
	}
	return dst[:size]
}
