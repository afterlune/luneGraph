package executor

import (
	"strconv"
	"testing"
)

func TestInvocationIndexTracksAppendCompactionAndCheckpointCopy(t *testing.T) {
	checkpoint := Checkpoint[int]{Invocations: []Invocation[int]{
		{ID: "i1", State: 1},
		{ID: "i2", State: 2},
	}}
	index := newInvocationIndex(checkpoint)

	if position, invocation := indexedInvocation(&index, &checkpoint, "i2"); position != 1 || invocation == nil || invocation.State != 2 {
		t.Fatalf("initial lookup = position %d, invocation %+v", position, invocation)
	}
	if position, invocation := indexedInvocation(&index, &checkpoint, "missing"); position != -1 || invocation != nil {
		t.Fatalf("missing lookup = position %d, invocation %+v", position, invocation)
	}
	appendInvocation(&index, &checkpoint, Invocation[int]{ID: "i3", State: 3})

	var spare Checkpoint[int]
	candidate := copyCheckpointInto(&spare, checkpoint)
	if position, invocation := indexedInvocation(&index, &candidate, "i3"); position != 2 || invocation == nil || invocation.State != 3 {
		t.Fatalf("copied lookup = position %d, invocation %+v", position, invocation)
	}

	removeInvocations(&index, &candidate, map[string]bool{"i2": true})
	if len(candidate.Invocations) != 2 || candidate.Invocations[0].ID != "i1" || candidate.Invocations[1].ID != "i3" {
		t.Fatalf("compacted invocations = %+v", candidate.Invocations)
	}
	if position, invocation := indexedInvocation(&index, &candidate, "i3"); position != 1 || invocation == nil || invocation.State != 3 {
		t.Fatalf("lookup after compaction = position %d, invocation %+v", position, invocation)
	}
	if position, invocation := indexedInvocation(&index, &candidate, "i2"); position != -1 || invocation != nil {
		t.Fatalf("removed lookup = position %d, invocation %+v", position, invocation)
	}
	if checkpoint.Invocations[1].ID != "i2" {
		t.Fatalf("compaction modified the source checkpoint: %+v", checkpoint.Invocations)
	}
	if tail := candidate.Invocations[:cap(candidate.Invocations)]; tail[2].ID != "" || tail[2].State != 0 || tail[2].Next != nil {
		t.Fatalf("compaction retained removed invocation: %+v", tail[2])
	}

	clearInvocationIndex(&index)
	candidate.Invocations = nil
	if position, invocation := indexedInvocation(&index, &candidate, "i1"); position != -1 || invocation != nil {
		t.Fatalf("cleared lookup = position %d, invocation %+v", position, invocation)
	}
}

func TestInvocationIndexMapsLargeCheckpointsAndShrinks(t *testing.T) {
	checkpoint := Checkpoint[int]{Invocations: make([]Invocation[int], 12)}
	for i := range checkpoint.Invocations {
		checkpoint.Invocations[i] = Invocation[int]{ID: "i" + strconv.Itoa(i+1), State: i + 1}
	}
	index := newInvocationIndex(checkpoint)
	if index.positions == nil {
		t.Fatal("large checkpoint did not build an ID index")
	}

	appendInvocation(&index, &checkpoint, Invocation[int]{ID: "i13", State: 13})
	if position, invocation := indexedInvocation(&index, &checkpoint, "i13"); position != 12 || invocation == nil || invocation.State != 13 {
		t.Fatalf("appended lookup = position %d, invocation %+v", position, invocation)
	}
	removeInvocations(&index, &checkpoint, map[string]bool{"i2": true, "i5": true})
	if len(checkpoint.Invocations) != 11 || index.positions == nil {
		t.Fatalf("large compaction = %d invocations, indexed=%t", len(checkpoint.Invocations), index.positions != nil)
	}
	if position, invocation := indexedInvocation(&index, &checkpoint, "i13"); position != 10 || invocation == nil || invocation.State != 13 {
		t.Fatalf("lookup after indexed compaction = position %d, invocation %+v", position, invocation)
	}

	removeInvocations(&index, &checkpoint, map[string]bool{"i1": true, "i3": true, "i4": true})
	if len(checkpoint.Invocations) != invocationIndexThreshold || index.positions != nil {
		t.Fatalf("small compaction = %d invocations, indexed=%t", len(checkpoint.Invocations), index.positions != nil)
	}
	if position, invocation := indexedInvocation(&index, &checkpoint, "i13"); position != invocationIndexThreshold-1 || invocation == nil || invocation.State != 13 {
		t.Fatalf("lookup after index shrink = position %d, invocation %+v", position, invocation)
	}
}
