package executor

import (
	"testing"
)

func TestLiveReadinessAndCandidateIsolation(t *testing.T) {
	cp := Checkpoint[int]{Invocations: []Invocation[int]{{ID: "i1", Status: InvocationGroup, ChildGroupID: "g3"}, {ID: "i5", Status: InvocationReady, GroupID: "g3", State: 1}, {ID: "i7", Status: InvocationReady, GroupID: "g3", State: 2}}, Groups: []ActivationGroup{{ID: "g3", ParentID: "i1", Branches: []ActivationBranch{{InvocationID: "i5"}, {InvocationID: "i7"}}}}}
	l := newLive(cp, nil, nil)
	for _, id := range []string{"i5", "i7"} {
		before := l.workspace.collect(l, id, false, "")
		after := before
		after.Invocations = append([]Invocation[int](nil), before.Invocations...)
		after.Invocations[0].Status = InvocationJoined
		after.Invocations[0].State = 9
		c := l.workspace.change(before, after)
		l.install(c)
	}
	if l.remaining["g3"] != 0 {
		t.Fatal(l.remaining)
	}
	ref, ok := l.next(false, nil)
	if !ok || !ref.join || ref.id != "g3" {
		t.Fatal(ref, ok)
	}
	if cp.Invocations[1].State != 1 || cp.Invocations[1].Status != InvocationReady {
		t.Fatal("candidate mutated source", cp)
	}
	l.install(Change[int]{Header: l.header.Header(), DeleteGroups: []string{"g3"}})
	if l.valid(ref) {
		t.Fatal("deleted group remains runnable")
	}
}
