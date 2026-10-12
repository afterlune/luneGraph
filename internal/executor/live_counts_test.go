package executor

import (
	"errors"
	"testing"
)

func assertLiveCounts(t *testing.T, l *live[int]) {
	t.Helper()
	cp := l.snapshot()
	ready, waiting := 0, 0
	for _, inv := range cp.Invocations {
		if inv.Status == InvocationReady {
			ready++
		}
		if inv.Status == InvocationWaiting {
			waiting++
		}
	}
	if ready != l.readyNodes || waiting != l.waitingInvocations {
		t.Fatalf("counts ready=%d/%d waiting=%d/%d", ready, l.readyNodes, waiting, l.waitingInvocations)
	}
	for id := range l.remaining {
		if _, exists := l.groups[id]; !exists {
			t.Fatal("deleted group retains a count", id)
		}
	}
	for _, g := range cp.Groups {
		unresolved := 0
		for _, id := range g.Children {
			found := false
			for _, inv := range cp.Invocations {
				if inv.ID != id {
					continue
				}
				found = true
				switch inv.Status {
				case InvocationReady, InvocationWaiting, InvocationGroup:
					unresolved++
				}
			}
			if !found {
				t.Fatalf("group %s missing child %s", g.ID, id)
			}
		}
		if got := l.remaining[g.ID]; got != unresolved {
			t.Fatalf("group %s remaining=%d want=%d", g.ID, got, unresolved)
		}
	}
}

func TestLiveInstallKeepsCountsAcrossNestedSettlementAndFailure(t *testing.T) {
	l := newLive(nestedCandidatePosition(), nil, nil)
	assertLiveCounts(t, l)
	inv, _ := l.invocation("i9")
	inv.Status = InvocationWaiting
	l.install(Change[int]{Header: l.header.Header(), UpsertInvocations: []Invocation[int]{inv}})
	assertLiveCounts(t, l)
	inv, _ = l.invocation("i10")
	inv.Status = InvocationReady
	l.install(Change[int]{Header: l.header.Header(), UpsertInvocations: []Invocation[int]{inv}})
	assertLiveCounts(t, l)
	// Settle one nested activation while its sibling and outer waiter remain.
	inv, _ = l.invocation("i3")
	inv.Status, inv.ChildGroupID = InvocationJoined, ""
	l.install(Change[int]{Header: l.header.Header(), UpsertInvocations: []Invocation[int]{inv}, DeleteInvocations: []string{"i6", "i7"}, DeleteGroups: []string{"g5"}})
	assertLiveCounts(t, l)
	if l.valid(taskRef{id: "g5", join: true}) {
		t.Fatal("settled activation is still runnable")
	}
	r := executorTestRunner(t, "unused", nil, []JoinSpec[int]{{Name: "inner-join", OnError: FailGroup}}, nil, nil, nil)
	// Test the same enclosing-group collection used when a Merge fails. g5 is
	// ready in a fresh position, so no user callback or scheduling is needed.
	l = newLive(nestedCandidatePosition(), nil, nil)
	change, terminalErr, err := r.resultChange(l, workResult[int]{ref: taskRef{id: "g5", join: true}, err: errors.New("merge failed")})
	if err != nil || terminalErr != nil {
		t.Fatalf("local failure = %v %v", err, terminalErr)
	}
	l.install(change)
	assertLiveCounts(t, l)
	if len(l.groups) != 1 || l.remaining["g0"] != 1 || !l.header.HadLocalFailures {
		t.Fatal("failure did not remove exactly its enclosing activation")
	}
	parent, _ := l.invocation("i1")
	other, ok := l.invocation("i90")
	if parent.Status != InvocationFailed || !ok || other.Status != InvocationWaiting || l.valid(taskRef{id: "g5", join: true}) {
		t.Fatal("failure affected unrelated waiter or retained a removed task")
	}
	header := l.header.Header()
	header.Completed = true
	l.install(Change[int]{Header: header})
	assertLiveCounts(t, l)
	if l.invocationCount() != 0 || l.ready.len() != 0 || l.ready.items != nil || l.hasSingle {
		t.Fatal("terminal position retained active storage")
	}
}
