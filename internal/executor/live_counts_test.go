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
	for _, g := range cp.Groups {
		unresolved := 0
		for _, branch := range g.Branches {
			id := branch.InvocationID
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
		lg, _, ok := l.group(g.ID, -1)
		if !ok || lg.remaining != unresolved {
			t.Fatalf("group %s remaining=%d want=%d", g.ID, lg.remaining, unresolved)
		}
	}
}

func TestLiveInstallKeepsCountsAcrossNestedSettlementAndFailure(t *testing.T) {
	l := newLive(nestedCandidatePosition(), nil)
	assertLiveCounts(t, l)
	inv, _ := l.getInvocation("i9")
	inv.Status = InvocationWaiting
	l.install(Change[int]{Header: l.header.Header(), UpsertInvocations: []Invocation[int]{inv}})
	assertLiveCounts(t, l)
	inv, _ = l.getInvocation("i10")
	inv.Status = InvocationReady
	l.install(Change[int]{Header: l.header.Header(), UpsertInvocations: []Invocation[int]{inv}})
	assertLiveCounts(t, l)
	// Settle one nested activation while its sibling and outer waiter remain.
	inv, _ = l.getInvocation("i3")
	inv.Status, inv.ChildGroupID = InvocationJoined, ""
	l.install(Change[int]{Header: l.header.Header(), UpsertInvocations: []Invocation[int]{inv}, DeleteInvocations: []string{"i6", "i7"}, DeleteGroups: []string{"g5"}})
	assertLiveCounts(t, l)
	if l.valid(taskRef{id: "g5", join: true}) {
		t.Fatal("settled activation is still runnable")
	}
	r := executorTestRunner(t, "unused", nil, []JoinSpec[int]{{Name: "inner-join", OnError: FailGroup}}, nil, nil, nil)
	// Test the same enclosing-group collection used when a Merge fails. g5 is
	// ready in a fresh position, so no user callback or scheduling is needed.
	l = newLive(nestedCandidatePosition(), nil)
	change, terminalErr, err := r.resultChange(l, workResult[int]{ref: taskRef{id: "g5", join: true}, err: errors.New("merge failed")}, 0)
	if err != nil || terminalErr != nil {
		t.Fatalf("local failure = %v %v", err, terminalErr)
	}
	l.install(change)
	assertLiveCounts(t, l)
	lg0, _, ok := l.group("g0", -1)
	if len(l.groups) != 1 || !ok || lg0.remaining != 1 || !l.header.HadLocalFailures {
		t.Fatal("failure did not remove exactly its enclosing activation")
	}
	parent, _ := l.getInvocation("i1")
	other, ok := l.getInvocation("i90")
	if parent.Status != InvocationFailed || !ok || other.Status != InvocationWaiting || l.valid(taskRef{id: "g5", join: true}) {
		t.Fatal("failure affected unrelated waiter or retained a removed task")
	}
	header := l.header.Header()
	header.Completed = true
	l.install(Change[int]{Header: header})
	assertLiveCounts(t, l)
	if l.invocationCount() != 0 || l.nodeQueue.len() != 0 || l.joinQueue.len() != 0 || len(l.groups) != 0 {
		t.Fatal("terminal position retained active storage")
	}
}
