package executor

import (
	"cmp"
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
)

func nestedCandidatePosition() Checkpoint[int] {
	return Checkpoint[int]{RunID: "candidate", MachineID: "machine-v1", FormatVersion: CheckpointFormatVersion, Revision: 1, NextID: 1000,
		Invocations: []Invocation[int]{
			{ID: "i0", Status: InvocationGroup, ChildGroupID: "g0"},
			{ID: "i1", Status: InvocationGroup, GroupID: "g0", ChildGroupID: "g2"},
			{ID: "i3", Status: InvocationGroup, GroupID: "g2", ChildGroupID: "g5"},
			{ID: "i4", Status: InvocationGroup, GroupID: "g2", ChildGroupID: "g8"},
			{ID: "i6", Status: InvocationJoined, GroupID: "g5", State: 6},
			{ID: "i7", Status: InvocationFailed, GroupID: "g5"},
			{ID: "i9", Status: InvocationReady, GroupID: "g8"},
			{ID: "i10", Status: InvocationWaiting, GroupID: "g8"},
			{ID: "i90", Status: InvocationWaiting, GroupID: "g0"},
		}, Groups: []ActivationGroup{
			{ID: "g0", ParentID: "i0", Branches: []ActivationBranch{{InvocationID: "i1"}, {InvocationID: "i90"}}},
			{ID: "g2", ParentID: "i1", Branches: []ActivationBranch{{InvocationID: "i3"}, {InvocationID: "i4"}}},
			{ID: "g5", ParentID: "i3", JoinNode: "inner-join", Branches: []ActivationBranch{{InvocationID: "i6"}, {InvocationID: "i7"}}},
			{ID: "g8", ParentID: "i4", Branches: []ActivationBranch{{InvocationID: "i9"}, {InvocationID: "i10"}}},
		}}
}

func TestCandidateCollectsOnlyAffectedNestedTopology(t *testing.T) {
	l := newLive(nestedCandidatePosition(), nil, nil)
	for _, test := range []struct {
		name        string
		subtree     bool
		inv, groups []string
	}{
		{"join", false, []string{"i3", "i6", "i7"}, []string{"g2", "g5"}},
		{"failed enclosing group", true, []string{"i1", "i3", "i4", "i6", "i7", "i9", "i10"}, []string{"g2", "g5", "g8"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before, after := l.workspace.prepare(l, "i3", test.subtree, "g5")
			var inv, groups []string
			for _, v := range before.Invocations {
				inv = append(inv, v.ID)
			}
			for _, g := range before.Groups {
				groups = append(groups, g.ID)
			}
			slices.Sort(inv)
			slices.Sort(groups)
			wantInv := slices.Clone(test.inv)
			slices.Sort(wantInv)
			if !reflect.DeepEqual(inv, wantInv) || !reflect.DeepEqual(groups, test.groups) {
				t.Fatalf("candidate inv=%v groups=%v", inv, groups)
			}
			after.Invocations[0].Status = InvocationFailed
			after.Groups[0].JoinNode = "changed"
			if before.Invocations[0].Status == InvocationFailed || before.Groups[0].JoinNode == "changed" || !reflect.DeepEqual(l.snapshot(), sortedPosition(nestedCandidatePosition())) {
				t.Fatal("working candidate changed the before or accepted position")
			}
		})
	}
}

func sortedPosition[S any](cp Checkpoint[S]) Checkpoint[S] {
	slices.SortFunc(cp.Invocations, func(a, b Invocation[S]) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(cp.Groups, func(a, b ActivationGroup) int { return cmp.Compare(a.ID, b.ID) })
	return cp
}

func TestCandidateReuseAfterInvalidTransitionAndRejectedCommit(t *testing.T) {
	r := executorTestRunner(t, "source", []NodeSpec[int]{executorTestNode("source", executorTestEnd, FailExecution), executorTestNode("next", executorTestEnd, FailExecution)}, nil, map[string][]string{"source": {"next"}}, nil, nil)
	cp := executorTestReadyCheckpoint("source", 1)
	l := newLive(cp, nil, nil)
	if _, _, err := r.resultChange(l, workResult[int]{ref: taskRef{id: "i1"}, transition: model.To(9, "missing")}); err == nil {
		t.Fatal("invalid transition accepted")
	}
	if !reflect.DeepEqual(l.snapshot(), cp) {
		t.Fatal("invalid transition changed position")
	}
	store := newExecutorTestStore()
	store.values[cp.RunID] = cp
	store.compareErr = ErrConflict
	change, terminalErr, err := r.resultChange(l, workResult[int]{ref: taskRef{id: "i1"}, transition: model.To(7, "next")})
	if err != nil || terminalErr != nil {
		t.Fatalf("candidate: %v %v", err, terminalErr)
	}
	if err := r.submit(context.Background(), l, change, store, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("commit = %v", err)
	}
	if !reflect.DeepEqual(l.snapshot(), cp) || !reflect.DeepEqual(store.values[cp.RunID], cp) {
		t.Fatal("rejected commit changed position")
	}
	store.compareErr = nil
	change, terminalErr, err = r.resultChange(l, workResult[int]{ref: taskRef{id: "i1"}, transition: model.To(5, "next")})
	if err != nil || terminalErr != nil {
		t.Fatalf("reused candidate: %v %v", err, terminalErr)
	}
	if err := r.submit(context.Background(), l, change, store, nil); err != nil {
		t.Fatal(err)
	}
	want := l.snapshot()
	if want.Revision != 2 || want.Steps != 1 || want.Invocations[0].Node != "next" || want.Invocations[0].State != 5 || want.Failure != nil {
		t.Fatalf("accepted position = %+v", want)
	}
	// Building another outcome must not clear records already installed or saved.
	if _, _, err := r.resultChange(l, workResult[int]{ref: taskRef{id: "i1"}, transition: model.To(8, "missing")}); err == nil {
		t.Fatal("invalid successor accepted")
	}
	if !reflect.DeepEqual(l.snapshot(), want) || !reflect.DeepEqual(store.values[cp.RunID], want) {
		t.Fatal("scratch reuse changed accepted records")
	}
}

func TestCandidateWideCollectionAndDifferenceReuse(t *testing.T) {
	cp := Checkpoint[int]{Invocations: []Invocation[int]{{ID: "parent", Status: InvocationGroup}}, Groups: []ActivationGroup{{ID: "wide", ParentID: "parent"}}}
	for i := range 32 {
		id := "child-" + strconv.Itoa(i)
		cp.Invocations = append(cp.Invocations, Invocation[int]{ID: id, Status: InvocationJoined, GroupID: "wide"})
		cp.Groups[0].Branches = append(cp.Groups[0].Branches, ActivationBranch{InvocationID: id})
	}
	l := newLive(cp, nil, nil)
	for range 2 {
		before, after := l.workspace.prepare(l, "parent", false, "wide")
		if len(before.Invocations) != 33 || len(before.Groups) != 1 {
			t.Fatal("wide candidate lost records")
		}
		after.Invocations = after.Invocations[:1]
		after.Groups = nil
		change := l.workspace.change(before, after)
		if len(change.DeleteInvocations) != 32 || !reflect.DeepEqual(change.DeleteGroups, []string{"wide"}) || len(change.UpsertInvocations) != 1 {
			t.Fatalf("wide change = %+v", change)
		}
	}
}

func TestCandidateUpsertsChangedActivationBranchRecords(t *testing.T) {
	workspace := &candidateWorkspace[int]{}
	before := Checkpoint[int]{Groups: []ActivationGroup{{ID: "g1", Branches: []ActivationBranch{{InvocationID: "i1", Target: "worker"}}}}}

	unchanged := model.Copy(before)
	change := workspace.change(before, unchanged)
	if len(change.UpsertGroups) != 0 {
		t.Fatalf("unchanged group produced upserts: %+v", change.UpsertGroups)
	}

	after := model.Copy(before)
	after.Groups[0].Branches[0].Error = "worker failed"
	change = workspace.change(before, after)
	if len(change.UpsertGroups) != 1 || change.UpsertGroups[0].Branches[0].Error != "worker failed" {
		t.Fatalf("changed branch metadata was not upserted: %+v", change.UpsertGroups)
	}
	if before.Groups[0].Branches[0].Error != "" {
		t.Fatal("group change mutated the prior position")
	}

	wideBefore := Checkpoint[int]{Groups: make([]ActivationGroup, 8)}
	for i := range wideBefore.Groups {
		wideBefore.Groups[i].ID = "g" + strconv.Itoa(i+1)
	}
	wideAfter := model.Copy(wideBefore)
	wideAfter.Groups[3].Branches = []ActivationBranch{{InvocationID: "i4", Target: "worker"}}
	change = workspace.change(wideBefore, wideAfter)
	if len(change.UpsertGroups) != 1 || change.UpsertGroups[0].ID != "g4" {
		t.Fatalf("wide group metadata change was not upserted: %+v", change.UpsertGroups)
	}
}
