package executor

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// Deliberately scans every child without using cached progress.
func fullReadyGroup(s Checkpoint[int]) int {
	for i, group := range s.Groups {
		complete := true
		for _, id := range group.Children {
			found := false
			for _, child := range s.Invocations {
				if child.ID == id {
					found = child.Status == InvocationJoined || child.Status == InvocationEnded || child.Status == InvocationFailed
					break
				}
			}
			if !found {
				complete = false
				break
			}
		}
		if complete {
			return i
		}
	}
	return -1
}

func TestGroupProgressCompletionOrder(t *testing.T) {
	for _, order := range []string{"forward", "reverse", "mixed"} {
		t.Run(order, func(t *testing.T) {
			var s Checkpoint[int]
			for g := range 5 {
				group := ActivationGroup{ID: fmt.Sprintf("g%d", g+1)}
				for j := range 16 {
					id := fmt.Sprintf("i%d", g*16+j+1)
					group.Children = append(group.Children, id)
					s.Invocations = append(s.Invocations, Invocation[int]{ID: id, Status: []InvocationStatus{InvocationWaiting, InvocationReady, InvocationGroup}[j%3]})
				}
				s.Groups = append(s.Groups, group)
			}
			index := newInvocationIndex(s)
			var progress groupProgress
			sequence := make([]int, len(s.Invocations))
			for i := range sequence {
				sequence[i] = i
			}
			if order == "reverse" {
				slices.Reverse(sequence)
			}
			if order == "mixed" {
				rng := rand.New(rand.NewPCG(41, 73))
				rng.Shuffle(len(sequence), func(i, j int) { sequence[i], sequence[j] = sequence[j], sequence[i] })
			}
			for step, position := range sequence {
				// Model the candidate buffer exchange without retaining pointers.
				s.Invocations = slices.Clone(s.Invocations)
				s.Groups = slices.Clone(s.Groups)
				s.Invocations[position].Status = []InvocationStatus{InvocationJoined, InvocationEnded, InvocationFailed}[step%3]
				if got, want := readyGroup(&s, &index, &progress), fullReadyGroup(s); got != want {
					t.Fatalf("step %d got %d want %d", step, got, want)
				}
			}
			// All eligible groups are still chosen in checkpoint order.
			for len(s.Groups) > 0 {
				if got := readyGroup(&s, &index, &progress); got != 0 {
					t.Fatalf("order = %d", got)
				}
				s.Groups = slices.Clone(s.Groups[1:])
			}
			if readyGroup(&s, &index, &progress) != -1 || progress.many != nil || progress.oneID != "" {
				t.Fatal("empty cache retained")
			}
		})
	}
}

func TestGroupProgressNestedCascade(t *testing.T) {
	s := Checkpoint[int]{
		Groups: []ActivationGroup{
			{ID: "g1", Children: []string{"i1", "i2"}},
			{ID: "g2", Children: []string{"i3", "i4"}},
		},
		Invocations: []Invocation[int]{
			{ID: "i1", Status: InvocationEnded},
			{ID: "i2", Status: InvocationGroup},
			{ID: "i3", Status: InvocationJoined},
			{ID: "i4", Status: InvocationFailed},
		},
	}
	index := newInvocationIndex(s)
	var progress groupProgress
	if readyGroup(&s, &index, &progress) != 1 || progress.order[0].prefix != 1 {
		t.Fatal("inner readiness")
	}
	removeInvocations(&index, &s, map[string]bool{"i3": true, "i4": true})
	s.Groups = s.Groups[:1]
	_, parent := indexedInvocation(&index, &s, "i2")
	parent.Status = InvocationJoined
	if readyGroup(&s, &index, &progress) != 0 || progress.onePrefix != 2 || progress.many != nil || progress.order != nil {
		t.Fatal("outer cascade or cache release")
	}
}

func TestGroupProgressReordersByActivationID(t *testing.T) {
	s := Checkpoint[int]{
		Groups:      []ActivationGroup{{ID: "g1", Children: []string{"i1", "i2"}}, {ID: "g2", Children: []string{"i3", "i4", "i5"}}},
		Invocations: []Invocation[int]{{ID: "i1", Status: InvocationEnded}, {ID: "i2", Status: InvocationWaiting}, {ID: "i3", Status: InvocationJoined}, {ID: "i4", Status: InvocationFailed}, {ID: "i5", Status: InvocationReady}},
	}
	index := newInvocationIndex(s)
	var progress groupProgress
	readyGroup(&s, &index, &progress)
	slices.Reverse(s.Groups)
	if readyGroup(&s, &index, &progress) != -1 || progress.order[0].prefix != 2 || progress.order[1].prefix != 1 {
		t.Fatal("prefix assigned to a different activation")
	}
	s.Invocations[4].Status = InvocationEnded
	if readyGroup(&s, &index, &progress) != 0 {
		t.Fatal("checkpoint group order ignored")
	}
}

func TestGroupProgressCompactsSurvivingPrefixes(t *testing.T) {
	var s Checkpoint[int]
	for i := range 4 {
		id := fmt.Sprintf("i%d", i+1)
		s.Groups = append(s.Groups, ActivationGroup{ID: fmt.Sprintf("g%d", i+1), Children: []string{id, "missing"}})
		s.Invocations = append(s.Invocations, Invocation[int]{ID: id, Status: InvocationEnded})
	}
	index := newInvocationIndex(s)
	var progress groupProgress
	readyGroup(&s, &index, &progress)
	s.Groups = []ActivationGroup{s.Groups[1], s.Groups[3]}
	if readyGroup(&s, &index, &progress) != -1 || len(progress.many) != 2 || len(progress.order) != 2 {
		t.Fatal("survivors not reconciled")
	}
	for i, cursor := range progress.order {
		if cursor.id != s.Groups[i].ID || cursor.prefix != 1 {
			t.Fatal("survivor prefix lost")
		}
	}
	for _, cursor := range progress.order[len(progress.order):cap(progress.order)] {
		if cursor.id != "" || cursor.prefix != 0 {
			t.Fatal("removed cursor retained")
		}
	}
	// Reordering after compaction must use the current prefixes, not an old map snapshot.
	slices.Reverse(s.Groups)
	readyGroup(&s, &index, &progress)
	if progress.order[0].id != "g4" || progress.order[0].prefix != 1 {
		t.Fatal("reordered survivor prefix lost")
	}
}

func TestGroupProgressPrefixAndMembership(t *testing.T) {
	s := Checkpoint[int]{Groups: []ActivationGroup{{ID: "g1", Children: []string{"i1", "i2", "i3"}}}, Invocations: []Invocation[int]{{ID: "i1", Status: InvocationJoined}, {ID: "i2", Status: InvocationWaiting}}}
	index := newInvocationIndex(s)
	var progress groupProgress
	if readyGroup(&s, &index, &progress) != -1 || progress.onePrefix != 1 || progress.many != nil {
		t.Fatal("single prefix")
	}
	s.Invocations[1].Status = InvocationEnded
	if readyGroup(&s, &index, &progress) != -1 || progress.onePrefix != 2 {
		t.Fatal("missing child must block")
	}
	appendInvocation(&index, &s, Invocation[int]{ID: "i3", Status: InvocationFailed})
	if readyGroup(&s, &index, &progress) != 0 || progress.onePrefix != 3 {
		t.Fatal("completed prefix")
	}
	s.Groups = append(s.Groups, ActivationGroup{ID: "g2", Children: []string{"i4"}})
	if readyGroup(&s, &index, &progress) != 0 || progress.order[0].prefix != 3 {
		t.Fatal("prefix lost on expansion")
	}
	s.Groups = []ActivationGroup{{ID: "g2", Children: []string{"i4"}}, {ID: "g3", Children: []string{"i5"}}}
	readyGroup(&s, &index, &progress)
	if _, exists := progress.many["g1"]; exists || len(progress.many) != 2 {
		t.Fatal("removed activation retained")
	}
	s.Groups = s.Groups[:1]
	readyGroup(&s, &index, &progress)
	if progress.many != nil || progress.oneID != "g2" || progress.onePrefix != 0 {
		t.Fatal("cache did not shrink")
	}
	// A later activation has a new ID and must start from zero.
	s.Groups[0] = ActivationGroup{ID: "g4", Children: []string{"i2", "i4"}}
	readyGroup(&s, &index, &progress)
	if progress.oneID != "g4" || progress.onePrefix != 1 {
		t.Fatal("new activation reused cursor")
	}
}
