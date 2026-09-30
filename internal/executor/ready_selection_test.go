package executor

import (
	"context"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

// This oracle sorts only eligible IDs, independently of the production index
// and of the order in which invocations occupy checkpoint storage.
func referenceReady(s Checkpoint[int], running map[string]context.CancelFunc, cursor uint64) string {
	var numbers []uint64
	for _, inv := range s.Invocations {
		_, active := running[inv.ID]
		if inv.Status != InvocationReady || active {
			continue
		}
		number, _ := strconv.ParseUint(inv.ID[1:], 10, 64)
		numbers = append(numbers, number)
	}
	slices.Sort(numbers)
	for _, number := range numbers {
		if number > cursor {
			return "i" + strconv.FormatUint(number, 10)
		}
	}
	if len(numbers) != 0 {
		return "i" + strconv.FormatUint(numbers[0], 10)
	}
	return ""
}

func assertSelection(t *testing.T, s Checkpoint[int], index *invocationIndex, running map[string]context.CancelFunc, cursor uint64) {
	t.Helper()
	want := referenceReady(s, running, cursor)
	inv, number := selectReady(s, index, running, cursor)
	if want == "" {
		if inv != nil || number != 0 {
			t.Fatalf("cursor %d: expected no work, got %+v/%d", cursor, inv, number)
		}
		return
	}
	position, current := indexedInvocation(index, &s, want)
	if inv == nil || inv.ID != want || position < 0 || inv != current || inv != &s.Invocations[position] || number != invocationNumber(want) {
		t.Fatalf("cursor %d: want %s in current buffer, got %+v/%d", cursor, want, inv, number)
	}
}

func TestSelectionOrderingAndCurrentState(t *testing.T) {
	s := Checkpoint[int]{Invocations: []Invocation[int]{
		{ID: "i10", Status: InvocationReady}, {ID: "i2", Status: InvocationReady},
		{ID: "i9", Status: InvocationReady}, {ID: "i4", Status: InvocationWaiting},
		{ID: "i5", Status: InvocationGroup}, {ID: "i6", Status: InvocationJoined},
		{ID: "i7", Status: InvocationEnded}, {ID: "i8", Status: InvocationFailed},
		{ID: "i18446744073709551614", Status: InvocationReady},
	}}
	index := newInvocationIndex(s)
	running := map[string]context.CancelFunc{"i9": func() {}}
	for _, cursor := range []uint64{0, 2, 3, 9, 10, math.MaxUint64 - 1, math.MaxUint64} {
		assertSelection(t, s, &index, running, cursor)
	}
	running["i2"] = nil // Membership, rather than the cancel value, means active.
	assertSelection(t, s, &index, running, 0)
	// Status changes must be read from the current checkpoint, not metadata.
	s.Invocations[0].Status = InvocationWaiting
	s.Invocations[3].Status = InvocationReady
	assertSelection(t, s, &index, running, 2)
	for i := range s.Invocations {
		s.Invocations[i].Status = InvocationEnded
	}
	assertSelection(t, s, &index, running, 2)
	assertSelection(t, Checkpoint[int]{}, nil, nil, 0)
}

func TestSelectionAcrossMixedTopologyChanges(t *testing.T) {
	random := rand.New(rand.NewPCG(27, 41))
	statuses := []InvocationStatus{InvocationReady, InvocationWaiting, InvocationGroup, InvocationJoined, InvocationEnded, InvocationFailed}
	for scenario := range 128 {
		s := Checkpoint[int]{}
		next := uint64(1)
		for range random.IntN(48) {
			s.Invocations = append(s.Invocations, Invocation[int]{ID: "i" + strconv.FormatUint(next, 10)})
			next += uint64(1 + random.IntN(5))
		}
		random.Shuffle(len(s.Invocations), func(i, j int) { s.Invocations[i], s.Invocations[j] = s.Invocations[j], s.Invocations[i] })
		index := newInvocationIndex(s)
		var spare Checkpoint[int]
		for step := range 8 {
			running := map[string]context.CancelFunc{}
			for i := range s.Invocations {
				s.Invocations[i].State = scenario*1000 + step*100 + i
				s.Invocations[i].Status = statuses[random.IntN(len(statuses))]
				if random.IntN(4) == 0 {
					running[s.Invocations[i].ID] = func() {}
				}
			}
			for _, cursor := range []uint64{0, random.Uint64N(next), next - 1, math.MaxUint64} {
				assertSelection(t, s, &index, running, cursor)
			}
			candidate := copyCheckpointInto(&spare, s)
			spare, s = s, candidate
			assertSelection(t, s, &index, running, random.Uint64N(next))
			remove := map[string]bool{}
			for _, inv := range s.Invocations {
				if random.IntN(3) == 0 {
					remove[inv.ID] = true
				}
			}
			removeInvocations(&index, &s, remove)
			for range random.IntN(12) {
				appendInvocation(&index, &s, Invocation[int]{ID: "i" + strconv.FormatUint(next, 10), Status: InvocationReady})
				next++
			}
			assertSelection(t, s, &index, running, random.Uint64N(next))
			for position, inv := range s.Invocations {
				got, current := indexedInvocation(&index, &s, inv.ID)
				if got != position || current != &s.Invocations[position] {
					t.Fatalf("scenario %d/%d: lookup %s lost its position", scenario, step, inv.ID)
				}
			}
		}
	}
}

func TestOrderStorageClearsRemovedIDs(t *testing.T) {
	s := Checkpoint[int]{}
	var index invocationIndex
	for _, number := range []int{11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1} {
		appendInvocation(&index, &s, Invocation[int]{ID: "i" + strconv.Itoa(number), Status: InvocationReady})
	}
	assertSelection(t, s, &index, nil, 2)
	storage := index.order
	removeInvocations(&index, &s, map[string]bool{"i2": true, "i3": true})
	if len(index.order) != 9 || storage[9] != (invocationOrder{}) || storage[10] != (invocationOrder{}) {
		t.Fatalf("removed metadata retained: %+v", storage)
	}
	assertSelection(t, s, &index, nil, 3)
	removeInvocations(&index, &s, map[string]bool{"i4": true})
	if index.positions != nil || index.order != nil {
		t.Fatal("small set retained an index")
	}
	for _, entry := range storage {
		if entry != (invocationOrder{}) {
			t.Fatalf("released metadata retained an ID: %+v", entry)
		}
	}
}
