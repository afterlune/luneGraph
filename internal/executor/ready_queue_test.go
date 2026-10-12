package executor

import (
	"context"
	"strconv"
	"testing"
)

func TestReadyQueueConsumptionAndCompaction(t *testing.T) {
	var q readyQueue
	for i := range 300 {
		q.push(taskRef{id: strconv.Itoa(i)})
	}
	for i := range 160 {
		if got := q.pop(); got.id != strconv.Itoa(i) {
			t.Fatalf("pop %d = %+v", i, got)
		}
	}
	for _, ref := range q.items[:q.head] {
		if ref != (taskRef{}) {
			t.Fatal("consumed slot retains a task", ref)
		}
	}
	q.push(taskRef{id: "tail"})
	if q.head != 0 || q.len() != 141 {
		t.Fatalf("compaction head=%d remaining=%d", q.head, q.len())
	}
	for i := 160; i < 300; i++ {
		if got := q.pop(); got.id != strconv.Itoa(i) {
			t.Fatalf("compaction changed FIFO: %+v", got)
		}
	}
	tail := q.pop()
	q.push(tail)
	if q.len() != 1 || q.pop() != tail {
		t.Fatal("re-enqueue after full consumption lost the task")
	}
	q.push(taskRef{id: "last"})
	backing := q.items
	q.clear()
	if q.len() != 0 || q.items != nil || q.head != 0 || backing[0] != (taskRef{}) {
		t.Fatal("clear retained queue storage")
	}
	q.push(tail)
	if q.pop() != tail {
		t.Fatal("cleared queue cannot be reused")
	}
}

func TestReadyQueueCursorOrder(t *testing.T) {
	for _, test := range []struct {
		cursor uint64
		want   []string
	}{
		{0, []string{"i2", "i7", "i11"}},
		{7, []string{"i11", "i2", "i7"}},
		{99, []string{"i2", "i7", "i11"}},
	} {
		var q readyQueue
		for _, id := range []string{"i11", "i2", "i7"} {
			q.push(taskRef{id: id})
		}
		q.orderAfter(test.cursor)
		for _, id := range test.want {
			if got := q.pop(); got.id != id {
				t.Fatalf("cursor %d: got %s want %s", test.cursor, got.id, id)
			}
		}
	}
	var empty readyQueue
	empty.orderAfter(1)
}

func TestLiveNextFiltersTasksAndDrainsJoinsAtBudget(t *testing.T) {
	l := newLive(Checkpoint[int]{Invocations: []Invocation[int]{
		{ID: "i1", Status: InvocationReady},
		{ID: "i2", Status: InvocationWaiting},
		{ID: "i3", Status: InvocationReady},
		{ID: "i9", Status: InvocationGroup, ChildGroupID: "g11"},
		{ID: "i10", Status: InvocationJoined, GroupID: "g11"},
	}, Groups: []ActivationGroup{{ID: "g11", ParentID: "i9", Branches: []ActivationBranch{{InvocationID: "i10"}}}}}, nil)
	l.ready.clear()
	for _, ref := range []taskRef{{id: "missing"}, {id: "i2"}, {id: "i3"}, {id: "i1"}, {id: "g11", join: true}} {
		l.enqueue(ref)
	}
	running := map[taskRef]context.CancelFunc{{id: "i3"}: func() {}}
	if ref, ok := l.next(false, running); !ok || ref != (taskRef{id: "g11", join: true}) {
		t.Fatalf("budget did not drain join: %+v %v", ref, ok)
	}
	if ref, ok := l.next(false, running); ok {
		t.Fatalf("budget admitted a node: %+v", ref)
	}
	if ref, ok := l.next(true, running); !ok || ref.id != "i1" {
		t.Fatalf("deferred node lost: %+v %v", ref, ok)
	}
	if ref, ok := l.next(true, running); ok {
		t.Fatalf("invalid/busy task admitted: %+v", ref)
	}
}
