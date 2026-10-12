package executor

import (
	"cmp"
	"slices"
)

type taskRef struct {
	id   string
	join bool
}

// readyQueue owns FIFO storage only. The live position decides task eligibility.
type readyQueue struct {
	items []taskRef
	head  int
}

func (q *readyQueue) len() int { return len(q.items) - q.head }

func (q *readyQueue) push(ref taskRef) {
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head >= 128 && q.head >= len(q.items)/2 {
		n := copy(q.items, q.items[q.head:])
		clear(q.items[n:])
		q.items = q.items[:n]
		q.head = 0
	}
	q.items = append(q.items, ref)
}

// pop consumes a reference from a non-empty queue and releases its slot.
func (q *readyQueue) pop() taskRef {
	ref := q.items[q.head]
	q.items[q.head] = taskRef{}
	q.head++
	return ref
}

func (q *readyQueue) clear() {
	clear(q.items)
	q.items = nil
	q.head = 0
}

// orderAfter initializes ready nodes in numeric ID order after the saved cursor.
// Joins are appended separately; subsequent admissions keep FIFO order.
func (q *readyQueue) orderAfter(cursor uint64) {
	slices.SortFunc(q.items, func(a, b taskRef) int { return cmp.Compare(invocationNumber(a.id), invocationNumber(b.id)) })
	at := 0
	for at < len(q.items) && invocationNumber(q.items[at].id) <= cursor {
		at++
	}
	reverseTasks(q.items[:at])
	reverseTasks(q.items[at:])
	reverseTasks(q.items)
}

func reverseTasks(tasks []taskRef) {
	for i, j := 0, len(tasks)-1; i < j; i, j = i+1, j-1 {
		tasks[i], tasks[j] = tasks[j], tasks[i]
	}
}
