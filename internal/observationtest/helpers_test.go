package observationtest

import (
	"context"
	"fmt"
	"sync"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint/memory"
)

type recorder struct {
	mu     sync.Mutex
	events []graph.Event
}

func (r *recorder) Observe(_ context.Context, event graph.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *recorder) snapshot() []graph.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]graph.Event(nil), r.events...)
}

func compile(t testing.TB, g *graph.Graph[int]) *graph.Runner[int] {
	t.Helper()
	r, err := g.Compile(graph.Config[int]{MachineID: "observation-v1", Clone: func(s int) (int, error) { return s, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func node(t testing.TB, g *graph.Graph[int], name string, run graph.Node[int]) {
	t.Helper()
	if err := g.AddNode(graph.NodeSpec[int]{Name: name, Run: run}); err != nil {
		t.Fatal(err)
	}
}

func edge(t testing.TB, g *graph.Graph[int], from, to string) {
	t.Helper()
	if err := g.AddEdge(from, to); err != nil {
		t.Fatal(err)
	}
}

func memoryStore(t testing.TB) *memory.Store[int] {
	t.Helper()
	s, err := memory.New[int](func(s int) (int, error) { return s, nil })
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func singleRunner(t testing.TB) *graph.Runner[int] {
	g := graph.New[int]("work")
	node(t, g, "work", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.EndExecution(s + 1), nil
	})
	return compile(t, g)
}

func finished(events []graph.Event, op graph.EventOperation) []graph.Event {
	var out []graph.Event
	for _, e := range events {
		if e.Operation == op && e.Phase == graph.PhaseFinished {
			out = append(out, e)
		}
	}
	return out
}

// checkPairs verifies causal ordering without requiring a global order between
// concurrent workers. Sequential Store spans can repeat the same key.
func checkPairs(t *testing.T, events []graph.Event) {
	t.Helper()
	open := make(map[string]graph.Event)
	for _, e := range events {
		if e.OperationID == 0 || e.Time.IsZero() || e.RunID == "" || e.MachineID != "observation-v1" {
			t.Fatalf("missing identity/time: %+v", e)
		}
		key := fmt.Sprintf("%d/%s/%s/%s", e.OperationID, e.Operation, e.InvocationID, e.CallID)
		if e.Phase == graph.PhaseStarted {
			if _, exists := open[key]; exists {
				t.Fatalf("duplicate start: %+v", e)
			}
			if e.Duration != 0 || e.Err != nil {
				t.Fatalf("start contains outcome: %+v", e)
			}
			open[key] = e
		} else if e.Phase == graph.PhaseFinished {
			start, exists := open[key]
			if !exists || e.Time.Before(start.Time) || e.Duration < 0 {
				t.Fatalf("unpaired finish: %+v", e)
			}
			delete(open, key)
		} else {
			t.Fatalf("invalid phase: %+v", e)
		}
	}
	if len(open) != 0 {
		t.Fatalf("unfinished spans: %+v", open)
	}
}

func checkCall(t *testing.T, events []graph.Event, operation graph.EventOperation, result graph.Result[int], err error) {
	t.Helper()
	checkPairs(t, events)
	if len(events) < 2 || events[0].Operation != operation || events[0].Phase != graph.PhaseStarted {
		t.Fatalf("first event: %+v", events)
	}
	last := events[len(events)-1]
	if last.Operation != operation || last.Phase != graph.PhaseFinished || last.Status != result.Status || last.Revision != result.Checkpoint.Revision || last.Err != err {
		t.Fatalf("last event = %+v, result = %+v, err = %v", last, result, err)
	}
	for _, e := range events {
		if e.OperationID != last.OperationID {
			t.Fatalf("mixed call IDs: %+v", events)
		}
	}
}
