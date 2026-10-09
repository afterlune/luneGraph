package observationtest

import (
	"context"
	"runtime"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/internal/limit"
)

func (f *failureFixture) diagnose(t *testing.T, phase string) {
	t.Helper()
	slots := limit.Slots(f.limiter)
	t.Logf("phase=%s limiter=%d/%d", phase, len(slots), cap(slots))
	events := f.log.snapshot()
	// Keep callback progress visible even when its event is outside the tail.
	progress := make(map[string]graph.EventPhase)
	for _, event := range events {
		if event.Node != "" && (event.Operation == graph.OperationNode || event.Operation == graph.OperationJoin) {
			progress[event.Node] = event.Phase
		}
	}
	for _, node := range []string{"deep", "nested", "a", "b", "blocked-a", "blocked-b", "innerjoin", "outerjoin"} {
		state := "not started (pending dispatch or admission)"
		switch progress[node] {
		case graph.PhaseStarted:
			state = "callback running"
		case graph.PhaseFinished:
			state = "callback finished; awaiting resolution"
		case graph.PhaseResolved:
			state = "callback resolved"
		}
		t.Logf("node=%s progress=%s", node, state)
	}
	if len(events) > 64 {
		events = events[len(events)-64:]
	}
	for _, event := range events {
		t.Logf("event: %+v", event)
	}
	buf := make([]byte, 256<<10)
	n := runtime.Stack(buf, true)
	t.Logf("goroutines (truncated=%t):\n%s", n == len(buf), buf[:n])
}

func (f *failureFixture) wait(t *testing.T, ctx context.Context, ch <-chan struct{}, phase string) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		f.diagnose(t, phase)
		t.Fatalf("%s: %v", phase, ctx.Err())
	}
}
