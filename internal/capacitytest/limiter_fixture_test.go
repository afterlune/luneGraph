package capacitytest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

// The observer measures only callback lifetimes; resolved events are separate.
type limiterTracker struct {
	active, peak atomic.Int32
	starts       [3]atomic.Uint64
	committed    atomic.Uint64
}

func (m *limiterTracker) Observe(_ context.Context, e graph.Event) {
	var role int
	switch e.Operation {
	case graph.OperationNode:
		role = 0
	case graph.OperationJoin:
		role = 1
	case graph.OperationApply:
		role = 2
	default:
		return
	}
	if e.Phase == graph.PhaseStarted {
		m.starts[role].Add(1)
		n := m.active.Add(1)
		for old := m.peak.Load(); n > old && !m.peak.CompareAndSwap(old, n); old = m.peak.Load() {
		}
	} else if e.Phase == graph.PhaseFinished {
		m.active.Add(-1)
	} else if e.Phase == graph.PhaseResolved && e.Outcome == graph.OutcomeCommitted {
		m.committed.Add(1)
	}
}

type limiterPopulation struct {
	runner  *graph.Runner[state]
	store   graph.Store[state]
	opts    graph.Options[state]
	slots   []interruptionSlot
	tracker limiterTracker
}

func newLimiterPopulation(t testing.TB, kind string, capacity int) *limiterPopulation {
	t.Helper()
	l, err := graph.NewLimiter(capacity)
	if err != nil {
		t.Fatal(err)
	}
	p := &limiterPopulation{runner: fanoutRunner(t, 8, 0, 1), store: openStore(t, kind), slots: make([]interruptionSlot, 64)}
	p.opts = graph.Options[state]{Store: p.store, Limiter: l, MaxConcurrency: 8, MaxSteps: 10, Observer: &p.tracker}
	for i := range p.slots {
		id := fmt.Sprintf("limited-%d", i)
		out, err := p.runner.Start(context.Background(), id, initial(id), p.opts)
		if err != nil || out.Status != graph.StatusWaiting {
			t.Fatalf("seed=%+v %v", out, err)
		}
		p.slots[i].id = id
		p.slots[i].remember(out.Checkpoint, true)
	}
	return p
}

func (p *limiterPopulation) advance(ctx context.Context, i int) (graph.Result[state], error) {
	s := &p.slots[i]
	before := s.checkpoint
	out, err := p.runner.Recover(ctx, s.id, interruptionInputs(before), p.opts)
	if err != nil {
		return out, err
	}
	v, stateErr := rootState(out.Checkpoint)
	if stateErr != nil {
		return out, stateErr
	}
	if out.Status != graph.StatusWaiting || v.Owner != s.id || v.Round != int(before.Steps/10)+1 || v.Total != v.Round*8 || v.Values["sum"] != v.Round*36 || v.Values["inputs"] != v.Round-1 || out.Checkpoint.Steps != uint64(v.Round*10) || out.Checkpoint.Revision != uint64(v.Round*11) {
		return out, fmt.Errorf("invalid limited round: %+v", out)
	}
	s.remember(out.Checkpoint, true)
	return out, nil
}

func (p *limiterPopulation) verify(t testing.TB, capacity int) {
	t.Helper()
	if p.tracker.active.Load() != 0 || p.tracker.peak.Load() > int32(capacity) || p.tracker.starts[2].Load() == 0 {
		t.Fatalf("callback budget: active=%d peak=%d apply=%d", p.tracker.active.Load(), p.tracker.peak.Load(), p.tracker.starts[2].Load())
	}
	var callbacks uint64
	for i := range p.tracker.starts {
		callbacks += p.tracker.starts[i].Load()
	}
	if p.tracker.committed.Load() != callbacks {
		t.Fatalf("unresolved callbacks: committed=%d started=%d", p.tracker.committed.Load(), callbacks)
	}
	for _, s := range p.slots {
		cp, err := p.store.Load(context.Background(), s.id)
		if err != nil {
			t.Fatal(err)
		}
		v, stateErr := rootState(cp)
		if stateErr != nil || v.Owner != s.id || cp.Steps != s.checkpoint.Steps || cp.Revision != s.checkpoint.Revision || len(cp.Groups) != 0 || len(cp.Invocations) != 1 || v.Total != v.Round*8 {
			t.Fatalf("stored round: %+v %v", cp, stateErr)
		}
	}
}

func TestCapacitySharedLimiter(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, capacity := range []int{1, 8} {
			t.Run(fmt.Sprintf("%s/capacity=%d", kind, capacity), func(t *testing.T) {
				p := newLimiterPopulation(t, kind, capacity)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				pool := newBatchPool(64, 8, func(i int) error { _, err := p.advance(ctx, i); return err })
				defer pool.close()
				for range 2 {
					if err := pool.batch(); err != nil {
						t.Fatal(err)
					}
				}
				p.verify(t, capacity)
			})
		}
	}
}
