package modeltest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	graph "github.com/afterlune/luneGraph"
)

type instruction struct{ mode, target, budget int }
type eventKey struct {
	operation uint64
	role      graph.EventOperation
	call      string
}
type counts struct{ started, finished, resolved int }
type request struct{ role, node, body string }

type controller struct {
	mu           sync.Mutex
	current      instruction
	seen         int
	fired        bool
	cancel       context.CancelFunc
	requests     map[string]request
	confirmed    map[string]bool
	writes       map[uint64]bool
	events       map[eventKey]counts
	active, peak int
	problem      error
	outcomes     map[eventKey]graph.CallbackOutcome
	operation    uint64
}

func newController() *controller {
	return &controller{requests: map[string]request{}, confirmed: map[string]bool{}, writes: map[uint64]bool{}, events: map[eventKey]counts{}, outcomes: map[eventKey]graph.CallbackOutcome{}}
}

func (c *controller) arm(i instruction, cancel context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = i
	c.cancel = cancel
	c.seen = 0
	c.fired = false
}

func (c *controller) triggerLocked(mode int) bool {
	if c.fired || c.current.mode != mode {
		return false
	}
	c.seen++
	if c.seen != c.current.target {
		return false
	}
	c.fired = true
	return true
}

func (c *controller) attempt(_ context.Context, call graph.CallInfo, role string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if call.RunID != "model-run" || call.CallID == "" || call.InvocationID == "" {
		c.problem = fmt.Errorf("invalid callback identity: %+v", call)
	}
	req := request{role, call.Node, string(data)}
	if prior, ok := c.requests[call.CallID]; ok && prior != req {
		c.problem = fmt.Errorf("replay request changed for %s: %+v -> %+v", call.CallID, prior, req)
	}
	if c.confirmed[call.CallID] {
		c.problem = fmt.Errorf("confirmed callback replayed: %s", call.CallID)
	}
	c.requests[call.CallID] = req
	mode := map[string]int{"node": 4, "join": 5, "apply": 6}[role]
	if c.triggerLocked(mode) {
		return graph.Interrupt(nil)
	}
	if c.triggerLocked(7) {
		c.cancel()
		return context.Canceled
	}
	return nil
}

func (c *controller) Observe(_ context.Context, e graph.Event) {
	if e.Operation == graph.OperationRecover && e.Phase == graph.PhaseStarted {
		c.mu.Lock()
		c.operation = e.OperationID
		c.mu.Unlock()
	}
	if e.Operation != graph.OperationNode && e.Operation != graph.OperationJoin && e.Operation != graph.OperationApply {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := eventKey{e.OperationID, e.Operation, e.CallID}
	n := c.events[key]
	switch e.Phase {
	case graph.PhaseStarted:
		n.started++
		c.active++
		if c.active > c.peak {
			c.peak = c.active
		}
	case graph.PhaseFinished:
		n.finished++
		c.active--
	case graph.PhaseResolved:
		n.resolved++
		c.outcomes[key] = e.Outcome
		switch e.Outcome {
		case graph.OutcomeCommitted:
			c.confirmed[e.CallID] = true
		case graph.OutcomeUnknown:
			if c.writes[e.Revision] {
				c.confirmed[e.CallID] = true
			}
		case graph.OutcomeDiscarded:
		default:
			c.problem = fmt.Errorf("invalid resolution: %+v", e)
		}
	default:
		c.problem = fmt.Errorf("invalid callback phase: %+v", e)
	}
	c.events[key] = n
}

func (c *controller) check(capacity int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.problem != nil {
		return c.problem
	}
	if c.active != 0 || c.peak > capacity {
		return fmt.Errorf("active=%d peak=%d capacity=%d", c.active, c.peak, capacity)
	}
	for key, n := range c.events {
		if n != (counts{1, 1, 1}) {
			return fmt.Errorf("callback not drained/resolved: %+v %+v", key, n)
		}
	}
	return nil
}
