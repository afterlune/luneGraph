package modeltest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	graph "github.com/afterlune/luneGraph"
)

type instruction struct {
	mode, target, budget int
	join                 string
}
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
	attempts     map[string]int
	faultCallID  string
	faultCalls   []string
	writes       map[uint64]bool
	events       map[eventKey]counts
	active, peak int
	problem      error
	outcomes     map[eventKey]graph.CallbackOutcome
	operation    uint64
}

func newController() *controller {
	return &controller{requests: map[string]request{}, confirmed: map[string]bool{}, attempts: map[string]int{}, writes: map[uint64]bool{}, events: map[eventKey]counts{}, outcomes: map[eventKey]graph.CallbackOutcome{}}
}

func (c *controller) arm(i instruction, cancel context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = i
	c.cancel = cancel
	c.seen = 0
	c.fired = false
	c.faultCallID = ""
	c.faultCalls = nil
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

// triggerJoinConflictLocked selects a persisted activation by topology, not by
// commit order. The nested sequence uses this to target only its outer join.
func (c *controller) triggerJoinConflictLocked(join string, deleted []string, groups []graph.ActivationGroup) bool {
	if c.fired || c.current.mode != 1 || c.current.join == "" || c.current.join != join {
		return false
	}
	for _, deletedID := range deleted {
		for _, group := range groups {
			if group.ID == deletedID && group.JoinNode == join {
				c.fired = true
				return true
			}
		}
	}
	return false
}

func (c *controller) attemptCounts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := make(map[string]int, len(c.attempts))
	for callID, count := range c.attempts {
		counts[callID] = count
	}
	return counts
}

func (c *controller) callbackCount(role, node string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for callID, request := range c.requests {
		if request.role == role && request.node == node {
			count += c.attempts[callID]
		}
	}
	return count
}

func (c *controller) requestCounts() map[request]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := make(map[request]int, len(c.requests))
	for callID, req := range c.requests {
		counts[req] += c.attempts[callID]
	}
	return counts
}

func (c *controller) callbackRequest(callID string) (request, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	req, ok := c.requests[callID]
	return req, ok
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
	c.attempts[call.CallID]++
	if prior, ok := c.requests[call.CallID]; ok && prior != req {
		c.problem = fmt.Errorf("replay request changed for %s: %+v -> %+v", call.CallID, prior, req)
	}
	if c.confirmed[call.CallID] {
		c.problem = fmt.Errorf("confirmed callback replayed: %s", call.CallID)
	}
	c.requests[call.CallID] = req
	mode := map[string]int{"node": 4, "join": 5, "apply": 6}[role]
	if c.triggerLocked(mode) {
		c.faultCallID = call.CallID
		return graph.Interrupt(nil)
	}
	if c.triggerLocked(7) {
		c.faultCallID = call.CallID
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

func (c *controller) confirmedCalls() (nodes, applies int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for callID, request := range c.requests {
		if !c.confirmed[callID] {
			continue
		}
		switch request.role {
		case "node":
			nodes++
		case "apply", "join":
			applies++
		}
	}
	return nodes, applies
}
