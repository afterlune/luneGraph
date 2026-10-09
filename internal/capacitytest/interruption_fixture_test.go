package capacitytest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type interruptionKey struct{ run, call string }
type interruptionAttempt struct {
	role   string
	source state
}

// Only pending identities are retained. Committed callbacks are removed after
// each public call; this controller is application readiness, not a run lease.
type interruptionController struct {
	mu      sync.Mutex
	pending map[interruptionKey]interruptionAttempt
	running map[string]int
	counts  [3]uint64
	active  atomic.Int32
	allow   bool
}

func (c *interruptionController) begin(run string) {
	c.active.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running == nil {
		c.running = make(map[string]int)
	}
	c.running[run]++
}

func (c *interruptionController) finish(run string) {
	c.mu.Lock()
	if c.running[run]--; c.running[run] == 0 {
		delete(c.running, run)
	}
	c.mu.Unlock()
	c.active.Add(-1)
}

func (c *interruptionController) quiescent(run string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running[run] == 0
}

func (c *interruptionController) attempt(call graph.CallInfo, role string, s state) error {
	if err := checkIdentity(call, s); err != nil {
		return err
	}
	if _, exists := s.Values["uncommitted"]; exists {
		return errors.New("uncommitted mutation reached callback")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.allow {
		return nil
	}
	if c.pending == nil {
		c.pending = make(map[interruptionKey]interruptionAttempt)
	}
	key := interruptionKey{call.RunID, call.CallID}
	if prior, ok := c.pending[key]; ok {
		if prior.role != role || !reflect.DeepEqual(prior.source, s) {
			return fmt.Errorf("replay changed request for %+v: %+v -> %+v", key, prior.source, s)
		}
		return nil
	}
	source, _ := clone(s)
	c.pending[key] = interruptionAttempt{role, source}
	switch role {
	case "node":
		c.counts[0]++
	case "join":
		c.counts[1]++
	case "apply":
		c.counts[2]++
	}
	return graph.Interrupt(nil)
}

func pendingCalls(cp graph.Checkpoint[state]) map[string]bool {
	ids := make(map[string]bool)
	for _, inv := range cp.Invocations {
		if inv.CallID != "" {
			ids[inv.CallID] = true
		}
	}
	for _, group := range cp.Groups {
		if group.CallID != "" {
			ids[group.CallID] = true
		}
	}
	return ids
}

func (c *interruptionController) reconcile(cp graph.Checkpoint[state]) int {
	current := pendingCalls(cp)
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for key := range c.pending {
		if key.run != cp.RunID {
			continue
		}
		if !current[key.call] {
			delete(c.pending, key)
		} else {
			count++
		}
	}
	return count
}

func interruptionRunner(t testing.TB, width int, c *interruptionController) *graph.Runner[state] {
	t.Helper()
	g := graph.New[state]("fork")
	targets := make([]string, width)
	for i := range width {
		targets[i] = "b" + strconv.Itoa(i)
	}
	addNode(t, g, "fork", func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
		c.begin(call.RunID)
		defer c.finish(call.RunID)
		if err := c.attempt(call, "node", s); err != nil {
			s.Values["uncommitted"] = 99
			return graph.EndExecution(s), err
		}
		return graph.To(s, targets...), nil
	})
	for i, name := range targets {
		addNode(t, g, name, func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
			c.begin(call.RunID)
			defer c.finish(call.RunID)
			if err := c.attempt(call, "node", s); err != nil {
				s.Values["uncommitted"] = 99
				return graph.To(s, "join"), err
			}
			s.Values["branch"] = i
			return graph.To(s, "join"), nil
		})
		addEdge(t, g, "fork", name)
	}
	if err := g.AddJoin(graph.JoinSpec[state]{Name: "join", From: "fork", OnError: graph.FailExecution, Merge: func(_ context.Context, call graph.CallInfo, values []state) (state, error) {
		c.begin(call.RunID)
		defer c.finish(call.RunID)
		if len(values) != width {
			return state{}, fmt.Errorf("join width=%d, want %d", len(values), width)
		}
		s := values[0]
		for i, v := range values {
			if v.Owner != s.Owner || v.Round != s.Round || v.Values["branch"] != i {
				return state{}, fmt.Errorf("mixed join state: %+v", v)
			}
		}
		if err := c.attempt(call, "join", s); err != nil {
			s.Values["uncommitted"] = 99
			return s, err
		}
		s.Round++
		s.Total = s.Round * width
		s.Values["sum"] = s.Round * width * (width + 1) / 2
		s.Values["branch"] = -1
		return s, nil
	}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range targets {
		addEdge(t, g, name, "join")
	}
	addNode(t, g, "boundary", func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
		c.begin(call.RunID)
		defer c.finish(call.RunID)
		return graph.Wait(s, "advance", "fork"), nil
	})
	addEdge(t, g, "join", "boundary")
	addEdge(t, g, "boundary", "fork")
	if err := graph.RegisterJSONContinuation(g, "advance", func(_ context.Context, call graph.CallInfo, s state, input int) (state, error) {
		c.begin(call.RunID)
		defer c.finish(call.RunID)
		if input != 1 {
			return s, fmt.Errorf("invalid input %d", input)
		}
		if err := c.attempt(call, "apply", s); err != nil {
			s.Values["uncommitted"] = 99
			return s, err
		}
		s.Values["inputs"]++
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
	return compile(t, g)
}
