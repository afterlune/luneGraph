package modeltest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

func runNestedSequence(t testing.TB, kind string, data []byte, requireFault bool) {
	t.Helper()
	m, concurrency, capacity, ops := decodeNested(data)
	c := newController()
	store := openStore[nestedState](t, kind, m, cloneNested, c)
	limiter, err := graph.NewLimiter(capacity)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	makeRunner := func() *graph.Runner[nestedState] { return nestedRunner(t, m, c) }
	r := makeRunner()
	maxSteps := 128
	opts := graph.Options[nestedState]{Store: store, Observer: c, Limiter: limiter, MaxConcurrency: concurrency, MaxSteps: maxSteps}
	initial := nestedState{Owner: "model-run", Branch: -1, Values: map[string]int{"sum": 0, "phase": phaseRoot, "branch": -1}}
	out, err := r.Start(ctx, "model-run", initial, opts)
	if err != nil || out.Status != graph.StatusWaiting {
		t.Fatalf("nested seed: %+v %v", out, err)
	}
	cp := out.Checkpoint
	if err := m.check(cp); err != nil {
		t.Fatalf("initial checkpoint: %v", err)
	}
	fired := false
	replayRequirements := make(map[string]int)
	noReplayAfterWrite := make(map[string]int)
	call := func(i instruction) {
		t.Helper()
		before := cp
		callCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		c.arm(i, cancel)
		opts.MaxSteps = i.budget
		var inputs []graph.ResumeInput
		for _, inv := range cp.Invocations {
			if inv.Status == graph.InvocationWaiting {
				inputs = append(inputs, graph.ResumeInput{InvocationID: inv.ID, Payload: []byte("1")})
			}
		}
		got, callErr := makeRunner().Recover(callCtx, "model-run", inputs, opts)
		if callErr != nil && !errors.Is(callErr, graph.ErrConflict) && !errors.Is(callErr, errStore) && !errors.Is(callErr, graph.ErrInterrupted) && !errors.Is(callErr, context.Canceled) {
			t.Fatalf("nested operation %+v: %+v %v", i, got, callErr)
		}
		if err := c.check(capacity); err != nil {
			t.Fatal(err)
		}
		if err := m.check(got.Checkpoint); err != nil {
			t.Fatalf("returned nested checkpoint: %v", err)
		}
		loaded, err := store.Load(ctx, "model-run")
		if err != nil {
			t.Fatal(err)
		}
		if err := m.check(loaded); err != nil {
			t.Fatalf("authoritative nested checkpoint: %v", err)
		}
		confirmedNodes, confirmedInputs := c.confirmedCalls()
		if loaded.Steps != uint64(confirmedNodes) || loaded.Revision != 1+uint64(confirmedNodes+confirmedInputs) {
			t.Fatalf("nested oracle counters differ: steps/revision=%d/%d confirmed node/apply=%d/%d", loaded.Steps, loaded.Revision, confirmedNodes, confirmedInputs)
		}
		if loaded.Revision < before.Revision || loaded.Steps < before.Steps || got.Checkpoint.Revision > loaded.Revision || got.Checkpoint.Steps > loaded.Steps {
			t.Fatalf("nested checkpoint regressed/advanced: before=%+v returned=%+v stored=%+v", before, got.Checkpoint, loaded)
		}
		if loaded.Steps != store.steps || loaded.Revision != 1+store.steps+store.inputs {
			t.Fatalf("nested store counters differ: %+v steps=%d inputs=%d", loaded, store.steps, store.inputs)
		}
		wantStatus := graph.StatusFailed
		if errors.Is(callErr, graph.ErrInterrupted) {
			wantStatus = graph.StatusInterrupted
		}
		if errors.Is(callErr, context.Canceled) {
			wantStatus = graph.StatusCancelled
		}
		if callErr != nil && got.Status != wantStatus {
			t.Fatalf("nested error/status mismatch: %+v %v", got, callErr)
		}
		if callErr == nil && got.Status != graph.StatusWaiting && got.Status != graph.StatusBudget && got.Status != graph.StatusCompleted && got.Status != graph.StatusCompletedWithFailures {
			t.Fatalf("unexpected nested status: %+v", got)
		}
		if callErr == nil {
			a, cloneErr := got.Checkpoint.Clone(cloneNested)
			if cloneErr != nil {
				t.Fatal(cloneErr)
			}
			b, cloneErr := loaded.Clone(cloneNested)
			if cloneErr != nil {
				t.Fatal(cloneErr)
			}
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("acknowledged nested result differs from Store: returned=%+v stored=%+v", a, b)
			}
		}
		c.mu.Lock()
		didFire, operation := c.fired, c.operation
		mode := i.mode
		faultCallID := c.faultCallID
		faultCalls := append([]string(nil), c.faultCalls...)
		c.mu.Unlock()
		if didFire {
			want := graph.OutcomeDiscarded
			if mode == 2 || mode == 3 {
				want = graph.OutcomeUnknown
			}
			found := false
			c.mu.Lock()
			for key, outcome := range c.outcomes {
				if key.operation == operation && outcome == want {
					found = true
				}
			}
			c.mu.Unlock()
			if !found {
				t.Fatalf("nested injected mode %d did not resolve as %s", mode, want)
			}
			if mode >= 4 {
				if faultCallID == "" {
					t.Fatalf("nested callback fault mode %d had no CallID", mode)
				}
				replayRequirements[faultCallID] = 2
			} else {
				if len(faultCalls) == 0 {
					t.Fatalf("nested Store fault mode %d did not identify its submitted callback", mode)
				}
				c.mu.Lock()
				for _, callID := range faultCalls {
					if mode == 3 {
						noReplayAfterWrite[callID] = c.attempts[callID]
					} else {
						replayRequirements[callID] = 2
					}
				}
				c.mu.Unlock()
			}
			fired = true
			if kind == "sqlite" {
				store.reopen(t)
				reloaded, err := store.Load(ctx, "model-run")
				if err != nil {
					t.Fatal(err)
				}
				if err := m.check(reloaded); err != nil {
					t.Fatalf("nested SQLite reopen: %v", err)
				}
				if !reflect.DeepEqual(loaded, reloaded) {
					t.Fatal("nested SQLite reopen changed checkpoint")
				}
			}
		}
		for _, inv := range loaded.Invocations {
			inv.State.Values["speculative"] = 1
		}
		if loaded.Final != nil {
			loaded.Final.Values["speculative"] = 1
		}
		if err := m.check(got.Checkpoint); err != nil {
			t.Fatalf("returned checkpoint aliases Store state: %v", err)
		}
		cp, err = store.Load(ctx, "model-run")
		if err != nil {
			t.Fatal(err)
		}
		if err := m.check(cp); err != nil {
			t.Fatalf("nested Store alias: %v", err)
		}
	}
	for _, op := range ops {
		if cp.Completed {
			break
		}
		call(op)
	}
	if requireFault && !fired {
		t.Fatalf("nested fixed seed did not inject fault: %x", data)
	}
	for range 64 {
		if cp.Completed {
			break
		}
		call(instruction{budget: 8, target: 1})
	}
	if !cp.Completed {
		t.Fatal("nested recovery did not complete in 64 calls")
	}
	if err := m.check(cp); err != nil {
		t.Fatalf("nested terminal model: %v", err)
	}
	c.mu.Lock()
	for callID, minimum := range replayRequirements {
		if c.attempts[callID] < minimum {
			request := c.requests[callID]
			attempts, confirmed := c.attempts[callID], c.confirmed[callID]
			var resolutions []graph.CallbackOutcome
			discarded, unknown := false, false
			for key, outcome := range c.outcomes {
				if key.call == callID {
					resolutions = append(resolutions, outcome)
					discarded = discarded || outcome == graph.OutcomeDiscarded
					unknown = unknown || outcome == graph.OutcomeUnknown
				}
			}
			groupPrunedSibling := m.failure == 2 && request.role == "node" && request.node != "leaf0"
			if !confirmed && groupPrunedSibling && (discarded || unknown) {
				// FailGroup can permanently remove non-failing leaf callbacks in
				// the same activation while committing the leaf0 failure.
				continue
			}
			c.mu.Unlock()
			t.Fatalf("fault callback %s (%s %s) attempts=%d want at least %d confirmed=%t resolutions=%v required=%v", callID, request.role, request.node, attempts, minimum, confirmed, resolutions, replayRequirements)
		}
	}
	for callID, attempts := range noReplayAfterWrite {
		if c.attempts[callID] != attempts {
			c.mu.Unlock()
			t.Fatalf("callback %s replayed after its write acknowledgement was lost: attempts=%d before recovery=%d", callID, c.attempts[callID], attempts)
		}
	}
	c.mu.Unlock()
	before := len(c.requests)
	call(instruction{budget: 8, target: 1})
	if len(c.requests) != before {
		t.Fatal("completed nested recovery executed a callback")
	}
	if ctx.Err() != nil {
		t.Fatalf("nested sequence deadline: %v", ctx.Err())
	}
}

func TestNestedExecutionModelSequences(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		for width := byte(0); width < 2; width++ {
			for failure := byte(0); failure < 4; failure++ {
				for config := byte(0); config < 4; config++ {
					header := width | (config&1)<<1 | (config&2)<<1 | failure<<3
					data := []byte{header, 56, 0}
					t.Run(fmt.Sprintf("%s/width=%d/failure=%d/concurrency=%d/capacity=%d", kind, 2+2*width, failure, 1+3*int((config>>0)&1), 1+3*int((config>>1)&1)), func(t *testing.T) {
						runNestedSequence(t, kind, data, false)
					})
				}
			}
		}
	}
	for _, kind := range []string{"memory", "sqlite"} {
		for i, data := range nestedSeeds(false)[8:] {
			t.Run(fmt.Sprintf("%s/fault=%d", kind, i), func(t *testing.T) { runNestedSequence(t, kind, data, true) })
		}
	}
}

func FuzzNestedExecution(f *testing.F) {
	for _, seed := range nestedSeeds(true) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) { runNestedSequence(t, "memory", data, false) })
}
