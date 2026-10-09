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

func decode(data []byte) (int, int, int, []instruction) {
	header := byte(0)
	if len(data) > 0 {
		header = data[0]
		data = data[1:]
	}
	width := 2
	if header&1 != 0 {
		width = 4
	}
	concurrency := 1
	if header&2 != 0 {
		concurrency = 4
	}
	capacity := 1
	if header&4 != 0 {
		capacity = 4
	}
	var ops []instruction
	for len(data) > 0 && len(ops) < 64 {
		a := data[0]
		b := byte(0)
		data = data[1:]
		if len(data) > 0 {
			b = data[0]
			data = data[1:]
		}
		ops = append(ops, instruction{mode: int(a % 8), target: 1 + int(b%4), budget: 1 + int((a/8)%8)})
	}
	return width, concurrency, capacity, ops
}

func runSequence(t testing.TB, parallel bool, kind string, data []byte, requireFault bool) {
	t.Helper()
	width, concurrency, capacity, ops := decode(data)
	if !parallel {
		width = 0
	}
	m := model{width: width}
	c := newController()
	r := runner(t, m, c)
	store := openStore(t, kind, m, c)
	limiter, err := graph.NewLimiter(capacity)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	opts := graph.Options[state]{Store: store, Observer: c, Limiter: limiter, MaxConcurrency: concurrency, MaxSteps: 8}
	out, err := r.Start(ctx, "model-run", state{Owner: "model-run", Branch: -1, Values: map[string]int{"sum": 0}}, opts)
	if err != nil || out.Status != graph.StatusWaiting {
		t.Fatalf("seed: %+v %v", out, err)
	}
	cp := out.Checkpoint
	fired := false
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
		got, callErr := r.Recover(callCtx, "model-run", inputs, opts)
		if callErr != nil && !errors.Is(callErr, graph.ErrConflict) && !errors.Is(callErr, errStore) && !errors.Is(callErr, graph.ErrInterrupted) && !errors.Is(callErr, context.Canceled) {
			t.Fatalf("operation %+v: %+v %v", i, got, callErr)
		}
		if err := c.check(capacity); err != nil {
			t.Fatal(err)
		}
		if err := m.check(got.Checkpoint); err != nil {
			t.Fatalf("returned checkpoint: %v", err)
		}
		wantStatus := graph.StatusFailed
		if errors.Is(callErr, graph.ErrInterrupted) {
			wantStatus = graph.StatusInterrupted
		}
		if errors.Is(callErr, context.Canceled) {
			wantStatus = graph.StatusCancelled
		}
		if callErr != nil && got.Status != wantStatus {
			t.Fatalf("error/status mismatch: %v %+v", callErr, got)
		}
		if callErr == nil && got.Status != graph.StatusWaiting && got.Status != graph.StatusBudget && got.Status != graph.StatusCompleted {
			t.Fatalf("unexpected successful status: %+v", got)
		}
		// The Store is authoritative even when the returned checkpoint is old.
		loaded, err := store.Load(ctx, "model-run")
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Revision < before.Revision || loaded.Steps < before.Steps {
			t.Fatal("authoritative counters regressed")
		}
		if got.Checkpoint.Revision > loaded.Revision || got.Checkpoint.Steps > loaded.Steps {
			t.Fatal("returned position ahead of authoritative Store")
		}
		if err := m.check(loaded); err != nil {
			t.Fatal(err)
		}
		if loaded.Steps != store.steps || loaded.Revision != 1+store.steps+store.inputs {
			t.Fatal("Store differs from confirmed write model")
		}
		// Clone normalizes nil versus empty public collections in both Stores.
		returned, cloneErr := got.Checkpoint.Clone(clone)
		if cloneErr != nil {
			t.Fatal(cloneErr)
		}
		storedCopy, cloneErr := loaded.Clone(clone)
		if cloneErr != nil {
			t.Fatal(cloneErr)
		}
		if callErr == nil && !reflect.DeepEqual(storedCopy, returned) {
			t.Fatalf("acknowledged result differs from Store: returned=%+v stored=%+v", got.Checkpoint, loaded)
		}
		c.mu.Lock()
		didFire := c.fired
		wantOutcome := graph.OutcomeDiscarded
		if i.mode == 2 || i.mode == 3 {
			wantOutcome = graph.OutcomeUnknown
		}
		foundOutcome := false
		for key, outcome := range c.outcomes {
			if key.operation == c.operation && outcome == wantOutcome {
				foundOutcome = true
			}
		}
		c.mu.Unlock()
		if didFire {
			wantErr := graph.ErrInterrupted
			switch i.mode {
			case 1:
				wantErr = graph.ErrConflict
			case 2, 3:
				wantErr = errStore
			case 7:
				wantErr = context.Canceled
			}
			if !errors.Is(callErr, wantErr) || !foundOutcome {
				t.Fatalf("injected mode=%d: error=%v want=%v outcome=%s found=%t", i.mode, callErr, wantErr, wantOutcome, foundOutcome)
			}
		}
		fired = fired || didFire
		if kind == "sqlite" && didFire {
			store.reopen(t)
			again, err := store.Load(ctx, "model-run")
			if err != nil || !reflect.DeepEqual(again, loaded) {
				t.Fatalf("reopen changed checkpoint: %v", err)
			}
		}
		// Loaded checkpoints must own their reference data independently.
		for _, inv := range loaded.Invocations {
			inv.State.Values["speculative"] = 1
		}
		if loaded.Final != nil {
			loaded.Final.Values["speculative"] = 1
		}
		cp, err = store.Load(ctx, "model-run")
		if err != nil {
			t.Fatal(err)
		}
		if err := m.check(cp); err != nil {
			t.Fatalf("Load alias: %v", err)
		}
	}
	for _, i := range ops {
		if cp.Completed {
			break
		}
		call(i)
	}
	if requireFault && !fired {
		t.Fatalf("seed did not inject its fault: %x", data)
	}
	for range 32 {
		if cp.Completed {
			break
		}
		call(instruction{budget: 8, target: 1})
	}
	if !cp.Completed {
		t.Fatal("normal recovery did not finish in 32 calls")
	}
	if err := m.check(cp); err != nil {
		t.Fatal(err)
	}
	// Completed recovery must not execute any callbacks.
	before := len(c.requests)
	call(instruction{budget: 8, target: 1})
	if len(c.requests) != before {
		t.Fatal("completed recovery executed a callback")
	}
	if ctx.Err() != nil {
		t.Fatal(fmt.Errorf("sequence deadline: %w", ctx.Err()))
	}
}
