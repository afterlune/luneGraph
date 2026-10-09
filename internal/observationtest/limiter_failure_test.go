package observationtest

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
	"github.com/afterlune/luneGraph/internal/limit"
)

func failureStore(t *testing.T, kind string) (graph.Store[int], func() graph.Store[int]) {
	t.Helper()
	if kind == "memory" {
		return memoryStore(t), nil
	}
	path := filepath.Join(t.TempDir(), "failure.db")
	open := func() *sqlite.Store[int] {
		t.Helper()
		store, err := sqlite.Open(context.Background(), path, checkpoint.JSON[int]{})
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store := open()
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	return store, func() graph.Store[int] {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		store = nil
		store = open()
		return store
	}
}

func failureResolutions(events []graph.Event) map[string]graph.Event {
	out := map[string]graph.Event{}
	for _, e := range events {
		if e.Phase == graph.PhaseResolved {
			out[e.Node] = e
		}
	}
	return out
}

func TestLimiterNestedFailGroupProgress(t *testing.T) {
	for _, origin := range []string{"node", "join"} {
		for _, kind := range []string{"memory", "sqlite"} {
			modes := []string{"success", "cancel"}
			if kind == "memory" {
				modes = append(modes, "conflict", "unknown without write", "unknown after write")
			}
			for _, mode := range modes {
				orders := []string{"deep-first"}
				if origin == "join" {
					orders = append(orders, "nested-first")
				}
				for _, order := range orders {
					t.Run(origin+"/"+kind+"/"+mode+"/"+order, func(t *testing.T) {
						store, reopen := failureStore(t, kind)
						f := newFailureFixture(t, origin, order, store)
						var fault error
						write := mode == "unknown after write"
						wantOutcome := graph.OutcomeCommitted
						candidateRevision := uint64(6)
						trigger := "failer"
						if origin == "join" {
							candidateRevision, trigger = 8, "b"
						}
						switch mode {
						case "conflict":
							fault, wantOutcome = graph.ErrConflict, graph.OutcomeDiscarded
						case "unknown without write", "unknown after write":
							fault, wantOutcome = errors.New("uncertain group commit"), graph.OutcomeUnknown
						case "cancel":
							wantOutcome = graph.OutcomeDiscarded
						}
						if fault != nil {
							f.store = &faultStore{Store: store, fault: fault, write: write, revision: candidateRevision}
						}
						type answer struct {
							result graph.Result[int]
							err    error
						}
						done := make(chan answer, 1)
						opts := graph.Options[int]{Store: f.store, Limiter: f.limiter, MaxConcurrency: 4, Observer: f.observer}
						f.goRun(func() {
							out, err := f.runner.Recover(f.ctx, "target", nil, opts)
							done <- answer{out, err}
						})
						f.wait(t, f.ctx, f.cloneReached, "outer join input clone did not start")
						// All four permits are now occupied. Cancellation must reach both
						// deleted descendants before the outer join or commit can finish.
						if limit.TryAcquire(f.limiter) {
							limit.Release(f.limiter)
							t.Fatal("fixture did not exhaust the shared budget")
						}
						f.allowClone()
						f.wait(t, f.ctx, f.blockedCancelled, "pruned callback cancellation blocked behind outer join")
						if mode == "cancel" {
							f.cancel()
						}
						f.releaseBlocked()
						var first answer
						select {
						case first = <-done:
						case <-time.After(5 * time.Second):
							f.diagnose(t, "draining target")
							t.Fatal("target did not drain")
						}
						events := f.log.snapshot()
						checkCall(t, events, graph.OperationRecover, first.result, first.err)
						if f.holderCtx.Err() != nil {
							t.Fatal("target cancelled independent execution")
						}
						select {
						case err := <-f.holderDone:
							t.Fatalf("independent holder exited: %v", err)
						default:
						}
						seen := failureResolutions(events)
						if seen[trigger].CallID == "" {
							t.Fatal("callback identity missing")
						}
						for _, inv := range f.seed.Invocations {
							if inv.Node == trigger && seen[inv.Node].CallID != inv.CallID {
								t.Fatalf("persisted callback identity changed: %+v", seen[inv.Node])
							}
						}
						for _, group := range f.seed.Groups {
							if group.JoinNode == "outerjoin" && mode != "cancel" && seen["outerjoin"].CallID != group.CallID {
								t.Fatal("outer join identity changed")
							}
						}
						for _, name := range []string{"blocked-a", "blocked-b"} {
							if seen[name].CallID == "" || seen[name].Outcome != graph.OutcomeDiscarded || seen[name].Action != graph.ActionEndExecution {
								t.Fatalf("cancelled callback accepted: %+v", seen[name])
							}
						}
						if seen[trigger].Outcome != wantOutcome {
							t.Fatalf("trigger=%+v", seen[trigger])
						}
						if origin == "join" && seen["innerjoin"].Outcome != wantOutcome {
							t.Fatalf("inner join=%+v", seen["innerjoin"])
						}
						candidateCallbacks := []string{trigger, "outerjoin"}
						resolutionErr := fault
						if mode == "cancel" {
							candidateCallbacks = candidateCallbacks[:1]
							resolutionErr = context.Canceled
						}
						if origin == "join" {
							candidateCallbacks = append(candidateCallbacks, "innerjoin")
						}
						for _, name := range candidateCallbacks {
							if !errors.Is(seen[name].Err, resolutionErr) {
								t.Fatalf("candidate resolution error %s=%+v", name, seen[name])
							}
						}
						loaded, err := store.Load(context.Background(), "target")
						if err != nil {
							t.Fatal(err)
						}
						if mode == "cancel" {
							if !errors.Is(first.err, context.Canceled) || first.result.Status != graph.StatusCancelled || f.joins.Load() != 0 || loaded.HadLocalFailures || loaded.Failure != nil || loaded.Completed || loaded.Revision != candidateRevision-1 || !reflect.DeepEqual(loaded, first.result.Checkpoint) {
								t.Fatalf("cancel=%+v %v stored=%+v", first.result, first.err, loaded)
							}
							if _, exists := seen["outerjoin"]; exists {
								t.Fatal("cancelled admission ran outer join")
							}
						} else {
							outer := seen["outerjoin"]
							if outer.Outcome != wantOutcome || outer.Revision != candidateRevision || seen[trigger].Revision != candidateRevision || f.joins.Load() != 1 {
								t.Fatalf("candidate resolutions=%+v", seen)
							}
							if fault == nil {
								checkFailureCompletion(t, first.result, first.err, candidateRevision)
								if !reflect.DeepEqual(loaded, first.result.Checkpoint) {
									t.Fatal("stored completion differs")
								}
							} else {
								if !errors.Is(first.err, fault) || first.result.Checkpoint.Revision != candidateRevision-1 || first.result.Checkpoint.HadLocalFailures || first.result.Checkpoint.Completed || first.result.Checkpoint.Failure != nil || !errors.Is(outer.Err, fault) {
									t.Fatalf("rejected candidate=%+v %v", first.result, first.err)
								}
								if write {
									if loaded.Revision != candidateRevision || !loaded.HadLocalFailures || loaded.Completed || loaded.Failure != nil {
										t.Fatalf("uncertain accepted candidate=%+v", loaded)
									}
								} else if !reflect.DeepEqual(loaded, first.result.Checkpoint) {
									t.Fatal("rejected candidate changed store")
								}
							}
						}
						// Release only test-owned admission and the healthy holder;
						// neither is needed to unblock target progress.
						f.releaseHeld()
						f.stopHolder()
						if err := <-f.holderDone; err != nil {
							t.Fatalf("holder failed: %v", err)
						}
						if reopen != nil {
							f.store = reopen()
							opts.Store = f.store
						}
						before := len(f.log.snapshot())
						recoverCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						last, err := f.runner.Recover(recoverCtx, "target", nil, opts)
						checkFailureCompletion(t, last, err, candidateRevision)
						checkCall(t, f.log.snapshot()[before:], graph.OperationRecover, last, err)
						replayed := failureResolutions(f.log.snapshot()[before:])
						for name, event := range seen {
							if _, exists := replayed[name]; exists && event.Outcome == graph.OutcomeCommitted {
								t.Fatalf("committed callback %s replayed", name)
							}
						}
						if mode == "success" || write {
							if _, exists := replayed["outerjoin"]; exists {
								t.Fatal("accepted join replayed")
							}
							if _, exists := replayed[trigger]; exists {
								t.Fatal("accepted trigger replayed")
							}
						} else {
							for _, name := range []string{"blocked-a", "blocked-b"} {
								if replayed[name].CallID != seen[name].CallID || replayed[name].Outcome != graph.OutcomeDiscarded {
									t.Fatalf("cancelled descendant replay %s=%+v original=%+v", name, replayed[name], seen[name])
								}
							}
							for _, name := range []string{trigger, "outerjoin"} {
								if replayed[name].Outcome != graph.OutcomeCommitted || (seen[name].CallID != "" && replayed[name].CallID != seen[name].CallID) || replayed[name].OperationID == seen[trigger].OperationID {
									t.Fatalf("replay %s=%+v original=%+v", name, replayed[name], seen[name])
								}
							}
							if origin == "join" && replayed["innerjoin"].CallID != seen["innerjoin"].CallID {
								t.Fatal("inner join replay identity changed")
							}
						}
						stored, err := f.store.Load(recoverCtx, "target")
						if err != nil || !reflect.DeepEqual(stored, last.Checkpoint) {
							t.Fatalf("stored recovery=%+v %v", stored, err)
						}
						// Prove all permits are reusable, rather than merely proving one
						// callback can still enter a partially leaked budget.
						acquired := 0
						defer func() {
							for range acquired {
								limit.Release(f.limiter)
							}
						}()
						for range 4 {
							if err := limit.Acquire(recoverCtx, f.limiter); err != nil {
								t.Fatal("permit leaked:", err)
							}
							acquired++
						}
						for acquired > 0 {
							limit.Release(f.limiter)
							acquired--
						}
						if _, err := singleRunner(t).Start(recoverCtx, "healthy", 1, graph.Options[int]{Limiter: f.limiter}); err != nil {
							t.Fatal("healthy execution:", err)
						}
					})
				}
			}
		}
	}
}

func checkFailureCompletion(t *testing.T, out graph.Result[int], err error, steps uint64) {
	t.Helper()
	if err != nil || out.Status != graph.StatusCompletedWithFailures || !out.Checkpoint.Completed || !out.Checkpoint.HadLocalFailures || out.Checkpoint.Failure != nil || out.Checkpoint.Steps != steps || out.Checkpoint.Revision != steps+1 || out.Checkpoint.Final == nil || *out.Checkpoint.Final != 17 || len(out.Checkpoint.Invocations) != 0 || len(out.Checkpoint.Groups) != 0 {
		t.Fatalf("completion=%+v %v", out, err)
	}
}
