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
						joinRevision := uint64(7)
						if origin == "join" {
							joinRevision = 10
						}
						var fault error
						write := mode == "unknown after write"
						want := graph.OutcomeCommitted
						switch mode {
						case "conflict":
							fault = graph.ErrConflict
							want = graph.OutcomeDiscarded
						case "unknown without write", "unknown after write":
							fault = errors.New("uncertain join commit")
							want = graph.OutcomeUnknown
						case "cancel":
							want = graph.OutcomeDiscarded
						}
						if fault != nil {
							f.store = &faultStore{Store: store, fault: fault, write: write, revision: joinRevision}
						}
						opts := graph.Options[int]{Store: f.store, Limiter: f.limiter, MaxConcurrency: 4, Observer: f.observer}
						type answer struct {
							out graph.Result[int]
							err error
						}
						done := make(chan answer, 1)
						f.goRun(func() { out, err := f.runner.Recover(f.ctx, "target", nil, opts); done <- answer{out, err} })
						f.wait(t, f.ctx, f.cloneReached, "outer join clone reached")
						if limit.TryAcquire(f.limiter) {
							limit.Release(f.limiter)
							t.Fatal("shared budget was not saturated")
						}
						f.allowClone()
						f.wait(t, f.ctx, f.blockedCancelled, "pruned callbacks cancelled")
						if mode == "cancel" {
							f.cancel()
						}
						f.releaseBlocked()
						var first answer
						select {
						case first = <-done:
						case <-time.After(5 * time.Second):
							t.Fatal("execution did not drain")
						}
						events := f.log.snapshot()
						checkCall(t, events, graph.OperationRecover, first.out, first.err)
						seen := failureResolutions(events)
						for _, name := range []string{"blocked-a", "blocked-b"} {
							e := seen[name]
							if e.CallID == "" || e.Outcome != graph.OutcomeDiscarded || e.Action != graph.ActionEndExecution {
								t.Fatalf("pruned callback accepted: %+v", e)
							}
						}
						trigger := "failer"
						if origin == "join" {
							trigger = "innerjoin"
							if seen["b"].Outcome != graph.OutcomeCommitted {
								t.Fatal("triggering branch was not committed")
							}
						}
						if seen[trigger].Outcome != graph.OutcomeCommitted || seen[trigger].Revision != joinRevision-1 {
							t.Fatalf("failure commit=%+v", seen[trigger])
						}
						loaded, err := store.Load(context.Background(), "target")
						if err != nil {
							t.Fatal(err)
						}
						if !loaded.HadLocalFailures || loaded.Failure != nil {
							t.Fatalf("failure marker missing: %+v", loaded)
						}
						if mode == "cancel" {
							if !errors.Is(first.err, context.Canceled) || first.out.Status != graph.StatusCancelled || loaded.Revision != joinRevision-1 {
								t.Fatalf("cancel=%+v %v", first.out, first.err)
							}
							if _, ok := seen["outerjoin"]; ok {
								t.Fatal("cancelled admission ran Merge")
							}
						} else {
							e := seen["outerjoin"]
							if e.CallID == "" || e.Outcome != want || e.Revision != joinRevision || !errors.Is(e.Err, fault) {
								t.Fatalf("join=%+v", e)
							}
							if fault != nil {
								if !errors.Is(first.err, fault) || first.out.Checkpoint.Revision != joinRevision-1 {
									t.Fatalf("uncertain=%+v %v", first.out, first.err)
								}
							} else if first.err != nil || !first.out.Checkpoint.Completed {
								t.Fatalf("completion=%+v %v", first.out, first.err)
							}
						}
						if !write && !reflect.DeepEqual(loaded, first.out.Checkpoint) {
							t.Fatal("stored position differs")
						}
						if write && loaded.Revision != joinRevision {
							t.Fatal("lost acknowledgement did not write")
						}
						if f.holderCtx.Err() != nil {
							t.Fatal("independent execution cancelled")
						}
						f.releaseHeld()
						f.stopHolder()
						if err := <-f.holderDone; err != nil {
							t.Fatal(err)
						}
						if reopen != nil {
							opts.Store = reopen()
						}
						before := len(f.log.snapshot())
						recoverCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						last, err := f.runner.Recover(recoverCtx, "target", nil, opts)
						steps := uint64(6)
						if origin == "join" {
							steps = 8
						}
						if err != nil || last.Status != graph.StatusCompletedWithFailures || last.Checkpoint.Steps != steps || last.Checkpoint.Revision != joinRevision+1 || last.Checkpoint.Final == nil || *last.Checkpoint.Final != 17 {
							t.Fatalf("recovery=%+v %v", last, err)
						}
						replay := failureResolutions(f.log.snapshot()[before:])
						for _, name := range []string{"failer", "b", "innerjoin", "blocked-a", "blocked-b"} {
							if _, ok := replay[name]; ok {
								t.Fatalf("committed/pruned callback %s replayed", name)
							}
						}
						if mode == "success" || write {
							if _, ok := replay["outerjoin"]; ok {
								t.Fatal("committed join replayed")
							}
						} else if replay["outerjoin"].Outcome != graph.OutcomeCommitted {
							t.Fatal("pending join was not replayed")
						}
						if old := seen["outerjoin"].CallID; old != "" && replay["outerjoin"].CallID != "" && old != replay["outerjoin"].CallID {
							t.Fatal("join replay identity changed")
						}
						stored, err := opts.Store.Load(recoverCtx, "target")
						if err != nil || !reflect.DeepEqual(stored, last.Checkpoint) {
							t.Fatal("recovery not durable", err)
						}
						acquired := 0
						defer func() {
							for range acquired {
								limit.Release(f.limiter)
							}
						}()
						for range 4 {
							if err := limit.Acquire(recoverCtx, f.limiter); err != nil {
								t.Fatal("permit leaked", err)
							}
							acquired++
						}
					})
				}
			}
		}
	}
}
