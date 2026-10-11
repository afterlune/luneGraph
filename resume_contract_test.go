package graph_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func resumeContractStore(t *testing.T, kind string) graph.Store[int] {
	t.Helper()
	if kind == "memory" {
		return newMemoryStore(t)
	}
	store, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "inputs.db"), checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func TestPendingResumeInputExample(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			t.Run("uncertain_prefix", func(t *testing.T) {
				ctx := context.Background()
				calls := make(map[string]int)
				r, err := resumeExampleRunner(func(call graph.CallInfo) { calls[call.CallID]++ })
				if err != nil {
					t.Fatal(err)
				}
				store := &resumeExampleStore{Store: resumeContractStore(t, kind)}
				opts := graph.Options{MaxConcurrency: 1}
				optsStore := store
				seedExecution, seed, err := r.Start(ctx, "prefix", 0, optsStore, opts)
				if err != nil || seed.Status != graph.StatusWaiting {
					t.Fatalf("seed=%+v err=%v", seed, err)
				}
				payload := []byte("3")
				left, err := recordResumeInput(mustCheckpoint(seedExecution), "left", payload)
				if err != nil {
					t.Fatal(err)
				}
				payload[0] = '9' // The application record must own its payload.
				right, err := recordResumeInput(mustCheckpoint(seedExecution), "right", []byte("4"))
				if err != nil {
					t.Fatal(err)
				}
				saved := []recordedResumeInput{left, right}
				batch, _ := pendingResumeInputs(mustCheckpoint(seedExecution), saved)
				store.loseAck.Store(true)
				_, failed, err := r.Resume(ctx, mustCheckpoint(seedExecution), batch, optsStore, opts)
				if !errors.Is(err, errResumeExampleAck) || failed.Header.Revision != seed.Header.Revision || failed.Header.Failure != nil {
					t.Fatalf("uncertain result=%+v err=%v", failed, err)
				}
				if calls[left.WaitingCallID] != 1 || calls[right.WaitingCallID] != 0 {
					t.Fatalf("accepted prefix calls=%v", calls)
				}
				before, err := store.Load(ctx, "prefix")
				if err != nil || before.Revision != seed.Header.Revision+1 {
					t.Fatalf("authoritative prefix=%+v err=%v", before, err)
				}
				if _, _, err := r.Recover(ctx, "prefix", batch, optsStore, opts); err == nil {
					t.Fatal("original batch with an advanced address was accepted")
				}
				after, err := store.Load(ctx, "prefix")
				if err != nil || !reflect.DeepEqual(before, after) || len(calls) != 1 {
					t.Fatalf("rejected batch changed position or Apply calls: %v %v", calls, err)
				}
				store.loads.Store(0)
				latest, err := store.Load(ctx, "prefix")
				if err != nil {
					t.Fatal(err)
				}
				matching, review := pendingResumeInputs(latest, saved)
				if len(matching) != 1 || matching[0].InvocationID != right.InvocationID || string(matching[0].Payload) != "4" || !reflect.DeepEqual(review, saved[:1]) {
					t.Fatalf("matching=%+v review=%+v", matching, review)
				}
				finishedExecution, finished, err := r.Resume(ctx, latest, matching, optsStore, opts)
				if err != nil || finished.Status != graph.StatusCompleted || finished.Final == nil || *finished.Final != 7 {
					t.Fatalf("finished=%+v err=%v", finished, err)
				}
				if store.loads.Load() != 2 || calls[left.WaitingCallID] != 1 || calls[right.WaitingCallID] != 1 {
					t.Fatalf("loads=%d calls=%v", store.loads.Load(), calls)
				}
				matching, terminalReview := pendingResumeInputs(mustCheckpoint(finishedExecution), saved)
				if len(matching) != 0 || !reflect.DeepEqual(terminalReview, saved) {
					t.Fatalf("terminal matching=%v review=%v", matching, terminalReview)
				}
			})
			t.Run("stale_position", func(t *testing.T) {
				ctx := context.Background()
				var applies int
				r, err := resumeExampleRunner(func(graph.CallInfo) { applies++ })
				if err != nil {
					t.Fatal(err)
				}
				store := resumeContractStore(t, kind)
				opts := graph.Options{MaxConcurrency: 1}
				optsStore := store
				seedExecution, _, err := r.Start(ctx, "stale", 0, optsStore, opts)
				if err != nil {
					t.Fatal(err)
				}
				left, err := recordResumeInput(mustCheckpoint(seedExecution), "left", []byte("3"))
				if err != nil {
					t.Fatal(err)
				}
				right, err := recordResumeInput(mustCheckpoint(seedExecution), "right", []byte("4"))
				if err != nil {
					t.Fatal(err)
				}
				loaded, err := store.Load(ctx, "stale")
				if err != nil {
					t.Fatal(err)
				}
				matching, _ := pendingResumeInputs(loaded, []recordedResumeInput{right})
				first, _ := pendingResumeInputs(mustCheckpoint(seedExecution), []recordedResumeInput{left})
				advancedExecution, advanced, err := r.Resume(ctx, mustCheckpoint(seedExecution), first, optsStore, opts)
				if err != nil || applies != 1 {
					t.Fatalf("concurrent advance=%+v applies=%d err=%v", advanced, applies, err)
				}
				if _, _, err := r.Resume(ctx, loaded, matching, optsStore, opts); !errors.Is(err, graph.ErrConflict) || applies != 1 {
					t.Fatalf("stale reference applied input: applies=%d err=%v", applies, err)
				}
				latest, err := store.Load(ctx, "stale")
				if err != nil || !reflect.DeepEqual(latest, mustCheckpoint(advancedExecution)) {
					t.Fatalf("stale rejection changed position: %+v %v", latest, err)
				}
				foreign := right
				foreign.RunID = "another-run"
				matching, review := pendingResumeInputs(latest, []recordedResumeInput{foreign})
				if len(matching) != 0 || !reflect.DeepEqual(review, []recordedResumeInput{foreign}) {
					t.Fatal("input from another run was rebound")
				}
			})
			t.Run("new_waiting_call", func(t *testing.T) {
				ctx := context.Background()
				var applies int
				g := graph.New[int]("wait")
				node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					return graph.Wait(s, "add", "wait"), nil
				})
				edge(t, g, "wait", "wait")
				if err := graph.RegisterJSONContinuation(g, "add", func(_ context.Context, _ graph.CallInfo, s, n int) (int, error) {
					applies++
					return s + n, nil
				}); err != nil {
					t.Fatal(err)
				}
				r, opts := intRunner(t, g), graph.Options{}
				seedExecution, _, err := r.Start(ctx, "loop-input", 0, nil, opts)
				if err != nil {
					t.Fatal(err)
				}
				original, err := recordResumeInput(mustCheckpoint(seedExecution), "wait", []byte("1"))
				if err != nil {
					t.Fatal(err)
				}
				batch, _ := pendingResumeInputs(mustCheckpoint(seedExecution), []recordedResumeInput{original})
				nextExecution, next, err := r.Resume(ctx, mustCheckpoint(seedExecution), batch, nil, opts)
				if err != nil || next.Status != graph.StatusWaiting || applies != 1 {
					t.Fatalf("next wait=%+v applies=%d err=%v", next, applies, err)
				}
				current, err := recordResumeInput(mustCheckpoint(nextExecution), "wait", []byte("2"))
				if err != nil || current.InvocationID != original.InvocationID || current.WaitingCallID == original.WaitingCallID {
					t.Fatalf("waiting identities: original=%+v current=%+v err=%v", original, current, err)
				}
				matching, review := pendingResumeInputs(mustCheckpoint(nextExecution), []recordedResumeInput{original})
				if len(matching) != 0 || !reflect.DeepEqual(review, []recordedResumeInput{original}) || applies != 1 {
					t.Fatal("old input was rebound or acknowledged")
				}
				matching, review = pendingResumeInputs(mustCheckpoint(nextExecution), []recordedResumeInput{current})
				if len(matching) != 1 || len(review) != 0 {
					t.Fatal("new input did not match its waiting identity")
				}
			})
		})
	}
}
