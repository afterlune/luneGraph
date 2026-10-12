package graph_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func identityStore(t *testing.T, kind string) graph.Store[int] {
	if kind == "none" {
		return nil
	}
	return resumeContractStore(t, kind)
}

func submitIdentityInputs(r *graph.Runner[int], e *graph.Execution[int], store graph.Store[int], entry string, inputs []graph.ResumeInput) (*graph.Execution[int], graph.Result[int], error) {
	ctx := context.Background()
	switch entry {
	case "advance":
		result, err := e.Advance(ctx, inputs, graph.Options{})
		return e, result, err
	case "resume":
		return r.Resume(ctx, mustCheckpoint(e), inputs, store, graph.Options{})
	default:
		return r.Recover(ctx, mustCheckpoint(e).RunID, inputs, store, graph.Options{})
	}
}

func TestResumeInputMatchesOriginalWait(t *testing.T) {
	for _, kind := range []string{"none", "memory", "sqlite"} {
		for _, entry := range []string{"advance", "resume", "recover"} {
			if kind == "none" && entry == "recover" {
				continue
			}
			t.Run(kind+"/"+entry, func(t *testing.T) {
				applies := 0
				g := graph.New[int]("wait")
				node(t, g, "wait", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					return graph.Wait(s, "input", "wait"), nil
				})
				edge(t, g, "wait", "wait")
				if err := graph.RegisterJSONContinuation(g, "input", func(_ context.Context, _ graph.CallInfo, s, n int) (int, error) {
					applies++
					return s + n, nil
				}); err != nil {
					t.Fatal(err)
				}
				r, store := intRunner(t, g), identityStore(t, kind)
				e, _, err := r.Start(context.Background(), "wait-identity", 0, store, graph.Options{})
				if err != nil {
					t.Fatal(err)
				}
				wait := e.Pending()[0]
				original := graph.ResumeInput{InvocationID: wait.InvocationID, CallID: wait.CallID, Payload: []byte("1")}
				e, _, err = submitIdentityInputs(r, e, store, entry, []graph.ResumeInput{original})
				if err != nil {
					t.Fatal(err)
				}
				before := mustCheckpoint(e)
				next := e.Pending()[0]
				if next.InvocationID != wait.InvocationID || next.CallID == wait.CallID {
					t.Fatal("loop did not create a new wait identity")
				}
				current := graph.ResumeInput{InvocationID: next.InvocationID, CallID: next.CallID, Payload: []byte("2")}
				for _, invalid := range []struct {
					name   string
					inputs []graph.ResumeInput
				}{
					{"old call", []graph.ResumeInput{original}},
					{"missing call", []graph.ResumeInput{{InvocationID: next.InvocationID, Payload: []byte("2")}}},
					{"wrong call", []graph.ResumeInput{{InvocationID: next.InvocationID, CallID: "other", Payload: []byte("2")}}},
					{"unknown invocation", []graph.ResumeInput{{InvocationID: "missing", CallID: next.CallID}}},
					{"duplicate", []graph.ResumeInput{current, current}},
				} {
					t.Run(invalid.name, func(t *testing.T) {
						got, result, err := submitIdentityInputs(r, e, store, entry, invalid.inputs)
						if !errors.Is(err, graph.ErrInvalidResumeInput) || result.Status != graph.StatusFailed || result.Header.Failure != nil || result.Header.Completed || applies != 1 || !reflect.DeepEqual(mustCheckpoint(got), before) {
							t.Fatalf("invalid input changed position: %+v applies=%d err=%v", result, applies, err)
						}
						if store != nil {
							persisted, err := store.Load(context.Background(), before.RunID)
							if err != nil || !reflect.DeepEqual(persisted, before) {
								t.Fatalf("stored position changed: %v", err)
							}
						}
					})
				}
				e, result, err := submitIdentityInputs(r, e, store, entry, []graph.ResumeInput{current})
				if err != nil || result.Status != graph.StatusWaiting || applies != 2 || mustCheckpoint(e).Invocations[0].State != 3 || result.Header.Revision != before.Revision+2 || result.Header.Steps != before.Steps+1 {
					t.Fatalf("current wait rejected: %+v applies=%d err=%v", result, applies, err)
				}
			})
		}
	}
}

func TestResumeBatchRejectsBeforeApply(t *testing.T) {
	for _, kind := range []string{"none", "memory", "sqlite"} {
		for _, entry := range []string{"advance", "resume", "recover"} {
			if kind == "none" && entry == "recover" {
				continue
			}
			t.Run(kind+"/"+entry, func(t *testing.T) {
				applies := 0
				r, err := resumeExampleRunner(func(graph.CallInfo) { applies++ })
				if err != nil {
					t.Fatal(err)
				}
				store := identityStore(t, kind)
				e, _, err := r.Start(context.Background(), "batch-identity", 0, store, graph.Options{})
				if err != nil {
					t.Fatal(err)
				}
				before := mustCheckpoint(e)
				waits := e.Pending()
				first := graph.ResumeInput{InvocationID: waits[0].InvocationID, CallID: waits[0].CallID, Payload: []byte("3")}
				second := graph.ResumeInput{InvocationID: waits[1].InvocationID, CallID: waits[1].CallID, Payload: []byte("4")}
				var groupID string
				for _, inv := range before.Invocations {
					if inv.Status == graph.InvocationGroup {
						groupID = inv.ID
					}
				}
				if groupID == "" {
					t.Fatal("missing fan-out parent")
				}
				for _, test := range []struct {
					name   string
					second graph.ResumeInput
					status graph.Status
				}{
					{"wrong call", graph.ResumeInput{InvocationID: second.InvocationID, CallID: "other", Payload: second.Payload}, graph.StatusFailed},
					{"not waiting", graph.ResumeInput{InvocationID: groupID, CallID: second.CallID}, graph.StatusFailed},
					{"decode error", graph.ResumeInput{InvocationID: second.InvocationID, CallID: second.CallID, Payload: []byte("bad")}, graph.StatusWaiting},
				} {
					t.Run(test.name, func(t *testing.T) {
						got, result, err := submitIdentityInputs(r, e, store, entry, []graph.ResumeInput{first, test.second})
						if err == nil || result.Status != test.status || applies != 0 || !reflect.DeepEqual(mustCheckpoint(got), before) {
							t.Fatalf("batch changed position: %+v applies=%d err=%v", result, applies, err)
						}
						if test.status == graph.StatusFailed && !errors.Is(err, graph.ErrInvalidResumeInput) {
							t.Fatalf("identity error = %v", err)
						}
						if test.status == graph.StatusWaiting && errors.Is(err, graph.ErrInvalidResumeInput) {
							t.Fatalf("decode error classified as identity error: %v", err)
						}
						if store != nil {
							persisted, loadErr := store.Load(context.Background(), before.RunID)
							if loadErr != nil || !reflect.DeepEqual(persisted, before) {
								t.Fatalf("stored batch changed: %v", loadErr)
							}
						}
					})
				}
				_, result, err := submitIdentityInputs(r, e, store, entry, []graph.ResumeInput{first, second})
				if err != nil || result.Status != graph.StatusCompleted || result.Final == nil || *result.Final != 7 || applies != 2 {
					t.Fatalf("valid batch failed: %+v applies=%d err=%v", result, applies, err)
				}
			})
		}
	}
}
