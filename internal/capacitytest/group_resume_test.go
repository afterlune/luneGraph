package capacitytest

import (
	"context"
	"errors"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestGroupResumeBatchCommitBoundaries(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, fault := range []string{"clone", "cas", "join"} {
			t.Run(kind+"/"+fault, func(t *testing.T) {
				ctx := context.Background()
				g := graph.New[state]("fork")
				addNode(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
					return graph.To(s, "a", "b"), nil
				})
				for _, name := range []string{"a", "b"} {
					addNode(t, g, name, func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
						return graph.Wait(s, "advance", "join"), nil
					})
					addEdge(t, g, "fork", name)
				}
				boom := errors.New("injected group fault")
				armed := false
				var applies, joins []string
				if err := graph.RegisterContinuation(g, "advance", func(p []byte) (bool, error) { return string(p) == "b", nil }, func(_ context.Context, call graph.CallInfo, s state, second bool) (state, error) {
					applies = append(applies, call.CallID)
					s.Total = 1

					return s, nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := g.AddJoin(graph.JoinSpec[state]{Name: "join", From: "fork", OnError: graph.FailExecution, Merge: func(_ context.Context, call graph.CallInfo, values []state) (state, error) {
					joins = append(joins, call.CallID)
					if fault == "join" && armed {
						return state{}, boom
					}
					if len(values) != 2 || values[0].Total != 1 || values[1].Total != 1 {
						t.Fatalf("join inputs: %+v", values)
					}
					out := values[0]
					out.Total = 2
					return out, nil
				}}); err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"a", "b"} {
					addEdge(t, g, name, "join")
				}
				addNode(t, g, "done", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
					return graph.EndExecution(s), nil
				})
				addEdge(t, g, "join", "done")
				r, err := g.Compile(graph.Config[state]{MachineID: "batch-group", Clone: func(s state) (state, error) {
					if fault == "clone" && armed && len(applies) == 2 && s.Total == 1 {
						return s, boom
					}
					return clone(s)
				}})
				if err != nil {
					t.Fatal(err)
				}
				store := openStore(t, kind)
				seedExecution, seed, err := r.Start(ctx, "batch", initial("batch"), store, graph.Options{MaxConcurrency: 1})
				if err != nil || seed.Status != graph.StatusWaiting {
					t.Fatalf("seed: %+v %v", seed, err)
				}
				ids := map[string]string{}
				for _, inv := range mustCheckpoint(seedExecution).Invocations {
					if inv.Status == graph.InvocationWaiting {
						ids[inv.Node] = inv.ID
					}
				}
				if fault == "cas" {
					store = &revisionConflictStore{Store: store, revision: seed.Header.Revision + 1}
				}
				armed = true
				_, out, runErr := r.Recover(ctx, "batch", []graph.ResumeInput{{InvocationID: ids["a"], Payload: []byte("a")}, {InvocationID: ids["b"], Payload: []byte("b")}}, store, graph.Options{MaxConcurrency: 1})
				if fault == "join" {
					if !errors.Is(runErr, boom) || !out.Header.Completed || out.Header.Revision != seed.Header.Revision+3 || out.Header.Failure == nil {
						t.Fatalf("join failure: %+v %v", out, runErr)
					}
					if _, _, err := r.Recover(ctx, "batch", nil, store, graph.Options{}); !errors.Is(err, graph.ErrRunFailed) {
						t.Fatalf("terminal recovery: %v", err)
					}
					return
				}
				wantErr := boom
				if fault == "cas" {
					wantErr = graph.ErrConflict
				}
				wantRevision := seed.Header.Revision + 1
				if fault == "clone" {
					wantRevision++
				}
				loaded, loadErr := store.Load(ctx, "batch")
				if loadErr != nil || loaded.Revision != out.Header.Revision {
					t.Fatalf("load: %v", loadErr)
				}
				if !errors.Is(runErr, wantErr) || out.Header.Revision != wantRevision || len(loaded.Groups) != 1 || (out.Header.Failure != nil || out.Header.HadLocalFailures) {
					t.Fatalf("partial: %+v %v", out, runErr)
				}
				armed = false
				var replay []graph.ResumeInput
				if fault == "cas" {
					replay = []graph.ResumeInput{{InvocationID: ids["b"], Payload: []byte("b")}}
				}
				_, out, err = r.Recover(ctx, "batch", replay, store, graph.Options{MaxConcurrency: 1})
				if err != nil || out.Final == nil || out.Final.Total != 2 || (fault == "cas" && (len(applies) != 3 || applies[1] != applies[2])) || (fault == "clone" && len(applies) != 2) {
					t.Fatalf("replay: %+v %v calls=%v", out, err, applies)
				}
				if fault == "cas" && (len(joins) != 1) {
					t.Fatalf("join replay IDs: %v", joins)
				}
			})
		}
	}
}
