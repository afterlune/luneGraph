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
					if second && armed && fault == "clone" {
						s.Total = 99
					}
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
					if s.Total == 99 {
						return s, boom
					}
					return clone(s)
				}})
				if err != nil {
					t.Fatal(err)
				}
				store := openStore(t, kind)
				seed, err := r.Start(ctx, "batch", initial("batch"), graph.Options[state]{Store: store, MaxConcurrency: 1})
				if err != nil || seed.Status != graph.StatusWaiting {
					t.Fatalf("seed: %+v %v", seed, err)
				}
				ids := map[string]string{}
				for _, inv := range seed.Checkpoint.Invocations {
					if inv.Status == graph.InvocationWaiting {
						ids[inv.Node] = inv.ID
					}
				}
				if fault == "cas" {
					store = &revisionConflictStore{Store: store, revision: seed.Checkpoint.Revision + 1}
				}
				armed = true
				out, err := r.Recover(ctx, "batch", []graph.ResumeInput{{InvocationID: ids["a"], Payload: []byte("a")}, {InvocationID: ids["b"], Payload: []byte("b")}}, graph.Options[state]{Store: store, MaxConcurrency: 1})
				if fault == "join" {
					if !errors.Is(err, boom) || !out.Checkpoint.Completed || out.Checkpoint.Revision != seed.Checkpoint.Revision+2 || out.Checkpoint.Failure == nil {
						t.Fatalf("join failure: %+v %v", out, err)
					}
					if _, err := r.Recover(ctx, "batch", nil, graph.Options[state]{Store: store}); !errors.Is(err, graph.ErrRunFailed) {
						t.Fatalf("terminal recovery: %v", err)
					}
					return
				}
				wantErr := boom
				if fault == "cas" {
					wantErr = graph.ErrConflict
				}
				if !errors.Is(err, wantErr) || out.Checkpoint.Revision != seed.Checkpoint.Revision+1 || len(out.Checkpoint.Groups) != 1 || (out.Checkpoint.Failure != nil || out.Checkpoint.HadLocalFailures) {
					t.Fatalf("partial: %+v %v", out, err)
				}
				loaded, err := store.Load(ctx, "batch")
				if err != nil || loaded.Revision != out.Checkpoint.Revision {
					t.Fatalf("load: %v", err)
				}
				armed = false
				out, err = r.Recover(ctx, "batch", []graph.ResumeInput{{InvocationID: ids["b"], Payload: []byte("b")}}, graph.Options[state]{Store: store, MaxConcurrency: 1})
				if err != nil || out.Checkpoint.Final == nil || out.Checkpoint.Final.Total != 2 || len(applies) != 3 || applies[1] != applies[2] {
					t.Fatalf("replay: %+v %v calls=%v", out, err, applies)
				}
				if fault == "cas" && (len(joins) != 2 || joins[0] != joins[1]) {
					t.Fatalf("join replay IDs: %v", joins)
				}
			})
		}
	}
}
