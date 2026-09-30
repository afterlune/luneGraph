package observationtest

import (
	"context"
	"errors"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestApplyAndJoinReplayIdentities(t *testing.T) {
	for _, operation := range []graph.EventOperation{graph.OperationApply, graph.OperationJoin} {
		t.Run(string(operation), func(t *testing.T) {
			log := &recorder{}
			store := &faultStore{Store: memoryStore(t), fault: graph.ErrConflict}
			var r *graph.Runner[int]
			var inputs []graph.ResumeInput
			var first graph.Result[int]
			var err error
			opts := graph.Options[int]{Store: store, Observer: log, MaxConcurrency: 1}
			if operation == graph.OperationApply {
				store.revision = 3
				r = mountedRunner(t)
				seed, seedErr := r.Start(context.Background(), "replay", 0, graph.Options[int]{Store: store})
				if seedErr != nil {
					t.Fatal(seedErr)
				}
				inputs = []graph.ResumeInput{{InvocationID: seed.Checkpoint.Invocations[0].ID, Payload: []byte("3")}}
				first, err = r.Resume(context.Background(), seed.Checkpoint, inputs, opts)
				checkCall(t, log.snapshot(), graph.OperationResume, first, err)
			} else {
				store.revision = 4
				g := graph.New[int]("fork")
				node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					return graph.To(s, "a", "b"), nil
				})
				for _, name := range []string{"a", "b"} {
					node(t, g, name, func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
						return graph.To(s+1, "join"), nil
					})
				}
				if joinErr := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, values []int) (int, error) {
					return values[0] + values[1], nil
				}}); joinErr != nil {
					t.Fatal(joinErr)
				}
				for _, name := range []string{"a", "b"} {
					edge(t, g, "fork", name)
					edge(t, g, name, "join")
				}
				r = compile(t, g)
				first, err = r.Start(context.Background(), "replay", 0, opts)
				checkCall(t, log.snapshot(), graph.OperationStart, first, err)
			}
			if !errors.Is(err, graph.ErrConflict) {
				t.Fatalf("first execution = %+v, %v", first, err)
			}
			initial := log.snapshot()
			firstCallbacks := finished(initial, operation)
			if len(firstCallbacks) != 1 {
				t.Fatalf("first callbacks = %+v", firstCallbacks)
			}
			result, err := r.Recover(context.Background(), "replay", inputs, opts)
			if err != nil || result.Status != graph.StatusCompleted {
				t.Fatalf("recovery = %+v, %v", result, err)
			}
			events := log.snapshot()[len(initial):]
			checkCall(t, events, graph.OperationRecover, result, err)
			replayed := finished(events, operation)
			if len(replayed) != 1 || replayed[0].CallID == "" || replayed[0].CallID != firstCallbacks[0].CallID || replayed[0].OperationID == firstCallbacks[0].OperationID {
				t.Fatalf("replay identity = %+v", replayed)
			}
		})
	}
}
