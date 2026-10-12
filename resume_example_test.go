package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint/memory"
)

// recordedResumeInput belongs to the application, not the checkpoint format.
type recordedResumeInput struct {
	RunID, InvocationID, WaitingCallID string
	Payload                            []byte
}

func recordResumeInput[S any](position graph.Checkpoint[S], name string, payload []byte) (recordedResumeInput, error) {
	for _, inv := range position.Invocations {
		if inv.Node == name && inv.Status == graph.InvocationWaiting {
			return recordedResumeInput{position.RunID, inv.ID, inv.CallID, bytes.Clone(payload)}, nil
		}
	}
	return recordedResumeInput{}, fmt.Errorf("node %q is not waiting", name)
}

// Unmatched inputs require application reconciliation. A changed CallID alone
// does not prove that this payload succeeded, so this helper never acknowledges it.
func pendingResumeInputs[S any](position graph.Checkpoint[S], saved []recordedResumeInput) (matching []graph.ResumeInput, review []recordedResumeInput) {
	waiting := make(map[string]string)
	if !position.Completed {
		for _, inv := range position.Invocations {
			if inv.Status == graph.InvocationWaiting {
				waiting[inv.ID] = inv.CallID
			}
		}
	}
	for _, input := range saved {
		callID, ok := waiting[input.InvocationID]
		if input.RunID == position.RunID && ok && callID == input.WaitingCallID {
			matching = append(matching, graph.ResumeInput{InvocationID: input.InvocationID, CallID: input.WaitingCallID, Payload: input.Payload})
		} else {
			review = append(review, input)
		}
	}
	return matching, review
}

func resumeExampleRunner(applied func(graph.CallInfo)) (*graph.Runner[int], error) {
	g := graph.New[int]("fork")
	for _, spec := range []graph.NodeSpec[int]{
		{Name: "fork", Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.To(s, "left", "right"), nil
		}},
		{Name: "left", Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.Wait(s, "add", "sum"), nil
		}},
		{Name: "right", Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.Wait(s, "add", "sum"), nil
		}},
		{Name: "done", Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.EndExecution(s), nil
		}},
	} {
		spec.OnError = graph.FailExecution
		if err := g.AddNode(spec); err != nil {
			return nil, err
		}
	}
	if err := g.AddJoin(graph.JoinSpec[int]{Name: "sum", From: "fork", OnError: graph.FailExecution,
		Merge: func(_ context.Context, _ graph.CallInfo, _ int, branches []graph.BranchResult[int]) (int, error) {
			values := graph.BranchValues(branches)
			if len(values) != 2 {
				return 0, fmt.Errorf("expected two inputs, got %d", len(values))
			}
			return values[0] + values[1], nil
		}}); err != nil {
		return nil, err
	}
	for _, pair := range [][2]string{{"fork", "left"}, {"fork", "right"}, {"left", "sum"}, {"right", "sum"}, {"sum", "done"}} {
		if err := g.AddEdge(pair[0], pair[1]); err != nil {
			return nil, err
		}
	}
	if err := graph.RegisterJSONContinuation(g, "add", func(_ context.Context, call graph.CallInfo, s, n int) (int, error) {
		if applied != nil {
			applied(call)
		}
		return s + n, nil
	}); err != nil {
		return nil, err
	}
	return g.Compile(graph.Config[int]{MachineID: "pending-input-example-v1", Clone: graph.ValueClone[int]})
}

var errResumeExampleAck = errors.New("commit acknowledgement lost")

// A test-only fault injector: the next successful Commit can lose its ack.
type resumeExampleStore struct {
	graph.Store[int]
	loseAck atomic.Bool
	loads   atomic.Int32
}

func (s *resumeExampleStore) Load(ctx context.Context, id string) (graph.Checkpoint[int], error) {
	s.loads.Add(1)
	return s.Store.Load(ctx, id)
}

func (s *resumeExampleStore) Commit(ctx context.Context, revision uint64, change graph.Change[int]) error {
	if err := s.Store.Commit(ctx, revision, change); err != nil {
		return err
	}
	if s.loseAck.Swap(false) {
		return errResumeExampleAck
	}
	return nil
}

func ExampleRunner_Resume() {
	ctx := context.Background()
	runner, err := resumeExampleRunner(nil)
	if err != nil {
		panic(err)
	}
	backing, err := memory.New(graph.ValueClone[int])
	if err != nil {
		panic(err)
	}
	store := &resumeExampleStore{Store: backing}
	opts := graph.Options{MaxConcurrency: 1}
	optsStore := store
	firstExecution, _, err := runner.Start(ctx, "inputs", 0, optsStore, opts)
	if err != nil {
		panic(err)
	}
	left, err := recordResumeInput(mustCheckpoint(firstExecution), "left", []byte("3"))
	if err != nil {
		panic(err)
	}
	right, err := recordResumeInput(mustCheckpoint(firstExecution), "right", []byte("4"))
	if err != nil {
		panic(err)
	}
	saved := []recordedResumeInput{left, right} // Persist these in application storage.
	batch, _ := pendingResumeInputs(mustCheckpoint(firstExecution), saved)
	store.loseAck.Store(true)
	if _, _, err := runner.Resume(ctx, mustCheckpoint(firstExecution), batch, optsStore, opts); !errors.Is(err, errResumeExampleAck) {
		panic(fmt.Sprintf("expected an uncertain commit, got %v", err))
	}

	// Load authoritative state; do not trust the checkpoint returned with an error.
	latest, err := store.Load(ctx, "inputs")
	if err != nil {
		panic(err)
	}
	matching, review := pendingResumeInputs(latest, saved)
	fmt.Printf("pending=%d review=%d\n", len(matching), len(review))
	// Keep review entries for application reconciliation; do not acknowledge them here.
	if latest.Completed || len(matching) == 0 {
		return
	}
	// Resume reloads and checks this revision. On another error, return to the
	// caller for reconciliation; this example performs no automatic retry.
	_,

		result, err := runner.Resume(ctx, latest, matching, optsStore, opts)
	if err != nil {
		panic(err)
	}
	// Output:
	// pending=1 review=1
	// final: 7
	fmt.Println("final:", *result.Final)

}
