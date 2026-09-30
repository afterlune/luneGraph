package capacitytest

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

const historyStepsPerRound = 5

// Two side branches end or fail; only the healthy branch reaches the join.
// Outcomes are discarded while live topology returns to one root.
func historyRunner(t testing.TB, mode string) *graph.Runner[state] {
	g := graph.New[state]("fork")
	addNode(t, g, "fork", func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
		if err := checkIdentity(call, s); err != nil {
			return graph.Transition[state]{}, err
		}
		return graph.To(s, "healthy", "side1", "side2"), nil
	})
	addNode(t, g, "healthy", func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
		if err := checkIdentity(call, s); err != nil {
			return graph.Transition[state]{}, err
		}
		s.Total++
		s.Values["sum"]++
		return graph.To(s, "join"), nil
	})
	for side := 1; side <= 2; side++ {
		name := "side" + strconv.Itoa(side)
		addNode(t, g, name, func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
			if err := checkIdentity(call, s); err != nil {
				return graph.Transition[state]{}, err
			}
			s.Values["branch"] = side
			s.Values["sum"] = -side
			if mode == "failure" || (mode == "mixed" && side == 2) {
				return graph.Transition[state]{}, fmt.Errorf("side%d round%d", side, s.Round)
			}
			return graph.EndBranch[state](), nil
		})
		addEdge(t, g, "fork", name)
	}
	if err := g.AddJoin(graph.JoinSpec[state]{Name: "join", From: "fork", Merge: func(_ context.Context, call graph.CallInfo, values []state) (state, error) {
		if len(values) != 1 {
			return state{}, fmt.Errorf("join received %d healthy branches", len(values))
		}
		s := values[0]
		if err := checkIdentity(call, s); err != nil {
			return state{}, err
		}
		if s.Total != s.Round+1 || s.Values["sum"] != s.Total || s.Values["branch"] != -1 {
			return state{}, fmt.Errorf("side branch contaminated healthy state: %+v", s)
		}
		s.Round++
		return s, nil
	}}); err != nil {
		t.Fatal(err)
	}
	addNode(t, g, "boundary", func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
		if err := checkIdentity(call, s); err != nil {
			return graph.Transition[state]{}, err
		}
		if s.Round%4 == 0 {
			return graph.Wait(s, "advance", "fork"), nil
		}
		return graph.To(s, "fork"), nil
	})
	addEdge(t, g, "fork", "healthy")
	addEdge(t, g, "healthy", "join")
	addEdge(t, g, "join", "boundary")
	addEdge(t, g, "boundary", "fork")
	continuation(t, g)
	return compile(t, g)
}

func checkHistory(cp graph.Checkpoint[state], mode string, round int) error {
	s, err := rootState(cp)
	if err != nil {
		return err
	}
	inputs := (round - 1) / 4
	if s.Owner != cp.RunID || s.Round != round || s.Total != round || s.Values["sum"] != round || s.Values["branch"] != -1 || s.Values["inputs"] != inputs || cp.Completed || cp.Final != nil || len(cp.Invocations) != 1 || len(cp.Groups) != 0 || cp.Failure != nil || cp.HadLocalFailures != (mode != "terminal") || cp.Steps != uint64(round*historyStepsPerRound) || cp.Revision != cp.Steps+1+uint64(inputs) {
		return fmt.Errorf("invalid history boundary: round=%d state=%+v revision=%d steps=%d topology=%d/%d", round, s, cp.Revision, cp.Steps, len(cp.Invocations), len(cp.Groups))
	}
	wantStatus := graph.InvocationReady
	if round%4 == 0 {
		wantStatus = graph.InvocationWaiting
	}
	if cp.Invocations[0].Status != wantStatus || cp.Invocations[0].CallID == "" {
		return fmt.Errorf("invalid root invocation: %+v", cp.Invocations[0])
	}
	return nil
}

func advanceHistory(ctx context.Context, r *graph.Runner[state], cp graph.Checkpoint[state], opts graph.Options[state]) (graph.Result[state], error) {
	var inputs []graph.ResumeInput
	if cp.Invocations[0].Status == graph.InvocationWaiting {
		inputs = []graph.ResumeInput{{InvocationID: cp.Invocations[0].ID, Payload: []byte("1")}}
	}
	if opts.Store == nil {
		return r.Resume(ctx, cp, inputs, opts)
	}
	return r.Recover(ctx, cp.RunID, inputs, opts)
}

func seedHistory(t testing.TB, r *graph.Runner[state], opts graph.Options[state], mode string, rounds int) graph.Checkpoint[state] {
	t.Helper()
	out, err := r.Start(context.Background(), "history", initial("history"), opts)
	if err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= rounds; round++ {
		wantStatus := graph.StatusBudget
		if round%4 == 0 {
			wantStatus = graph.StatusWaiting
		}
		if out.Status != wantStatus {
			t.Fatalf("round %d status=%s want=%s", round, out.Status, wantStatus)
		}
		if err := checkHistory(out.Checkpoint, mode, round); err != nil {
			t.Fatal(err)
		}
		if round < rounds {
			out, err = advanceHistory(context.Background(), r, out.Checkpoint, opts)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if opts.Store != nil {
		saved, err := opts.Store.Load(context.Background(), out.Checkpoint.RunID)
		if err != nil || !reflect.DeepEqual(saved, out.Checkpoint) {
			t.Fatalf("stored history differs: %v", err)
		}
	}
	return out.Checkpoint
}
