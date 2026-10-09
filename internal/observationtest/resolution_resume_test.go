package observationtest

import (
	"context"
	"errors"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestResolvedNestedJoinInterruptionDiscardsWholeCandidate(t *testing.T) {
	g := graph.New[int]("outer")
	node(t, g, "outer", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "inner", "sibling"), nil
	})
	node(t, g, "inner", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "a", "b"), nil
	})
	for _, name := range []string{"a", "b"} {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.To(s+1, "innerjoin"), nil
		})
	}
	node(t, g, "sibling", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s+1, "outerjoin"), nil
	})
	interrupted := false
	for _, spec := range []graph.JoinSpec[int]{
		{Name: "innerjoin", From: "inner", Merge: func(_ context.Context, _ graph.CallInfo, v []int) (int, error) { return v[0] + v[1], nil }},
		{Name: "outerjoin", From: "outer", Merge: func(_ context.Context, _ graph.CallInfo, v []int) (int, error) {
			if !interrupted {
				interrupted = true
				return 0, graph.Interrupt(nil)
			}
			return v[0] + v[1], nil
		}},
	} {
		if err := g.AddJoin(spec); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range [][2]string{{"outer", "inner"}, {"outer", "sibling"}, {"inner", "a"}, {"inner", "b"}, {"a", "innerjoin"}, {"b", "innerjoin"}, {"innerjoin", "outerjoin"}, {"sibling", "outerjoin"}} {
		edge(t, g, e[0], e[1])
	}
	r, log := compile(t, g), &recorder{}
	opts := graph.Options[int]{Observer: log, MaxConcurrency: 1, Store: memoryStore(t)}
	first, err := r.Start(context.Background(), "nested", 1, opts)
	if !errors.Is(err, graph.ErrInterrupted) || len(first.Checkpoint.Groups) != 2 {
		t.Fatalf("interruption=%+v %v", first, err)
	}
	checkCall(t, log.snapshot(), graph.OperationStart, first, err)
	joins := resolved(log.snapshot(), graph.OperationJoin)
	if len(joins) != 2 {
		t.Fatalf("joins=%+v", joins)
	}
	ids := map[string]string{}
	for _, e := range joins {
		if e.Outcome != graph.OutcomeDiscarded || e.Revision != first.Checkpoint.Revision {
			t.Fatalf("nested resolution=%+v", e)
		}
		ids[e.Node] = e.CallID
	}
	before := len(log.snapshot())
	last, err := r.Recover(context.Background(), "nested", nil, opts)
	if err != nil || !last.Checkpoint.Completed {
		t.Fatalf("recovery=%+v %v", last, err)
	}
	checkCall(t, log.snapshot()[before:], graph.OperationRecover, last, err)
	joins = resolved(log.snapshot()[before:], graph.OperationJoin)
	if len(joins) != 2 {
		t.Fatalf("replayed joins=%+v", joins)
	}
	for _, e := range joins {
		if e.Outcome != graph.OutcomeCommitted || e.Revision != last.Checkpoint.Revision || ids[e.Node] != e.CallID {
			t.Fatalf("replayed resolution=%+v", e)
		}
	}
}

func TestResolvedApplyInputPrefix(t *testing.T) {
	g := graph.New[int]("fork")
	node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.To(s, "a", "b"), nil
	})
	for _, name := range []string{"a", "b"} {
		node(t, g, name, func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
			return graph.Wait(s, "input", "end"), nil
		})
		edge(t, g, "fork", name)
	}
	node(t, g, "end", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.EndBranch[int](), nil
	})
	for _, name := range []string{"a", "b"} {
		edge(t, g, name, "end")
	}
	interrupted := false
	if err := graph.RegisterJSONContinuation(g, "input", func(_ context.Context, _ graph.CallInfo, s, p int) (int, error) {
		if p == 2 && !interrupted {
			interrupted = true
			return 99, graph.Interrupt(nil)
		}
		return s + p, nil
	}); err != nil {
		t.Fatal(err)
	}
	r, store := compile(t, g), memoryStore(t)
	seed, err := r.Start(context.Background(), "prefix", 1, graph.Options[int]{Store: store, MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	var inputs []graph.ResumeInput
	for _, inv := range seed.Checkpoint.Invocations {
		if inv.Status == graph.InvocationWaiting {
			payload := []byte("1")
			if len(inputs) == 1 {
				payload = []byte("2")
			}
			inputs = append(inputs, graph.ResumeInput{InvocationID: inv.ID, Payload: payload})
		}
	}
	if len(inputs) != 2 {
		t.Fatal("waiting fixture")
	}
	log := &recorder{}
	opts := graph.Options[int]{Store: store, Observer: log, MaxConcurrency: 1}
	first, err := r.Resume(context.Background(), seed.Checkpoint, inputs, opts)
	if !errors.Is(err, graph.ErrInterrupted) || first.Checkpoint.Revision != seed.Checkpoint.Revision+1 {
		t.Fatalf("prefix=%+v %v", first, err)
	}
	checkCall(t, log.snapshot(), graph.OperationResume, first, err)
	seen := resolved(log.snapshot(), graph.OperationApply)
	if len(seen) != 2 || seen[0].Outcome != graph.OutcomeCommitted || seen[1].Outcome != graph.OutcomeDiscarded || seen[0].Revision != first.Checkpoint.Revision || seen[1].Revision != first.Checkpoint.Revision {
		t.Fatalf("apply prefix=%+v", seen)
	}
	before := len(log.snapshot())
	last, err := r.Recover(context.Background(), "prefix", inputs[1:], opts)
	if err != nil || !last.Checkpoint.Completed {
		t.Fatalf("recover=%+v %v", last, err)
	}
	checkCall(t, log.snapshot()[before:], graph.OperationRecover, last, err)
	replay := resolved(log.snapshot()[before:], graph.OperationApply)
	if len(replay) != 1 || replay[0].CallID != seen[1].CallID || replay[0].Outcome != graph.OutcomeCommitted {
		t.Fatalf("apply replay=%+v", replay)
	}
}
