package observationtest

import (
	"context"
	"errors"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func resolved(events []graph.Event, op graph.EventOperation) []graph.Event {
	var out []graph.Event
	for _, e := range events {
		if e.Phase == graph.PhaseResolved && e.Operation == op {
			out = append(out, e)
		}
	}
	return out
}

func TestResolvedStoreOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault error
		write bool
		want  graph.CallbackOutcome
	}{
		{"conflict", graph.ErrConflict, false, graph.OutcomeDiscarded},
		{"unacknowledged", errors.New("lost acknowledgement"), true, graph.OutcomeUnknown},
		{"unconfirmed", errors.New("write rejected"), false, graph.OutcomeUnknown},
		{"store cancellation", context.Canceled, true, graph.OutcomeUnknown},
		{"store deadline", context.DeadlineExceeded, false, graph.OutcomeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, log := singleRunner(t), &recorder{}
			store := &faultStore{Store: memoryStore(t), fault: tc.fault, write: tc.write, revision: 2}
			opts := graph.Options[int]{Observer: log, Store: store}
			first, err := r.Start(context.Background(), "resolution", 1, opts)
			if !errors.Is(err, tc.fault) || first.Checkpoint.Revision != 1 {
				t.Fatalf("first=%+v %v", first, err)
			}
			checkCall(t, log.snapshot(), graph.OperationStart, first, err)
			seen := resolved(log.snapshot(), graph.OperationNode)
			if len(seen) != 1 || seen[0].Outcome != tc.want || seen[0].Revision != 2 || !errors.Is(seen[0].Err, tc.fault) {
				t.Fatalf("resolution=%+v", seen)
			}
			before := len(log.snapshot())
			out, err := r.Recover(context.Background(), "resolution", nil, opts)
			if err != nil || !out.Checkpoint.Completed {
				t.Fatalf("recover=%+v %v", out, err)
			}
			checkCall(t, log.snapshot()[before:], graph.OperationRecover, out, err)
			if tc.write {
				if len(resolved(log.snapshot()[before:], graph.OperationNode)) != 0 {
					t.Fatal("committed work replayed")
				}
			} else {
				replay := resolved(log.snapshot()[before:], graph.OperationNode)
				if len(replay) != 1 || replay[0].Outcome != graph.OutcomeCommitted || replay[0].CallID != seen[0].CallID || replay[0].OperationID == seen[0].OperationID {
					t.Fatalf("replay=%+v", replay)
				}
			}
			if resolved(log.snapshot(), graph.OperationNode)[0].Outcome != tc.want {
				t.Fatal("recovery rewrote earlier observation")
			}
		})
	}
}

func TestResolvedCallbackDecisions(t *testing.T) {
	for _, mode := range []string{"success", "local failure", "execution failure", "panic", "invalid transition", "interrupt", "cancel before commit"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g := graph.New[int]("work")
			scope := graph.FailExecution
			if mode == "local failure" {
				scope = graph.FailInvocation
			}
			cause := errors.New("callback failed")
			if err := g.AddNode(graph.NodeSpec[int]{Name: "work", OnError: scope, Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				switch mode {
				case "local failure", "execution failure":
					return graph.Transition[int]{}, cause
				case "panic":
					panic(cause)
				case "invalid transition":
					return graph.To(s, "missing"), nil
				case "interrupt":
					return graph.EndExecution(99), graph.Interrupt(cause)
				case "cancel before commit":
					cancel()
				}
				return graph.EndExecution(s + 1), nil
			}}); err != nil {
				t.Fatal(err)
			}
			log := &recorder{}
			out, err := compile(t, g).Start(ctx, "decision", 1, graph.Options[int]{Observer: log})
			checkCall(t, log.snapshot(), graph.OperationStart, out, err)
			seen := resolved(log.snapshot(), graph.OperationNode)
			want, rev := graph.OutcomeCommitted, uint64(2)
			if mode == "invalid transition" || mode == "interrupt" || mode == "cancel before commit" {
				want, rev = graph.OutcomeDiscarded, 1
			}
			if len(seen) != 1 || seen[0].Outcome != want || seen[0].Revision != rev || (want == graph.OutcomeCommitted && seen[0].Err != nil) || (want == graph.OutcomeDiscarded && seen[0].Err == nil) {
				t.Fatalf("resolution=%+v out=%+v err=%v", seen, out, err)
			}
		})
	}
}

func TestResolvedJoinBatch(t *testing.T) {
	for _, mode := range []string{"success", "interrupt", "conflict", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			g := graph.New[int]("fork")
			node(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				return graph.To(s, "a", "b"), nil
			})
			for _, name := range []string{"a", "b"} {
				node(t, g, name, func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
					return graph.To(s+1, "join"), nil
				})
				edge(t, g, "fork", name)
			}
			if err := g.AddJoin(graph.JoinSpec[int]{Name: "join", From: "fork", Merge: func(_ context.Context, _ graph.CallInfo, v []int) (int, error) {
				if mode == "interrupt" {
					return 0, graph.Interrupt(nil)
				}
				return v[0] + v[1], nil
			}}); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"a", "b"} {
				edge(t, g, name, "join")
			}
			store := &faultStore{Store: memoryStore(t), revision: 4}
			if mode == "conflict" {
				store.fault = graph.ErrConflict
			} else if mode == "unknown" {
				store.fault = errors.New("unconfirmed")
				store.write = true
			} else {
				store.failed = true
			}
			log := &recorder{}
			out, err := compile(t, g).Start(context.Background(), "batch", 1, graph.Options[int]{Observer: log, Store: store, MaxConcurrency: 1})
			checkCall(t, log.snapshot(), graph.OperationStart, out, err)
			joins, nodes := resolved(log.snapshot(), graph.OperationJoin), resolved(log.snapshot(), graph.OperationNode)
			if len(joins) != 1 || len(nodes) != 3 {
				t.Fatalf("events=%+v", log.snapshot())
			}
			want, rev := graph.OutcomeCommitted, uint64(4)
			if mode == "interrupt" {
				want, rev = graph.OutcomeDiscarded, 3
			} else if mode == "conflict" {
				want = graph.OutcomeDiscarded
			} else if mode == "unknown" {
				want = graph.OutcomeUnknown
			}
			if joins[0].Outcome != want || joins[0].Revision != rev || nodes[2].Outcome != want || nodes[2].Revision != rev || nodes[0].Outcome != graph.OutcomeCommitted || nodes[1].Outcome != graph.OutcomeCommitted {
				t.Fatalf("nodes=%+v joins=%+v", nodes, joins)
			}
		})
	}
}
