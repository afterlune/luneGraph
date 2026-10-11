package capacitytest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type revisionConflictStore struct {
	graph.Store[state]
	revision uint64
	once     sync.Once
}

func (s *revisionConflictStore) Commit(ctx context.Context, expected uint64, change graph.Change[state]) error {
	conflict := false
	if expected == s.revision {
		s.once.Do(func() { conflict = true })
	}
	if conflict {
		return graph.ErrConflict
	}
	return s.Store.Commit(ctx, expected, change)
}

func TestNestedGroupFailureAndCompactionReplay(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			g := graph.New[state]("fork")
			outer := []string{"nested"}
			for i := range 15 {
				outer = append(outer, fmt.Sprintf("outer-%d", i))
			}
			inner := make([]string, 10)
			for i := range inner {
				inner[i] = fmt.Sprintf("inner-%d", i)
			}
			addNode(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
				return graph.To(s, outer...), nil
			})
			addNode(t, g, "nested", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
				return graph.To(s, inner...), nil
			})
			addEdge(t, g, "fork", "nested")
			boom := errors.New("nested group failure")
			var nodeCalls, joinCalls []string
			for i, name := range inner {
				err := g.AddNode(graph.NodeSpec[state]{Name: name, OnError: graph.FailGroup, Run: func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
					if i != 0 {
						return graph.Transition[state]{}, fmt.Errorf("removed sibling %d was scheduled", i)
					}
					nodeCalls = append(nodeCalls, call.CallID)
					return graph.Transition[state]{}, boom
				}})
				if err != nil {
					t.Fatal(err)
				}
				addEdge(t, g, "nested", name)
			}
			for i, name := range outer[1:] {
				addNode(t, g, name, func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
					if err := checkIdentity(call, s); err != nil {
						return graph.Transition[state]{}, err
					}
					s.Values["branch"] = i
					return graph.To(s, "join"), nil
				})
				addEdge(t, g, "fork", name)
			}
			if err := g.AddJoin(graph.JoinSpec[state]{Name: "join", From: "fork", Merge: func(_ context.Context, call graph.CallInfo, values []state) (state, error) {
				joinCalls = append(joinCalls, call.CallID)
				if len(values) != 15 {
					return state{}, fmt.Errorf("join received %d branches", len(values))
				}
				out := values[0]
				for i, s := range values {
					if s.Owner != out.Owner || s.Values["branch"] != i {
						return state{}, fmt.Errorf("join branch order changed: %+v", values)
					}
				}
				out.Total = len(values)
				return out, nil
			}}); err != nil {
				t.Fatal(err)
			}
			for _, name := range outer[1:] {
				addEdge(t, g, name, "join")
			}
			addNode(t, g, "done", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
				return graph.EndExecution(s), nil
			})
			addEdge(t, g, "join", "done")
			r := compile(t, g)
			store := &revisionConflictStore{Store: openStore(t, kind), revision: 18}
			opts := graph.Options{MaxSteps: 1, MaxConcurrency: 1}
			optsStore := store
			_, out, err := r.Start(context.Background(), "nested", initial("nested"), optsStore, opts)
			if err != nil {
				t.Fatal(err)
			}
			conflicts := 0
			for attempt := 0; !out.Header.Completed && attempt < 20; attempt++ {
				_, out, err = r.Recover(context.Background(), "nested", nil, optsStore, opts)
				if errors.Is(err, graph.ErrConflict) {
					conflicts++
					checkpoint, loadErr := store.Load(context.Background(), "nested")
					if loadErr != nil || out.Header.Revision != 18 || out.Header.Steps != 17 || len(checkpoint.Invocations) != 27 || len(checkpoint.Groups) != 2 || (out.Header.Failure != nil || out.Header.HadLocalFailures) {
						t.Fatalf("candidate compaction changed the last commit: %+v", out)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if out.Status != graph.StatusCompletedWithFailures || out.Final == nil || out.Final.Total != 15 || out.Header.Steps != 19 || (!out.Header.HadLocalFailures || out.Header.Failure != nil) || conflicts != 1 {
				t.Fatalf("nested failure recovery: %+v conflicts=%d", out, conflicts)
			}
			if len(nodeCalls) != 2 || nodeCalls[0] == "" || nodeCalls[0] != nodeCalls[1] || len(joinCalls) != 1 || joinCalls[0] == "" {
				t.Fatalf("compaction replay IDs: node=%v join=%v", nodeCalls, joinCalls)
			}
		})
	}
}
