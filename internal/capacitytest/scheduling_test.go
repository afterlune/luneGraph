package capacitytest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestWideRotationAcrossBudgetAndWait(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			g := graph.New[state]("fork")
			targets := make([]string, 16)
			for i := range targets {
				targets[i] = fmt.Sprintf("branch-%d", i)
			}
			addNode(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
				return graph.To(s, targets...), nil
			})
			type visit struct {
				branch int
				id     uint64
			}
			var visits []visit // MaxConcurrency=1; read only after each call drains.
			for i, target := range targets {
				addNode(t, g, target, func(_ context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
					if err := checkIdentity(call, s); err != nil {
						return graph.Transition[state]{}, err
					}
					id, err := strconv.ParseUint(strings.TrimPrefix(call.InvocationID, "i"), 10, 64)
					if err != nil {
						return graph.Transition[state]{}, err
					}
					visits = append(visits, visit{i, id})
					s.Round++
					s.Values["branch"] = i
					if i == 0 && s.Round == 1 {
						return graph.To(s, target), nil
					}
					if i == 1 && s.Round == 1 {
						return graph.Wait(s, "advance", target), nil
					}
					return graph.EndBranch(s), nil
				})
				addEdge(t, g, "fork", target)
				if i < 2 {
					addEdge(t, g, target, target)
				}
			}
			continuation(t, g)
			r, store := compile(t, g), openStore(t, kind)
			opts := graph.Options[state]{Store: store, MaxSteps: 1, MaxConcurrency: 1}
			out, err := r.Start(context.Background(), "rotation", initial("rotation"), opts)
			if err != nil || out.Status != graph.StatusBudget || len(out.Checkpoint.Invocations) != 17 {
				t.Fatalf("fork: %+v, %v", out, err)
			}
			var waitingID string
			for step := range 17 {
				out, err = r.Recover(context.Background(), "rotation", nil, opts)
				want := step
				if step == 16 {
					want = 0
				}
				if err != nil || len(visits) != step+1 || visits[step].branch != want || out.Checkpoint.ScheduleCursor != visits[step].id || out.Checkpoint.Steps != uint64(step+2) {
					t.Fatalf("step %d: %+v visits=%v, %v", step, out, visits, err)
				}
				for _, inv := range out.Checkpoint.Invocations {
					if inv.Status == graph.InvocationWaiting {
						waitingID = inv.ID
					}
				}
			}
			if out.Status != graph.StatusWaiting || waitingID == "" {
				t.Fatalf("expected one waiting branch: %+v", out)
			}
			out, err = r.Recover(context.Background(), "rotation", []graph.ResumeInput{{InvocationID: waitingID, Payload: []byte("1")}}, opts)
			if err != nil || out.Status != graph.StatusCompleted || len(visits) != 18 || visits[17].branch != 1 || len(out.Checkpoint.Terminals) != 16 || len(out.Checkpoint.Groups) != 0 {
				t.Fatalf("continued branch: %+v visits=%v, %v", out, visits, err)
			}
			for _, terminal := range out.Checkpoint.Terminals {
				s := terminal.State
				want := 1
				if s.Values["branch"] < 2 {
					want = 2
				}
				if s.Owner != "rotation" || s.Round != want || (s.Values["branch"] == 1 && s.Values["inputs"] != 1) {
					t.Fatalf("mixed branch state: %+v", terminal)
				}
			}
		})
	}
}
