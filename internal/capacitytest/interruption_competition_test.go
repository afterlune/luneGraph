package capacitytest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

type competitionContextKey struct{}
type competitionResult struct {
	out graph.Result[state]
	err error
}

func TestCapacityInterruptionRecoveryCompetition(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		for _, mode := range []string{"both-interrupt", "both-commit", "complete-before-interrupt", "fail-before-interrupt"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				arrivals := make(chan graph.CallInfo, 2)
				release := make(chan struct{})
				var releaseOnce sync.Once
				defer releaseOnce.Do(func() { close(release) })
				firstRelease := make(chan struct{})
				var firstOnce sync.Once
				defer firstOnce.Do(func() { close(firstRelease) })
				boom := errors.New("competing failure")
				g := graph.New[state]("work")
				if err := g.AddNode(graph.NodeSpec[state]{Name: "work", OnError: graph.FailExecution, Run: func(ctx context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
					participant, _ := ctx.Value(competitionContextKey{}).(int)
					if participant == 0 {
						return graph.Transition[state]{}, graph.Interrupt(nil)
					}
					arrivals <- call
					select {
					case <-release:
					case <-ctx.Done():
						return graph.Transition[state]{}, ctx.Err()
					}
					if participant == 1 && (mode == "complete-before-interrupt" || mode == "fail-before-interrupt") {
						select {
						case <-firstRelease:
						case <-ctx.Done():
							return graph.Transition[state]{}, ctx.Err()
						}
					}
					if mode == "both-interrupt" || (participant == 1 && mode != "both-commit") {
						return graph.Transition[state]{}, graph.Interrupt(nil)
					}
					if mode == "fail-before-interrupt" {
						return graph.Transition[state]{}, boom
					}
					s.Total++
					return graph.EndExecution(s), nil
				}}); err != nil {
					t.Fatal(err)
				}
				r, store := compile(t, g), openStore(t, kind)
				seed, err := r.Start(ctx, "competition", initial("competition"), graph.Options[state]{Store: store})
				if !errors.Is(err, graph.ErrInterrupted) {
					t.Fatal(err)
				}
				// Both recoveries must enter the original callback before either can
				// submit a result. Separate release gates order the mixed outcomes.
				finished := make([]chan competitionResult, 2)
				for i := range 2 {
					finished[i] = make(chan competitionResult, 1)
					callCtx := context.WithValue(ctx, competitionContextKey{}, i+1)
					go func() {
						out, err := r.Recover(callCtx, "competition", nil, graph.Options[state]{Store: store})
						finished[i] <- competitionResult{out, err}
					}()
				}
				calls := make([]graph.CallInfo, 0, 2)
				for range 2 {
					select {
					case call := <-arrivals:
						calls = append(calls, call)
					case <-ctx.Done():
						t.Fatal("callbacks did not overlap")
					}
				}
				if calls[0].CallID != calls[1].CallID || calls[0].CallID != seed.Checkpoint.Invocations[0].CallID {
					t.Fatalf("identity changed: %+v", calls)
				}
				// Order mixed outcomes so the interrupted call returns an older revision.
				releaseOnce.Do(func() { close(release) })
				var results [2]competitionResult
				select {
				case results[1] = <-finished[1]:
				case <-ctx.Done():
					t.Fatal("second recovery hung")
				}
				firstOnce.Do(func() { close(firstRelease) })
				select {
				case results[0] = <-finished[0]:
				case <-ctx.Done():
					t.Fatal("first recovery hung")
				}
				saved, err := store.Load(ctx, "competition")
				if err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "both-interrupt":
					for _, result := range results {
						if result.out.Status != graph.StatusInterrupted || !errors.Is(result.err, graph.ErrInterrupted) {
							t.Fatalf("result=%+v", result)
						}
					}
					if saved.Revision != seed.Checkpoint.Revision || saved.Completed {
						t.Fatalf("interruptions wrote checkpoint: %+v", saved)
					}
				case "both-commit":
					success, conflict := 0, 0
					for _, result := range results {
						if result.err == nil && result.out.Status == graph.StatusCompleted {
							success++
						}
						if errors.Is(result.err, graph.ErrConflict) {
							conflict++
						}
					}
					if success != 1 || conflict != 1 || !saved.Completed || saved.Revision != 2 || saved.Final == nil || saved.Final.Total != 1 {
						t.Fatalf("CAS outcomes=%+v saved=%+v", results, saved)
					}
				default:
					first := results[0]
					if first.out.Status != graph.StatusInterrupted || !errors.Is(first.err, graph.ErrInterrupted) || first.out.Checkpoint.Revision != 1 || saved.Revision != 2 || !saved.Completed {
						t.Fatalf("stale interruption=%+v saved=%+v", first, saved)
					}
					_, err = r.Resume(ctx, first.out.Checkpoint, nil, graph.Options[state]{Store: store})
					if !errors.Is(err, graph.ErrConflict) {
						t.Fatalf("stale Resume=%v", err)
					}
					terminal, recoverErr := r.Recover(ctx, "competition", nil, graph.Options[state]{Store: store})
					if mode == "fail-before-interrupt" {
						if !errors.Is(results[1].err, boom) || !errors.Is(recoverErr, graph.ErrRunFailed) || terminal.Status != graph.StatusFailed {
							t.Fatalf("terminal=%+v %v", terminal, recoverErr)
						}
					} else if recoverErr != nil || terminal.Status != graph.StatusCompleted {
						t.Fatalf("terminal=%+v %v", terminal, recoverErr)
					}
				}
			})
		}
	}
}

func TestCapacityInterruptionWaitsForUncooperativeCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var active atomic.Int32
	g := graph.New[state]("fork")
	addNode(t, g, "fork", func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
		return graph.To(s, "slow", "stop"), nil
	})
	addNode(t, g, "slow", func(ctx context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
		active.Add(1)
		defer active.Add(-1)
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return graph.EndExecution(s), nil
	})
	addNode(t, g, "stop", func(ctx context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
		select {
		case <-started:
		case <-ctx.Done():
			return graph.Transition[state]{}, ctx.Err()
		}
		return graph.Transition[state]{}, graph.Interrupt(nil)
	})
	addEdge(t, g, "fork", "slow")
	addEdge(t, g, "fork", "stop")
	done := make(chan competitionResult, 1)
	r := compile(t, g)
	go func() {
		out, err := r.Start(ctx, "drain", initial("drain"), graph.Options[state]{MaxConcurrency: 2})
		done <- competitionResult{out, err}
	}()
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("sibling was not cancelled")
	}
	select {
	case result := <-done:
		t.Fatalf("returned before callback exit: %+v", result)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case result := <-done:
		if result.out.Status != graph.StatusInterrupted || !errors.Is(result.err, graph.ErrInterrupted) || active.Load() != 0 || result.out.Checkpoint.Revision != 2 {
			t.Fatalf("drain=%+v active=%d", result, active.Load())
		}
	case <-ctx.Done():
		t.Fatal("did not return after releasing callback")
	}
}
