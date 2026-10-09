package capacitytest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

type conflictStore struct {
	graph.Store[state]
	once sync.Once
}

func (s *conflictStore) CompareAndSwap(ctx context.Context, revision uint64, next graph.Checkpoint[state]) error {
	conflict := false
	if next.RunID == "conflict" {
		s.once.Do(func() { conflict = true })
	}
	if conflict {
		return graph.ErrConflict
	}
	return s.Store.CompareAndSwap(ctx, revision, next)
}

func TestSharedExecutionFailureIsolation(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			store := &conflictStore{Store: openStore(t, kind)}
			started := make(chan struct{})
			cancelledCallbackDone := make(chan struct{})
			var active atomic.Int32
			var replayMu sync.Mutex
			var replay []string
			var interruptedReplay []string
			boom := errors.New("deliberate failure")
			g := graph.New[state]("work")
			err := g.AddNode(graph.NodeSpec[state]{Name: "work", OnError: graph.FailExecution, Run: func(ctx context.Context, call graph.CallInfo, s state) (graph.Transition[state], error) {
				active.Add(1)
				defer active.Add(-1)
				if err := checkIdentity(call, s); err != nil {
					return graph.Transition[state]{}, err
				}
				s.Values["steps"]++
				s.Total++
				switch s.Owner {
				case "cancel":
					defer close(cancelledCallbackDone)
					close(started)
					<-ctx.Done()
					return graph.Transition[state]{}, ctx.Err()
				case "fail":
					return graph.Transition[state]{}, boom
				case "conflict":
					replayMu.Lock()
					replay = append(replay, call.CallID)
					replayMu.Unlock()
				case "interrupt":
					replayMu.Lock()
					interruptedReplay = append(interruptedReplay, call.CallID)
					first := len(interruptedReplay) == 1
					replayMu.Unlock()
					if first {
						return graph.EndExecution(s), graph.Interrupt(nil)
					}
				}
				return graph.EndExecution(s), nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			r := compile(t, g)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				select {
				case <-started:
					cancel()
				case <-ctx.Done():
				}
			}()
			pool := newBatchPool(64, 8, func(i int) error {
				id := fmt.Sprintf("ordinary-%d", i)
				callCtx := context.Background()
				switch i {
				case 0:
					id, callCtx = "cancel", ctx
				case 1:
					id = "fail"
				case 2:
					id = "conflict"
				case 3:
					id = "interrupt"
				}
				out, err := r.Start(callCtx, id, initial(id), graph.Options[state]{Store: store})
				saved, loadErr := store.Load(context.Background(), id)
				if loadErr != nil {
					return loadErr
				}
				switch id {
				case "cancel":
					select {
					case <-cancelledCallbackDone:
					default:
						return errors.New("cancelled callback outlived its public call")
					}
					if !errors.Is(err, context.Canceled) || saved.Completed || (saved.Failure != nil || saved.HadLocalFailures) || saved.Steps != 0 {
						return fmt.Errorf("cancellation affected checkpoint: %+v, %v", saved, err)
					}
				case "fail":
					if !errors.Is(err, boom) || !saved.Completed || saved.Failure == nil {
						return fmt.Errorf("failure terminal: %+v, %v", saved, err)
					}
				case "conflict", "interrupt":
					want := graph.ErrConflict
					if id == "interrupt" {
						want = graph.ErrInterrupted
					}
					if !errors.Is(err, want) || saved.Revision != 1 || saved.Completed || saved.Failure != nil || saved.HadLocalFailures {
						return fmt.Errorf("conflict wrote state: %+v, %v", saved, err)
					}
					out, err = r.Recover(context.Background(), id, nil, graph.Options[state]{Store: store})
					fallthrough
				default:
					if err != nil || out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || out.Checkpoint.Final.Owner != id || out.Checkpoint.Final.Total != 1 || out.Checkpoint.Final.Values["steps"] != 1 {
						return fmt.Errorf("unrelated run %s: %+v, %v", id, out, err)
					}
					saved, err = store.Load(context.Background(), id)
					if err != nil || !saved.Completed || saved.Final == nil || saved.Final.Owner != id || saved.Final.Total != 1 || saved.Final.Values["steps"] != 1 || saved.Revision != out.Checkpoint.Revision {
						return fmt.Errorf("stored run %s: %+v, %v", id, saved, err)
					}
				}
				return nil
			})
			err = pool.batch()
			pool.close()
			if err != nil {
				t.Fatal(err)
			}
			if active.Load() != 0 || len(replay) != 2 || replay[0] == "" || replay[0] != replay[1] {
				t.Fatalf("callbacks did not drain or replay identity changed: active=%d replay=%v", active.Load(), replay)
			}
			if len(interruptedReplay) != 2 || interruptedReplay[0] == "" || interruptedReplay[0] != interruptedReplay[1] {
				t.Fatalf("interruption replay identity: %v", interruptedReplay)
			}
		})
	}
}
