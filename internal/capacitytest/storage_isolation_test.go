package capacitytest

import (
	"context"
	graph "github.com/afterlune/luneGraph"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCapacityDeleteManyIsolation(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			store := openStore(t, kind)
			r := waitingRunner(t)
			seed, err := r.Start(ctx, "active", initial("active"), graph.Options[state]{Store: store})
			if err != nil {
				t.Fatal(err)
			}
			complete := compile(t, chainGraph(t, 2))
			for _, id := range []string{"old-a", "old-b"} {
				if _, err := complete.Start(ctx, id, initial(id), graph.Options[state]{Store: store}); err != nil {
					t.Fatal(err)
				}
			}
			started, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			opts := graph.Options[state]{Store: store, Observer: graph.ObserverFunc(func(ctx context.Context, e graph.Event) {
				if e.Operation == graph.OperationNode && e.Phase == graph.PhaseStarted {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
					}
				}
			})}
			var workers sync.WaitGroup
			workers.Go(func() { _, err := r.Recover(ctx, "active", interruptionInputs(seed.Checkpoint), opts); done <- err })
			t.Cleanup(func() { cancel(); workers.Wait() })
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := store.DeleteMany(ctx, []string{"old-a", "old-b"}); err != nil {
				t.Fatal(err)
			}
			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			cp, err := store.Load(ctx, "active")
			want := initial("active")
			want.Values["sum"] = 2
			want.Values["inputs"] = 1
			if err != nil || len(cp.Invocations) != 1 || !reflect.DeepEqual(cp.Invocations[0].State.Values, want.Values) {
				t.Fatalf("active isolation: %+v %v", cp, err)
			}
		})
	}
}
