package graph_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/memory"
	"lune-graph/checkpoint/sqlite"
)

type concurrentRunState struct {
	Values map[string]int
}

func cloneConcurrentRunState(state concurrentRunState) (concurrentRunState, error) {
	values := make(map[string]int, len(state.Values))
	for key, value := range state.Values {
		values[key] = value
	}
	return concurrentRunState{Values: values}, nil
}

func TestConcurrentRunsShareRunnerAndStore(t *testing.T) {
	const runCount = 16
	for _, persistence := range []string{"none", "memory", "sqlite"} {
		t.Run(persistence, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var store graph.Store[concurrentRunState]
			var closeStore func() error
			switch persistence {
			case "memory":
				memoryStore, err := memory.New[concurrentRunState](cloneConcurrentRunState)
				if err != nil {
					t.Fatal(err)
				}
				store = memoryStore
			case "sqlite":
				sqliteStore, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "runs.db"), checkpoint.JSON[concurrentRunState]{})
				if err != nil {
					t.Fatal(err)
				}
				store = sqliteStore
				closeStore = sqliteStore.Close
			}
			if closeStore != nil {
				t.Cleanup(func() {
					if err := closeStore(); err != nil {
						t.Errorf("close shared store: %v", err)
					}
				})
			}

			entered := make(chan string, runCount)
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseCallbacks := func() { releaseOnce.Do(func() { close(release) }) }
			g := graph.New[concurrentRunState]("finish")
			if err := g.AddNode(graph.NodeSpec[concurrentRunState]{Name: "finish", Run: func(ctx context.Context, call graph.CallInfo, state concurrentRunState) (graph.Transition[concurrentRunState], error) {
				select {
				case entered <- call.RunID:
				case <-ctx.Done():
					return graph.Transition[concurrentRunState]{}, ctx.Err()
				}
				select {
				case <-release:
				case <-ctx.Done():
					return graph.Transition[concurrentRunState]{}, ctx.Err()
				}
				state.Values["value"]++
				return graph.EndExecution(state), nil
			}}); err != nil {
				t.Fatal(err)
			}
			runner, err := g.Compile(graph.Config[concurrentRunState]{MachineID: "concurrent-runs-v1", Clone: cloneConcurrentRunState})
			if err != nil {
				t.Fatal(err)
			}

			type runOutcome struct {
				runID  string
				result graph.Result[concurrentRunState]
				err    error
			}
			outcomes := make(chan runOutcome, runCount)
			var workers sync.WaitGroup
			for i := range runCount {
				runID := fmt.Sprintf("run-%02d", i)
				initial := concurrentRunState{Values: map[string]int{"run": i, "value": i}}
				workers.Add(1)
				go func() {
					defer workers.Done()
					result, err := runner.Start(ctx, runID, initial, graph.Options[concurrentRunState]{Store: store})
					outcomes <- runOutcome{runID: runID, result: result, err: err}
				}()
			}
			defer func() {
				cancel()
				releaseCallbacks()
				workers.Wait()
			}()

			enteredRuns := make(map[string]bool, runCount)
			for range runCount {
				select {
				case runID := <-entered:
					if enteredRuns[runID] {
						t.Fatalf("run %q entered its callback more than once", runID)
					}
					enteredRuns[runID] = true
				case <-ctx.Done():
					t.Fatalf("only %d of %d callbacks entered: %v", len(enteredRuns), runCount, ctx.Err())
				}
			}
			releaseCallbacks()

			resultsByRun := make(map[string]graph.Result[concurrentRunState], runCount)
			for range runCount {
				select {
				case outcome := <-outcomes:
					if outcome.err != nil {
						t.Fatalf("run %q failed: %v", outcome.runID, outcome.err)
					}
					if outcome.result.Status != graph.StatusCompleted || outcome.result.Checkpoint.Final == nil || outcome.result.Checkpoint.Revision != 2 || outcome.result.Checkpoint.Steps != 1 {
						t.Fatalf("run %q result = %+v", outcome.runID, outcome.result)
					}
					resultsByRun[outcome.runID] = outcome.result
				case <-ctx.Done():
					t.Fatalf("only %d of %d runs completed: %v", len(resultsByRun), runCount, ctx.Err())
				}
			}

			for i := range runCount {
				runID := fmt.Sprintf("run-%02d", i)
				result, exists := resultsByRun[runID]
				if !exists {
					t.Fatalf("missing result for %q", runID)
				}
				if result.Checkpoint.RunID != runID || result.Checkpoint.Final.Values["run"] != i || result.Checkpoint.Final.Values["value"] != i+1 {
					t.Fatalf("run %q state was mixed: %+v", runID, *result.Checkpoint.Final)
				}
				if store == nil {
					continue
				}
				stored, err := store.Load(ctx, runID)
				if err != nil {
					t.Fatalf("load run %q: %v", runID, err)
				}
				if stored.RunID != runID || stored.MachineID != "concurrent-runs-v1" || stored.Revision != 2 || stored.Revision != result.Checkpoint.Revision || !stored.Completed || stored.Final == nil || stored.Final.Values["run"] != i || stored.Final.Values["value"] != i+1 {
					t.Fatalf("stored run %q checkpoint = %+v", runID, stored)
				}
			}
		})
	}
}
