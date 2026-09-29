package sqlite_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/memory"
	"lune-graph/checkpoint/sqlite"
)

type benchmarkState struct {
	Values map[string]int
}

func BenchmarkCheckpointCAS(b *testing.B) {
	for _, kind := range []string{"memory", "sqlite"} {
		b.Run(kind, func(b *testing.B) {
			var store graph.Store[benchmarkState]
			var closeStore func() error
			switch kind {
			case "memory":
				memoryStore, err := memory.New[benchmarkState](cloneBenchmarkState)
				if err != nil {
					b.Fatal(err)
				}
				store = memoryStore
			case "sqlite":
				sqliteStore, err := sqlite.Open(context.Background(), filepath.Join(b.TempDir(), "cas.db"), checkpoint.JSON[benchmarkState]{})
				if err != nil {
					b.Fatal(err)
				}
				store = sqliteStore
				closeStore = sqliteStore.Close
			}
			if closeStore != nil {
				defer func() {
					if err := closeStore(); err != nil {
						b.Error(err)
					}
				}()
			}

			runner := benchmarkCheckpointRunner(b)
			seed, err := runner.Start(context.Background(), "cas", newBenchmarkState(), graph.Options[benchmarkState]{Store: store, MaxSteps: 1})
			if err != nil || seed.Status != graph.StatusBudget || len(seed.Checkpoint.Invocations) != 1 {
				b.Fatalf("seed checkpoint = %+v, %v", seed, err)
			}
			current := seed.Checkpoint
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				next := current
				next.Revision++
				next.Invocations = append([]graph.Invocation[benchmarkState](nil), current.Invocations...)
				next.Invocations[0].State = cloneBenchmarkStateValue(current.Invocations[0].State)
				next.Invocations[0].State.Values["value-0"]++
				if err := store.CompareAndSwap(context.Background(), current.Revision, next); err != nil {
					b.Fatal(err)
				}
				if next.Revision != current.Revision+1 || len(next.Invocations[0].State.Values) != benchmarkStateSize {
					b.Fatalf("invalid next checkpoint: revision=%d state size=%d", next.Revision, len(next.Invocations[0].State.Values))
				}
				current = next
			}
		})
	}
}

func BenchmarkSQLiteReopenRecover(b *testing.B) {
	path := filepath.Join(b.TempDir(), "recover.db")
	runner := benchmarkRecoveryRunner(b)
	seedSQLiteWaitingRun(b, path, runner)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store, err := sqlite.Open(context.Background(), path, checkpoint.JSON[benchmarkState]{})
		if err != nil {
			b.Fatal(err)
		}
		result, recoverErr := runner.Recover(context.Background(), "cold-recovery", []graph.ResumeInput{{InvocationID: "i1", Payload: []byte("1")}}, graph.Options[benchmarkState]{Store: store})
		closeErr := store.Close()
		if recoverErr != nil || result.Status != graph.StatusCompleted || result.Checkpoint.Final == nil || (*result.Checkpoint.Final).Values["value-0"] != 1 {
			b.Fatalf("recovered result = status %s, checkpoint revision %d, error %v", result.Status, result.Checkpoint.Revision, recoverErr)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
		if i+1 == b.N {
			continue
		}

		b.StopTimer()
		if err := removeSQLiteFiles(path); err != nil {
			b.Fatal(err)
		}
		seedSQLiteWaitingRun(b, path, runner)
		b.StartTimer()
	}
}

const benchmarkStateSize = 16

func newBenchmarkState() benchmarkState {
	values := make(map[string]int, benchmarkStateSize)
	for i := range benchmarkStateSize {
		values["value-"+strconv.FormatInt(int64(i), 10)] = i
	}
	return benchmarkState{Values: values}
}

func cloneBenchmarkState(value benchmarkState) (benchmarkState, error) {
	return cloneBenchmarkStateValue(value), nil
}

func cloneBenchmarkStateValue(value benchmarkState) benchmarkState {
	cloned := benchmarkState{Values: make(map[string]int, len(value.Values))}
	for key, item := range value.Values {
		cloned.Values[key] = item
	}
	return cloned
}

func benchmarkCheckpointRunner(b *testing.B) *graph.Runner[benchmarkState] {
	b.Helper()
	g := graph.New[benchmarkState]("step")
	if err := g.AddNode(graph.NodeSpec[benchmarkState]{Name: "step", Run: func(_ context.Context, _ graph.CallInfo, state benchmarkState) (graph.Transition[benchmarkState], error) {
		state.Values["value-0"]++
		return graph.To(state, "step"), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddEdge("step", "step"); err != nil {
		b.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[benchmarkState]{MachineID: "benchmark-cas-v1", Clone: cloneBenchmarkState})
	if err != nil {
		b.Fatal(err)
	}
	return runner
}

func benchmarkRecoveryRunner(b *testing.B) *graph.Runner[benchmarkState] {
	b.Helper()
	g := graph.New[benchmarkState]("wait")
	if err := g.AddNode(graph.NodeSpec[benchmarkState]{Name: "wait", Run: func(_ context.Context, _ graph.CallInfo, state benchmarkState) (graph.Transition[benchmarkState], error) {
		return graph.Wait(state, "add", "done"), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddNode(graph.NodeSpec[benchmarkState]{Name: "done", Run: func(_ context.Context, _ graph.CallInfo, state benchmarkState) (graph.Transition[benchmarkState], error) {
		return graph.EndExecution(state), nil
	}}); err != nil {
		b.Fatal(err)
	}
	if err := g.AddEdge("wait", "done"); err != nil {
		b.Fatal(err)
	}
	if err := graph.RegisterContinuation(g, "add", func(payload []byte) (int, error) {
		return strconv.Atoi(string(payload))
	}, func(_ context.Context, _ graph.CallInfo, state benchmarkState, amount int) (benchmarkState, error) {
		state.Values["value-0"] += amount
		return state, nil
	}); err != nil {
		b.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[benchmarkState]{MachineID: "benchmark-recovery-v1", Clone: cloneBenchmarkState})
	if err != nil {
		b.Fatal(err)
	}
	return runner
}

func seedSQLiteWaitingRun(b *testing.B, path string, runner *graph.Runner[benchmarkState]) {
	b.Helper()
	store, err := sqlite.Open(context.Background(), path, checkpoint.JSON[benchmarkState]{})
	if err != nil {
		b.Fatal(err)
	}
	result, runErr := runner.Start(context.Background(), "cold-recovery", newBenchmarkState(), graph.Options[benchmarkState]{Store: store})
	closeErr := store.Close()
	if runErr != nil || result.Status != graph.StatusWaiting || result.Checkpoint.Revision != 2 {
		b.Fatalf("seed waiting run = status %s, revision %d, error %v", result.Status, result.Checkpoint.Revision, runErr)
	}
	if closeErr != nil {
		b.Fatal(closeErr)
	}
}

func removeSQLiteFiles(path string) error {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		err := os.Remove(path + suffix)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
