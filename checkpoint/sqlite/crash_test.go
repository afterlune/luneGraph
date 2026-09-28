package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/sqlite"
)

func crashRunner(t *testing.T, marker string, block bool) *graph.Runner[int] {
	t.Helper()
	g := graph.New[int]("work")
	err := g.AddNode(graph.NodeSpec[int]{
		Name: "work",
		Run: func(ctx context.Context, state int) (graph.Transition[int], error) {
			file, err := os.OpenFile(marker, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				return graph.Transition[int]{}, err
			}
			if _, err = file.Write([]byte("x")); err == nil {
				err = file.Sync()
			}
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				return graph.Transition[int]{}, err
			}
			if block {
				select {
				case <-ctx.Done():
					return graph.Transition[int]{}, ctx.Err()
				case <-time.After(time.Hour):
					return graph.Transition[int]{}, errors.New("node was not interrupted")
				}
			}
			return graph.EndExecution(state + 1), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[int]{
		MachineID: "crash-recovery-v1",
		Clone:     func(state int) (int, error) { return state, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestCrashProcessHelper(t *testing.T) {
	if os.Getenv("LUNE_CHECKPOINT_CRASH_HELPER") != "1" {
		return
	}
	ctx := context.Background()
	store, err := sqlite.Open(ctx, os.Getenv("LUNE_CHECKPOINT_CRASH_DB"), checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runner := crashRunner(t, os.Getenv("LUNE_CHECKPOINT_CRASH_MARKER"), true)
	if _, err := runner.Start(ctx, "crash-run", 0, graph.Options[int]{Store: store}); err != nil {
		t.Fatal(err)
	}
	t.Fatal("blocked node returned before process termination")
}

func TestCrashRecoveryReplaysUncommittedNode(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "runs.db")
	marker := filepath.Join(dir, "starts")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashProcessHelper$")
	cmd.Env = append(os.Environ(),
		"LUNE_CHECKPOINT_CRASH_HELPER=1",
		"LUNE_CHECKPOINT_CRASH_DB="+dbPath,
		"LUNE_CHECKPOINT_CRASH_MARKER="+marker,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	deadline := time.Now().Add(30 * time.Second)
	for {
		data, err := os.ReadFile(marker)
		if err == nil && len(data) == 1 {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("child did not start the node: %s", stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		waitErr := cmd.Wait()
		t.Fatalf("kill child: %v; child exit: %v; stderr: %s", err, waitErr, stderr.String())
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child exited normally")
	}

	ctx := context.Background()
	store, err := sqlite.Open(ctx, dbPath, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	saved, err := store.Load(ctx, "crash-run")
	if err != nil {
		t.Fatal(err)
	}
	if saved.Revision != 1 || saved.Steps != 0 || len(saved.Invocations) != 1 || saved.Invocations[0].Status != graph.InvocationReady {
		t.Fatalf("checkpoint after crash = %+v", saved)
	}
	runner := crashRunner(t, marker, false)
	result, err := runner.Recover(ctx, "crash-run", nil, graph.Options[int]{Store: store})
	if err != nil || result.Status != graph.StatusCompleted || result.Checkpoint.Revision != 2 || result.Checkpoint.Steps != 1 || result.Checkpoint.Final == nil || *result.Checkpoint.Final != 1 {
		t.Fatalf("recovery result = %+v, %v", result, err)
	}
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "xx" {
		t.Fatalf("node starts = %q, %v", data, err)
	}
	committed, err := store.Load(ctx, "crash-run")
	if err != nil || committed.Revision != 2 || committed.Final == nil || *committed.Final != 1 {
		t.Fatalf("committed recovery = %+v, %v", committed, err)
	}
}
