package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	graph "lune-graph"
	"lune-graph/checkpoint"
	"lune-graph/checkpoint/sqlite"
)

func TestCASProcessHelper(t *testing.T) {
	if os.Getenv("LUNE_CHECKPOINT_CAS_HELPER") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := sqlite.Open(ctx, os.Getenv("LUNE_CHECKPOINT_DB"), checkpoint.JSON[int]{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	defer store.Close()
	if err := os.WriteFile(os.Getenv("LUNE_CHECKPOINT_READY"), []byte("ready"), 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	for {
		if _, err := os.Stat(os.Getenv("LUNE_CHECKPOINT_GO")); err == nil {
			break
		}
		if ctx.Err() != nil {
			fmt.Fprintln(os.Stderr, "timed out waiting for CAS signal")
			os.Exit(3)
		}
		time.Sleep(10 * time.Millisecond)
	}
	value, _ := strconv.Atoi(os.Getenv("LUNE_CHECKPOINT_VALUE"))
	next := graph.Checkpoint[int]{FormatVersion: graph.CheckpointFormatVersion, RunID: "race", MachineID: "machine-v1", Revision: 2, Steps: uint64(value)}
	err = store.CompareAndSwap(ctx, 1, next)
	if err == nil {
		os.Exit(0)
	}
	if errors.Is(err, graph.ErrConflict) {
		os.Exit(2)
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(3)
}

func TestCrossProcessCompareAndSwap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.db")
	store, err := sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	initial := graph.Checkpoint[int]{FormatVersion: graph.CheckpointFormatVersion, RunID: "race", MachineID: "machine-v1", Revision: 1}
	if err := store.Create(ctx, initial); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	startFile := filepath.Join(dir, "go")
	type child struct {
		cmd    *exec.Cmd
		ready  string
		stderr bytes.Buffer
	}
	children := make([]*child, 0, 2)
	defer func() {
		for _, item := range children {
			if item.cmd.ProcessState == nil {
				_ = item.cmd.Process.Kill()
				_ = item.cmd.Wait()
			}
		}
	}()
	for value := 1; value <= 2; value++ {
		item := &child{ready: filepath.Join(dir, fmt.Sprintf("ready-%d", value))}
		item.cmd = exec.Command(os.Args[0], "-test.run=^TestCASProcessHelper$")
		item.cmd.Env = append(os.Environ(),
			"LUNE_CHECKPOINT_CAS_HELPER=1",
			"LUNE_CHECKPOINT_DB="+path,
			"LUNE_CHECKPOINT_READY="+item.ready,
			"LUNE_CHECKPOINT_GO="+startFile,
			"LUNE_CHECKPOINT_VALUE="+strconv.Itoa(value),
		)
		item.cmd.Stdout = io.Discard
		item.cmd.Stderr = &item.stderr
		if err := item.cmd.Start(); err != nil {
			t.Fatal(err)
		}
		children = append(children, item)
	}
	deadline := time.Now().Add(30 * time.Second)
	for _, item := range children {
		for {
			if _, err := os.Stat(item.ready); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("child did not become ready: %s", item.stderr.String())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := os.WriteFile(startFile, []byte("go"), 0600); err != nil {
		t.Fatal(err)
	}
	var succeeded, conflicted int
	for _, item := range children {
		err := item.cmd.Wait()
		if err == nil {
			succeeded++
			continue
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 2 {
			conflicted++
			continue
		}
		t.Fatalf("CAS child failed: %v: %s", err, item.stderr.String())
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("CAS outcomes: %d success, %d conflict", succeeded, conflicted)
	}
	store, err = sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stored, err := store.Load(ctx, "race")
	if err != nil || stored.Revision != 2 || (stored.Steps != 1 && stored.Steps != 2) {
		t.Fatalf("stored winner = %+v, %v", stored, err)
	}
}
