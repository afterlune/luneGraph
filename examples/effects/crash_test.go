package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
	"github.com/afterlune/luneGraph/examples/effects/internal/ledger"
)

func TestEffectCrashHelper(t *testing.T) {
	if os.Getenv("LUNE_EFFECT_CRASH_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	store, err := sqlite.Open(ctx, os.Getenv("LUNE_EFFECT_CHECKPOINTS"), checkpoint.JSON[state]{})
	check(t, err)
	defer store.Close()
	l, err := ledger.Open(ctx, os.Getenv("LUNE_EFFECT_DATABASE"))
	check(t, err)
	defer l.Close()
	add := effect(l)
	r, err := newRunner(func(ctx context.Context, call graph.CallInfo, delta int64) (int64, error) {
		value, err := add(ctx, call, delta)
		if err != nil {
			return 0, err
		}
		// The receipt transaction has committed. No transition has reached Runner.
		if _, err := fmt.Fprintln(os.Stdout, "effect-committed "+call.CallID); err != nil {
			return 0, err
		}
		<-ctx.Done()
		return value, ctx.Err()
	})
	check(t, err)
	_, err = r.Start(ctx, "crash", state{Delta: 3}, graph.Options[state]{Store: store})
	t.Fatalf("blocked callback returned: %v", err)
}

func TestRecoveryAfterEffectCommitAndProcessCrash(t *testing.T) {
	dir := t.TempDir()
	cpPath, effectPath := filepath.Join(dir, "runs.db"), filepath.Join(dir, "effects.db")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEffectCrashHelper$")
	cmd.Env = append(os.Environ(), "LUNE_EFFECT_CRASH_HELPER=1", "LUNE_EFFECT_CHECKPOINTS="+cpPath, "LUNE_EFFECT_DATABASE="+effectPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	check(t, err)
	check(t, cmd.Start())
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
		} else {
			ready <- ""
		}
	}()
	var line string
	select {
	case line = <-ready:
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("child did not commit effect: %s", stderr.String())
	}
	const prefix = "effect-committed "
	if !strings.HasPrefix(line, prefix) || len(line) == len(prefix) {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("invalid commit notification %q: %s", line, stderr.String())
	}
	callID := strings.TrimPrefix(line, prefix)
	check(t, cmd.Process.Kill())
	if err := cmd.Wait(); err == nil {
		t.Fatal("child exited normally")
	}
	store, err := sqlite.Open(context.Background(), cpPath, checkpoint.JSON[state]{})
	check(t, err)
	defer store.Close()
	l, err := ledger.Open(context.Background(), effectPath)
	check(t, err)
	defer l.Close()
	saved, err := store.Load(context.Background(), "crash")
	check(t, err)
	if saved.Revision != 1 || saved.Steps != 0 || len(saved.Invocations) != 1 || saved.Invocations[0].CallID != callID || saved.Invocations[0].Status != graph.InvocationReady {
		t.Fatalf("checkpoint after crash: %+v", saved)
	}
	assertEffects(t, effectPath, 3, 1)
	var calls []graph.CallInfo
	add := effect(l)
	r, err := newRunner(func(ctx context.Context, call graph.CallInfo, delta int64) (int64, error) {
		calls = append(calls, call)
		return add(ctx, call, delta)
	})
	check(t, err)
	out, err := r.Recover(context.Background(), "crash", nil, graph.Options[state]{Store: store})
	check(t, err)
	if out.Status != graph.StatusCompleted || out.Checkpoint.Final == nil || out.Checkpoint.Final.Value != 3 || len(calls) != 1 || calls[0].RunID != "crash" || calls[0].CallID != callID {
		t.Fatalf("recovery=%+v calls=%+v", out, calls)
	}
	committed, err := store.Load(context.Background(), "crash")
	check(t, err)
	if !committed.Completed || committed.Final == nil || committed.Final.Value != 3 || committed.Revision != 2 {
		t.Fatalf("stored recovery: %+v", committed)
	}
	assertEffects(t, effectPath, 3, 1)
}
