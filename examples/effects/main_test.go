package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMainEntryPoint(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "main-checkpoints.db")
	effectPath := filepath.Join(dir, "main-effects.db")
	args := []string{"start", "-checkpoints", checkpointPath, "-effects", effectPath, "-run", "main-run", "-delta", "5"}
	if output := callEffectsMain(t, args); !strings.Contains(output, "status=completed run=main-run value=5") {
		t.Fatalf("main start output = %q", output)
	}
	args = []string{"recover", "-checkpoints", checkpointPath, "-effects", effectPath, "-run", "main-run"}
	if output := callEffectsMain(t, args); !strings.Contains(output, "status=completed run=main-run value=5") {
		t.Fatalf("main recover output = %q", output)
	}
}

func callEffectsMain(t *testing.T, args []string) string {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldArgs, oldStdout := os.Args, os.Stdout
	os.Args = append([]string{"effects"}, args...)
	os.Stdout = write
	defer func() {
		os.Args, os.Stdout = oldArgs, oldStdout
		_ = read.Close()
		_ = write.Close()
	}()
	main()
	if err := write.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	return string(output)
}

func TestCommandsPreserveOriginalResult(t *testing.T) {
	dir := t.TempDir()
	paths := []string{"-checkpoints", filepath.Join(dir, "runs.db"), "-effects", filepath.Join(dir, "effects.db")}
	for _, command := range []struct {
		args []string
		want string
	}{
		{[]string{"start", "-run", "first", "-delta", "3"}, "status=completed run=first value=3\n"},
		{[]string{"start", "-run", "second", "-delta", "4"}, "status=completed run=second value=7\n"},
		{[]string{"recover", "-run", "first"}, "status=completed run=first value=3\n"},
		{[]string{"recover", "-run", "second"}, "status=completed run=second value=7\n"},
	} {
		var output, diagnostics bytes.Buffer
		args := append(append([]string(nil), command.args...), paths...)
		check(t, run(context.Background(), args, &output, &diagnostics))
		if output.String() != command.want {
			t.Fatalf("%v: %q, want %q", command.args, output.String(), command.want)
		}
	}
	assertEffects(t, paths[3], 7, 2)
}

func TestCommandsRejectInvalidArguments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same.db")
	for _, args := range [][]string{nil, {"resume"}, {"start", "extra"}, {"recover", "-delta", "3"}, {"start", "-checkpoints", path, "-effects", path}} {
		var output, diagnostics bytes.Buffer
		if err := run(context.Background(), args, &output, &diagnostics); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	var output, diagnostics bytes.Buffer
	err := run(context.Background(), []string{"start", "-checkpoints", path, "-effects", path}, &output, &diagnostics)
	if err == nil || !strings.Contains(err.Error(), "separate files") {
		t.Fatalf("shared file error: %v", err)
	}
}

type failingOutput struct{ err error }

func (w failingOutput) Write([]byte) (int, error) { return 0, w.err }

func TestCommandsReportStoreAndOutputFailures(t *testing.T) {
	dir := t.TempDir()
	checkpointPath := filepath.Join(dir, "checkpoints.db")
	missingEffectPath := filepath.Join(dir, "missing", "effects.db")
	args := []string{"start", "-checkpoints", checkpointPath, "-effects", missingEffectPath}
	if err := run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("start accepted an effects database path with a missing parent")
	}
	missingCheckpointPath := filepath.Join(dir, "missing-checkpoint-parent", "checkpoints.db")
	args = []string{"start", "-checkpoints", missingCheckpointPath, "-effects", filepath.Join(dir, "unused-effects.db")}
	if err := run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("start accepted a checkpoint database path with a missing parent")
	}

	writeErr := errors.New("output unavailable")
	args = []string{"start", "-checkpoints", filepath.Join(t.TempDir(), "checkpoints.db"), "-effects", filepath.Join(t.TempDir(), "effects.db")}
	if err := run(context.Background(), args, failingOutput{writeErr}, &bytes.Buffer{}); !errors.Is(err, writeErr) {
		t.Fatalf("run output error = %v", err)
	}
}
