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

	graph "github.com/afterlune/luneGraph"
)

func TestMainEntryPoint(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "main.db")
	output := callDurableMain(t, []string{"start", "-db", dbPath, "-run", "main-run"})
	if !strings.Contains(output, "status=waiting run=main-run") {
		t.Fatalf("main start output = %q", output)
	}
	output = callDurableMain(t, []string{"resume", "-db", dbPath, "-run", "main-run", "-value", "8"})
	if !strings.Contains(output, "status=completed run=main-run value=8") {
		t.Fatalf("main resume output = %q", output)
	}
}

func callDurableMain(t *testing.T, args []string) string {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldArgs, oldStdout := os.Args, os.Stdout
	os.Args = append([]string{"durable"}, args...)
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

func TestDurableWorkflow(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_runs.db")

	// 1. Invalid commands & argument checks
	if err := run(context.Background(), nil, nil, nil); err == nil {
		t.Fatal("expected error with no args")
	}
	if err := run(context.Background(), []string{"invalid"}, nil, nil); err == nil {
		t.Fatal("expected error with invalid command")
	}
	if err := run(context.Background(), []string{"start", "-extra"}, nil, nil); err == nil {
		t.Fatal("expected error with unexpected flags")
	}
	if err := run(context.Background(), []string{"start", "extra_arg"}, nil, nil); err == nil {
		t.Fatal("expected error with unexpected positional args")
	}

	// 2. Start command
	var startOut, startDiag bytes.Buffer
	err := run(context.Background(), []string{"start", "-db", dbPath, "-run", "test-run", "-observe"}, &startOut, &startDiag)
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if !strings.Contains(startOut.String(), "status=waiting run=test-run") {
		t.Fatalf("unexpected start output: %s", startOut.String())
	}
	if startDiag.Len() == 0 {
		t.Fatal("expected observation output on stderr with -observe")
	}

	// 3. Resume command
	var resumeOut, resumeDiag bytes.Buffer
	err = run(context.Background(), []string{"resume", "-db", dbPath, "-run", "test-run", "-value", "42", "-observe"}, &resumeOut, &resumeDiag)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if !strings.Contains(resumeOut.String(), "status=completed run=test-run value=42") {
		t.Fatalf("unexpected resume output: %s", resumeOut.String())
	}

	// 4. Resume completed run
	err = run(context.Background(), []string{"resume", "-db", dbPath, "-run", "test-run", "-value", "10"}, &resumeOut, &resumeDiag)
	if !errors.Is(err, graph.ErrRunCompleted) {
		t.Fatalf("expected ErrRunCompleted, got: %v", err)
	}

	// 5. Resume non-existent run
	err = run(context.Background(), []string{"resume", "-db", dbPath, "-run", "non-existent", "-value", "10"}, &resumeOut, &resumeDiag)
	if err == nil {
		t.Fatal("expected error for non-existent run")
	}
}

func TestWaitingInvocation(t *testing.T) {
	// Completed
	_, err := waitingInvocation(graph.Checkpoint[state]{Completed: true})
	if !errors.Is(err, graph.ErrRunCompleted) {
		t.Fatalf("got %v, want ErrRunCompleted", err)
	}

	// No waiting value invocation
	_, err = waitingInvocation(graph.Checkpoint[state]{
		RunID: "run-empty",
		Invocations: []graph.Invocation[state]{
			{Status: graph.InvocationReady},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "has no waiting value invocation") {
		t.Fatalf("unexpected err: %v", err)
	}
}

type failingOutput struct{ err error }

func (w failingOutput) Write([]byte) (int, error) { return 0, w.err }

func TestRunReportsDatabaseAndOutputFailures(t *testing.T) {
	missingDB := filepath.Join(t.TempDir(), "missing", "runs.db")
	if err := run(context.Background(), []string{"start", "-db", missingDB}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("start accepted a database path with a missing parent")
	}

	writeErr := errors.New("output unavailable")
	err := run(context.Background(), []string{"start", "-db", filepath.Join(t.TempDir(), "runs.db")}, failingOutput{writeErr}, &bytes.Buffer{})
	if !errors.Is(err, writeErr) {
		t.Fatalf("run output error = %v", err)
	}
}
