package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

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
