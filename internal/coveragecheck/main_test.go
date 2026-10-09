package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunChecksAllDiscoveredProductionPackages(t *testing.T) {
	t.Chdir("../..")
	packages, err := listProduction()
	if err != nil {
		t.Fatal(err)
	}
	var data strings.Builder
	data.WriteString("mode: set\n")
	for _, name := range packages {
		fmt.Fprintf(&data, "%s/a.go:1.1,2.1 1 1\n", name)
	}
	filename := filepath.Join(t.TempDir(), "coverage.out")
	if err := os.WriteFile(filename, []byte(data.String()), 0600); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	if err := run([]string{"-profile", filename}, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), "PASS") != len(packages) {
		t.Fatal(output.String())
	}
	if strings.Contains(output.String(), "internal/storetest") || strings.Contains(output.String(), "internal/coveragecheck") || !strings.Contains(output.String(), "examples/effects") {
		t.Fatal(output.String())
	}
	if err := os.WriteFile(filename, []byte("mode: set\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-profile", filename}, io.Discard); err == nil {
		t.Fatal("accepted missing package coverage")
	}
	if err := os.WriteFile(filename, []byte("bad profile\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-profile", filename}, io.Discard); err == nil {
		t.Fatal("accepted malformed profile")
	}
}

func TestRunErrors(t *testing.T) {
	for _, args := range [][]string{nil, {"-bad"}, {"-profile", "missing", "extra"}, {"-profile", filepath.Join(t.TempDir(), "missing")}} {
		if err := run(args, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	t.Setenv("PATH", t.TempDir())
	if _, err := listProduction(); err == nil {
		t.Fatal("ignored go list failure")
	}
}
