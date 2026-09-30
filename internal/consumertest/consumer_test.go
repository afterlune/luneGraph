package consumertest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestIndependentModuleDurableExample(t *testing.T) {
	const modulePath = "github.com/afterlune/luneGraph"
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(runCommand(t, repoRoot, "go", "list", "-m")); got != modulePath {
		t.Fatalf("module path = %q, want %q", got, modulePath)
	}
	consumerDir := filepath.Join(t.TempDir(), "consumer module")
	if err := os.Mkdir(consumerDir, 0700); err != nil {
		t.Fatal(err)
	}
	module := "module example.com/lunegraph-consumer-check\n\ngo 1.26.5\n\nrequire " + modulePath + " v0.0.0\n\nreplace " + modulePath + " => " + strconv.Quote(filepath.ToSlash(repoRoot)) + "\n"
	if err := os.WriteFile(filepath.Join(consumerDir, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	exampleDir := filepath.Join(repoRoot, "examples", "durable")
	entries, err := os.ReadDir(exampleDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		source, err := os.ReadFile(filepath.Join(exampleDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(consumerDir, entry.Name()), source, 0600); err != nil {
			t.Fatal(err)
		}
	}
	binary := filepath.Join(consumerDir, "durable")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	runCommand(t, consumerDir, "go", "build", "-mod=mod", "-o", binary, ".")
	dbPath := filepath.Join(consumerDir, "runs.db")
	if got := runCommand(t, consumerDir, binary, "start", "-db", dbPath, "-run", "demo"); got != "status=waiting run=demo\n" {
		t.Fatalf("start output = %q", got)
	}
	if got := runCommand(t, consumerDir, binary, "resume", "-db", dbPath, "-run", "demo", "-value", "3"); got != "status=completed run=demo value=3\n" {
		t.Fatalf("resume output = %q", got)
	}
}

func runCommand(t *testing.T, dir, program string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", program, args, err, output)
	}
	return string(output)
}
