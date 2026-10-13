package consumertest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestConsumerControlFlowConstructionBoundary(t *testing.T) {
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	module := "module example.com/controlflow\n\ngo 1.26.5\n\nrequire github.com/afterlune/luneGraph v0.0.0\nreplace github.com/afterlune/luneGraph => " + strconv.Quote(filepath.ToSlash(repo)) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, body, diagnostic string }{
		{"constructors", `var _ = []graph.Transition[int]{graph.To(1, "next"), graph.ToTargets(1, []string{"next"}), graph.Wait(1, "input", "next"), graph.FanOut(1, graph.Branch[int]{Target:"next", State:1, EntryTimeout:0}), graph.CompleteBranch(1), graph.EndBranch[int](), graph.EndExecution(1), graph.Return(1), {}}`, ""},
		{"forged action", `var _ = graph.Transition[int]{Action: graph.ActionEndExecution}`, "unknown field Action"},
		{"forged state", `var _ = graph.Transition[int]{State: 1}`, "unknown field State"},
		{"forged targets", `var _ = graph.Transition[int]{Targets: []string{"next"}}`, "unknown field Targets"},
		{"forged branches", `var _ = graph.Transition[int]{Branches: []graph.Branch[int]{}}`, "unknown field Branches"},
		{"forged continuation", `var _ = graph.Transition[int]{Continuation:"input"}`, "unknown field Continuation"},
		{"wait without target", `var _ = graph.Wait(1, "input")`, "not enough arguments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := "package controlflow\nimport graph \"github.com/afterlune/luneGraph\"\n" + tc.body + "\n"
			if err := os.WriteFile(filepath.Join(dir, "consumer.go"), []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, "go", "build", "-mod=mod", ".")
			cmd.Dir, cmd.Env = dir, append(os.Environ(), "GOWORK=off")
			out, err := cmd.CombinedOutput()
			if tc.diagnostic == "" {
				if err != nil {
					t.Fatalf("valid consumer: %v\n%s", err, out)
				}
			} else if err == nil || !strings.Contains(string(out), tc.diagnostic) {
				t.Fatalf("construction boundary: %v\n%s", err, out)
			}
		})
	}
}
