package consumertest

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func verifyObservedExample(t *testing.T, dir, binary, dbPath string) {
	t.Helper()
	for _, command := range []string{"start", "resume"} {
		args := []string{command, "-db", dbPath, "-run", "observed", "-observe"}
		want := "status=waiting run=observed\n"
		if command == "resume" {
			args = append(args, "-value", "3")
			want = "status=completed run=observed value=3\n"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Dir, cmd.Env = dir, append(os.Environ(), "GOWORK=off")
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		cancel()
		if err != nil || stdout.String() != want {
			t.Fatalf("observed %s: %v, stdout=%q stderr=%q", command, err, stdout.String(), stderr.String())
		}
		var nodes, publicFinish int
		for _, line := range strings.Split(strings.TrimSpace(stderr.String()), "\n") {
			var event map[string]any
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				t.Fatalf("invalid event %q: %v", line, err)
			}
			if event["run_id"] != "observed" || event["machine_id"] != "durable-example-v1" {
				t.Fatalf("event identity = %+v", event)
			}
			if event["operation"] == "node" && event["phase"] == "finished" {
				nodes++
			}
			operation := "start"
			if command == "resume" {
				operation = "recover"
			}
			if event["operation"] == operation && event["phase"] == "finished" {
				publicFinish++
			}
		}
		if nodes != 1 || publicFinish != 1 {
			t.Fatalf("missing execution events: %s", stderr.String())
		}
	}
}
