package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func teamOutputField(t *testing.T, output, name string) string {
	t.Helper()
	for _, token := range strings.Fields(output) {
		key, value, ok := strings.Cut(token, "=")
		if ok && key == name {
			return value
		}
	}
	t.Fatalf("missing %s in output %q", name, output)
	return ""
}

func TestOfflineTeamStartAndApprovalRecovery(t *testing.T) {
	db := filepath.Join(t.TempDir(), "team.db")
	var output, diagnostics bytes.Buffer
	if err := run(context.Background(), []string{"start", "-db", db, "-run", "team-approval", "-tasks", "2"}, &output, &diagnostics); err != nil {
		t.Fatalf("start: %v (%s)", err, diagnostics.String())
	}
	if !strings.Contains(output.String(), "status=waiting") {
		t.Fatalf("start output: %s", output.String())
	}
	invocation, call := teamOutputField(t, output.String(), "invocation"), teamOutputField(t, output.String(), "call")
	output.Reset()
	diagnostics.Reset()
	stale := []string{"resume", "-db", db, "-run", "team-approval", "-invocation", invocation, "-call", "stale-call"}
	if err := run(context.Background(), stale, &output, &diagnostics); err == nil {
		t.Fatal("resume rebound an input with a stale CallID")
	}
	output.Reset()
	diagnostics.Reset()
	args := []string{"resume", "-db", db, "-run", "team-approval", "-invocation", invocation, "-call", call, "-approved=true"}
	if err := run(context.Background(), args, &output, &diagnostics); err != nil {
		t.Fatalf("resume: %v (%s)", err, diagnostics.String())
	}
	want := "status=completed run=team-approval approved=true results=task-0 via local/search-model,task-1/research via local/search-model,task-1/plan via local/planning-model"
	if !strings.Contains(output.String(), want) {
		t.Fatalf("resume output = %q, want to contain %q", output.String(), want)
	}
}

func TestOfflineTeamEmptyPlanAndRejectedApproval(t *testing.T) {
	db := filepath.Join(t.TempDir(), "empty.db")
	var output, diagnostics bytes.Buffer
	if err := run(context.Background(), []string{"start", "-db", db, "-run", "team-empty", "-tasks", "0"}, &output, &diagnostics); err != nil {
		t.Fatal(err)
	}
	invocation, call := teamOutputField(t, output.String(), "invocation"), teamOutputField(t, output.String(), "call")
	output.Reset()
	args := []string{"resume", "-db", db, "-run", "team-empty", "-invocation", invocation, "-call", call, "-approved=false"}
	if err := run(context.Background(), args, &output, &diagnostics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "status=completed run=team-empty approved=false results=") {
		t.Fatal(output.String())
	}
}

func TestOfflineTeamRejectsInvalidPlanSizeAndMissingWaitIdentity(t *testing.T) {
	var output, diagnostics bytes.Buffer
	if err := run(context.Background(), []string{"start", "-tasks", "17"}, &output, &diagnostics); err == nil {
		t.Fatal("oversized Team plan accepted")
	}
	db := filepath.Join(t.TempDir(), "missing-identity.db")
	if err := run(context.Background(), []string{"start", "-db", db, "-run", "team-missing"}, &output, &diagnostics); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"resume", "-db", db, "-run", "team-missing"}, &output, &diagnostics); err == nil {
		t.Fatal("resume accepted a missing waiting identity")
	}
}

func TestTeamRunRejectsInvalidArgumentsAndDatabasePaths(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing command"},
		{name: "unknown command", args: []string{"inspect"}},
		{name: "invalid flag value", args: []string{"start", "-tasks", "many"}},
		{name: "unexpected positional argument", args: []string{"start", "unexpected"}},
		{name: "unavailable database directory", args: []string{"start", "-db", filepath.Join(t.TempDir(), "missing", "runs.db")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output, diagnostics bytes.Buffer
			if err := run(context.Background(), test.args, &output, &diagnostics); err == nil {
				t.Fatal("invalid CLI input was accepted")
			}
		})
	}
}

func TestTeamMainStartsDurableRun(t *testing.T) {
	previousArgs, previousStdout := os.Args, os.Stdout
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Args = []string{"team", "start", "-db", filepath.Join(t.TempDir(), "runs.db"), "-run", "main-process", "-tasks", "0"}
	os.Stdout = write
	main()
	if err := write.Close(); err != nil {
		t.Fatal(err)
	}
	os.Args, os.Stdout = previousArgs, previousStdout
	data, err := io.ReadAll(read)
	_ = read.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "status=waiting run=main-process") {
		t.Fatalf("main output = %s", data)
	}
}

func TestOfflineTeamRejectsIncompatibleModelAssignments(t *testing.T) {
	for _, assignment := range []task{
		{Target: "search", ID: "wrong-search", Model: modelPlan},
		{Target: "planner", ID: "wrong-planner", Model: modelSearch},
	} {
		t.Run(assignment.Target, func(t *testing.T) {
			plan := teamState{Plan: []task{assignment}, MaxDepth: 1}
			runner, err := newTeamRunner()
			if err != nil {
				t.Fatal(err)
			}
			execution, waiting, err := runner.Start(context.Background(), "bad-model-"+assignment.Target, plan, nil, graph.Options{MaxConcurrency: 1})
			if err != nil || waiting.Status != graph.StatusWaiting || !waiting.Header.HadLocalFailures {
				t.Fatalf("invalid model assignment = %+v, %v", waiting, err)
			}
			pending := execution.Pending()[0]
			completed, err := execution.Advance(context.Background(), []graph.ResumeInput{{InvocationID: pending.InvocationID, CallID: pending.CallID, Payload: []byte("true")}}, graph.Options{})
			if err != nil || completed.Status != graph.StatusCompletedWithFailures || completed.Final == nil || len(completed.Final.Results) != 0 {
				t.Fatalf("invalid model result = %+v, %v", completed, err)
			}
		})
	}
}

func TestAddTeamDefinitionRejectsDuplicateRegistrations(t *testing.T) {
	for _, name := range []string{"lead", "search", "planner", "worker-return", "approval", "finish"} {
		t.Run("node/"+name, func(t *testing.T) {
			g := graph.New[teamState]("lead")
			if err := g.AddNode(graph.NodeSpec[teamState]{Name: name, Run: func(context.Context, graph.CallInfo, teamState) (graph.Transition[teamState], error) {
				return graph.EndExecution(teamState{}), nil
			}}); err != nil {
				t.Fatal(err)
			}
			if err := addTeamDefinition(g); err == nil {
				t.Fatal("duplicate node accepted")
			}
		})
	}
	for _, name := range []string{"worker-join", "lead-join"} {
		t.Run("join/"+name, func(t *testing.T) {
			g := graph.New[teamState]("lead")
			if err := g.AddJoin(graph.JoinSpec[teamState]{Name: name, From: "lead", Merge: mergeTeamResults}); err != nil {
				t.Fatal(err)
			}
			if err := addTeamDefinition(g); err == nil {
				t.Fatal("duplicate join accepted")
			}
		})
	}
	t.Run("continuation", func(t *testing.T) {
		g := graph.New[teamState]("lead")
		if err := graph.RegisterJSONContinuation(g, "approval", func(context.Context, graph.CallInfo, teamState, bool) (teamState, error) { return teamState{}, nil }); err != nil {
			t.Fatal(err)
		}
		if err := addTeamDefinition(g); err == nil {
			t.Fatal("duplicate continuation accepted")
		}
	})
}
