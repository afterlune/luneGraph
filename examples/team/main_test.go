package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

func TestTeamUsesInjectedModelsAndStableCallInfo(t *testing.T) {
	type modelCall struct {
		call graph.CallInfo
		req  ModelRequest
	}
	var mu sync.Mutex
	calls := make(map[string][]modelCall)
	record := func(modelID string, call graph.CallInfo, request ModelRequest) {
		mu.Lock()
		calls[modelID] = append(calls[modelID], modelCall{call: call, req: request})
		mu.Unlock()
	}
	models := ModelRegistry{
		"test/lead": modelAdapterFunc(func(_ context.Context, call graph.CallInfo, request ModelRequest) (ModelResponse, error) {
			record("test/lead", call, request)
			return ModelResponse{Assignments: []TaskAssignment{
				{Target: roleSearch, ID: "research", ModelID: "test/search", Instruction: "find evidence"},
				{Target: rolePlanner, ID: "plan", ModelID: "test/planner", Instruction: "summarize"},
			}}, nil
		}),
		"test/search": modelAdapterFunc(func(_ context.Context, call graph.CallInfo, request ModelRequest) (ModelResponse, error) {
			record("test/search", call, request)
			return ModelResponse{Text: "injected research"}, nil
		}),
		"test/planner": modelAdapterFunc(func(_ context.Context, call graph.CallInfo, request ModelRequest) (ModelResponse, error) {
			record("test/planner", call, request)
			return ModelResponse{Text: "injected plan"}, nil
		}),
	}
	runner, err := newTeamRunner(models)
	if err != nil {
		t.Fatal(err)
	}
	// Runner construction snapshots registry selection; later caller map edits
	// cannot redirect a persisted model ID to a different adapter.
	models["test/search"] = modelAdapterFunc(func(context.Context, graph.CallInfo, ModelRequest) (ModelResponse, error) {
		return ModelResponse{Text: "wrong adapter"}, nil
	})
	execution, waiting, err := runner.Start(context.Background(), "injected-models", teamState{
		Goal: "answer the question", RequestedTasks: 2, ModelID: "test/lead", MaxDepth: 0,
	}, nil, graph.Options{MaxConcurrency: 4})
	if err != nil || waiting.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", waiting, err)
	}
	pending := execution.Pending()
	if len(pending) != 1 || pending[0].Continuation != "approval" || pending[0].CallID == "" {
		t.Fatalf("pending approval = %+v", pending)
	}
	completed, err := execution.Advance(context.Background(), []graph.ResumeInput{{
		InvocationID: pending[0].InvocationID,
		CallID:       pending[0].CallID,
		Payload:      []byte("true"),
	}}, graph.Options{})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Final == nil {
		t.Fatalf("advance = %+v, %v", completed, err)
	}
	if got, want := completed.Final.Results, []string{"injected research", "injected plan"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("results = %v, want %v", got, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls["test/lead"]) != 1 || len(calls["test/search"]) != 1 || len(calls["test/planner"]) != 1 {
		t.Fatalf("adapter calls = %+v", calls)
	}
	for modelID, invocations := range calls {
		for _, observed := range invocations {
			if observed.call.RunID != "injected-models" || observed.call.CallID == "" || observed.call.InvocationID == "" {
				t.Fatalf("%s received incomplete CallInfo: %+v", modelID, observed.call)
			}
		}
	}
	if calls["test/lead"][0].req.Role != roleLead || calls["test/search"][0].req.Role != roleSearch || calls["test/planner"][0].req.Role != rolePlanner {
		t.Fatalf("adapter roles = %+v", calls)
	}
}

func TestTeamValidatesAssignmentsAndRegistry(t *testing.T) {
	models := offlineModelRegistry()
	for _, test := range []struct {
		name        string
		assignments []TaskAssignment
	}{
		{name: "unknown target", assignments: []TaskAssignment{{Target: "unknown", ID: "task", ModelID: modelSearch}}},
		{name: "unknown model", assignments: []TaskAssignment{{Target: roleSearch, ID: "task", ModelID: "missing"}}},
		{name: "empty ID", assignments: []TaskAssignment{{Target: roleSearch, ModelID: modelSearch}}},
		{name: "duplicate IDs", assignments: []TaskAssignment{{Target: roleSearch, ID: "same", ModelID: modelSearch}, {Target: rolePlanner, ID: "same", ModelID: modelPlan}}},
		{name: "too many", assignments: makeAssignments(maxTasks + 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateAssignments(test.assignments, models); err == nil {
				t.Fatal("invalid assignments were accepted")
			}
		})
	}
	if _, err := copyModelRegistry(nil); err == nil {
		t.Fatal("empty model registry was accepted")
	}
	if _, err := copyModelRegistry(ModelRegistry{"broken": nil}); err == nil {
		t.Fatal("nil model adapter was accepted")
	}
}

func TestTeamRejectsNegativeDepthBudget(t *testing.T) {
	runner, err := newTeamRunner(offlineModelRegistry())
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := runner.Start(context.Background(), "negative-depth", teamState{
		Goal: "goal", ModelID: modelLead, MaxDepth: -1,
	}, nil, graph.Options{})
	if err != nil || result.Status != graph.StatusCompletedWithFailures || !result.Header.HadLocalFailures {
		t.Fatalf("negative depth budget = %+v, %v", result, err)
	}
}

func makeAssignments(count int) []TaskAssignment {
	assignments := make([]TaskAssignment, count)
	for i := range assignments {
		assignments[i] = TaskAssignment{Target: roleSearch, ID: "task-" + strconv.Itoa(i), ModelID: modelSearch}
	}
	return assignments
}

func TestTeamAdapterFailureIsAggregatedAsBranchFailure(t *testing.T) {
	models := offlineModelRegistry()
	models[modelLead] = modelAdapterFunc(func(context.Context, graph.CallInfo, ModelRequest) (ModelResponse, error) {
		return ModelResponse{Assignments: []TaskAssignment{
			{Target: roleSearch, ID: "search-fails", ModelID: "test/failing"},
			{Target: rolePlanner, ID: "plan-succeeds", ModelID: modelPlan},
		}}, nil
	})
	models["test/failing"] = modelAdapterFunc(func(context.Context, graph.CallInfo, ModelRequest) (ModelResponse, error) {
		return ModelResponse{}, errors.New("provider unavailable")
	})
	runner, err := newTeamRunner(models)
	if err != nil {
		t.Fatal(err)
	}
	execution, waiting, err := runner.Start(context.Background(), "adapter-failure", teamState{Goal: "goal", ModelID: modelLead, MaxDepth: 0}, nil, graph.Options{MaxConcurrency: 2})
	if err != nil || waiting.Status != graph.StatusWaiting || !waiting.Header.HadLocalFailures {
		t.Fatalf("start with failed adapter = %+v, %v", waiting, err)
	}
	pending := execution.Pending()[0]
	completed, err := execution.Advance(context.Background(), []graph.ResumeInput{{InvocationID: pending.InvocationID, CallID: pending.CallID, Payload: []byte("true")}}, graph.Options{})
	if err != nil || completed.Status != graph.StatusCompletedWithFailures || completed.Final == nil {
		t.Fatalf("completed adapter failure run = %+v, %v", completed, err)
	}
	if strings.Join(completed.Final.Results, ",") != "plan-succeeds via local/planning-model" || len(completed.Final.Failures) != 1 || !strings.Contains(completed.Final.Failures[0], "provider unavailable") {
		t.Fatalf("partial aggregation = %+v", completed.Final)
	}
}

func TestPlannerCannotDelegateBeyondConfiguredDepth(t *testing.T) {
	models := offlineModelRegistry()
	models[modelLead] = modelAdapterFunc(func(context.Context, graph.CallInfo, ModelRequest) (ModelResponse, error) {
		return ModelResponse{Assignments: []TaskAssignment{{Target: rolePlanner, ID: "plan", ModelID: modelPlan}}}, nil
	})
	models[modelPlan] = modelAdapterFunc(func(context.Context, graph.CallInfo, ModelRequest) (ModelResponse, error) {
		return ModelResponse{Assignments: []TaskAssignment{{Target: roleSearch, ID: "too-deep", ModelID: modelSearch}}}, nil
	})
	runner, err := newTeamRunner(models)
	if err != nil {
		t.Fatal(err)
	}
	execution, waiting, err := runner.Start(context.Background(), "max-depth", teamState{Goal: "goal", ModelID: modelLead, MaxDepth: 0}, nil, graph.Options{MaxConcurrency: 1})
	if err != nil || waiting.Status != graph.StatusWaiting || !waiting.Header.HadLocalFailures {
		t.Fatalf("start beyond max depth = %+v, %v", waiting, err)
	}
	pending := execution.Pending()[0]
	completed, err := execution.Advance(context.Background(), []graph.ResumeInput{{InvocationID: pending.InvocationID, CallID: pending.CallID, Payload: []byte("true")}}, graph.Options{})
	if err != nil || completed.Status != graph.StatusCompletedWithFailures || completed.Final == nil || len(completed.Final.Results) != 0 || len(completed.Final.Failures) != 1 || !strings.Contains(completed.Final.Failures[0], "maximum depth") {
		t.Fatalf("max-depth result = %+v, %v", completed, err)
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
			if err := addTeamDefinition(g, offlineModelRegistry()); err == nil {
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
			if err := addTeamDefinition(g, offlineModelRegistry()); err == nil {
				t.Fatal("duplicate join accepted")
			}
		})
	}
	t.Run("continuation", func(t *testing.T) {
		g := graph.New[teamState]("lead")
		if err := graph.RegisterJSONContinuation(g, "approval", func(context.Context, graph.CallInfo, teamState, bool) (teamState, error) { return teamState{}, nil }); err != nil {
			t.Fatal(err)
		}
		if err := addTeamDefinition(g, offlineModelRegistry()); err == nil {
			t.Fatal("duplicate continuation accepted")
		}
	})
}
