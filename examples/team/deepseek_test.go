package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
)

func TestDeepSeekAdapterUsesChatCompletionsJSONMode(t *testing.T) {
	var usage bytes.Buffer
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != deepSeekChatCompletionsURL {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-secret" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		var request deepSeekRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if request.Model != deepSeekProviderModel || request.MaxTokens != 256 || request.ResponseFormat.Type != "json_object" {
			t.Errorf("request options = %+v", request)
		}
		if len(request.Messages) != 2 || request.Messages[0].Role != "system" || request.Messages[1].Role != "user" {
			t.Errorf("messages = %+v", request.Messages)
			return
		}
		prompt := request.Messages[0].Content + request.Messages[1].Content
		if !strings.Contains(strings.ToLower(prompt), "json") || !strings.Contains(prompt, `"assignments"`) || !strings.Contains(prompt, modelDeepSeekFlash) {
			t.Errorf("prompt omitted JSON contract/example: %q", prompt)
		}
		if strings.Contains(prompt, "test-secret") {
			t.Error("API key appeared in the prompt")
		}
		writeDeepSeekResponse(w, `{"assignments":[{"target":"search","id":"research","model_id":"deepseek/deepseek-flash","instruction":"find evidence"}]}`, "stop", true)
	}))
	defer server.Close()
	adapter := mustDeepSeekAdapter(t, "test-secret", deepSeekConfig{
		baseURL: server.URL, requestTimeout: time.Second, maxTokens: 256, usageWriter: &usage,
	})
	result, err := adapter.Generate(context.Background(), graph.CallInfo{RunID: "run-1", CallID: "call-2"}, ModelRequest{
		Role: roleLead, Goal: "answer a question", RequestedTasks: 1,
		AvailableModels: []ModelOption{{ModelID: modelDeepSeekFlash, Provider: "deepseek", Model: deepSeekProviderModel}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Assignments) != 1 || result.Assignments[0].ModelID != modelDeepSeekFlash || result.Assignments[0].Instruction != "find evidence" {
		t.Fatalf("result = %+v", result)
	}
	var record UsageRecord
	if err := json.Unmarshal(bytes.TrimSpace(usage.Bytes()), &record); err != nil {
		t.Fatalf("usage record %q: %v", usage.String(), err)
	}
	if record.Provider != "deepseek" || record.Model != deepSeekProviderModel || record.Role != roleLead || record.RunID != "run-1" || record.CallID != "call-2" || !record.UsageAvailable || record.PromptTokens != 17 || record.CompletionTokens != 9 || record.TotalTokens != 26 {
		t.Fatalf("usage record = %+v", record)
	}
	if strings.Contains(usage.String(), "test-secret") || strings.Contains(usage.String(), "find evidence") {
		t.Fatalf("usage output contains sensitive request/response content: %s", usage.String())
	}
}

func TestDeepSeekParsesRoleSpecificResponses(t *testing.T) {
	tests := []struct {
		name    string
		role    string
		content string
		wantErr string
	}{
		{name: "search text", role: roleSearch, content: `{"text":"evidence"}`},
		{name: "lead assignments", role: roleLead, content: `{"assignments":[]}`},
		{name: "planner text", role: rolePlanner, content: `{"text":"plan"}`},
		{name: "planner delegates", role: rolePlanner, content: `{"assignments":[{"target":"planner","id":"child","model_id":"deepseek/deepseek-flash","instruction":"continue"}]}`},
		{name: "unknown field", role: roleSearch, content: `{"text":"x","extra":1}`, wantErr: "unknown field"},
		{name: "empty search", role: roleSearch, content: `{"text":"  "}`, wantErr: "non-empty text"},
		{name: "search cannot delegate", role: roleSearch, content: `{"assignments":[{}]}`, wantErr: "must not return assignments"},
		{name: "lead needs assignments", role: roleLead, content: `{}`, wantErr: "missing assignments"},
		{name: "planner cannot return both", role: rolePlanner, content: `{"text":"plan","assignments":[{}]}`, wantErr: "either text or assignments"},
		{name: "planner needs output", role: rolePlanner, content: `{}`, wantErr: "needs text or assignments"},
		{name: "must be JSON", role: roleSearch, content: "not JSON", wantErr: "invalid JSON"},
		{name: "one value only", role: roleSearch, content: `{"text":"x"} {}`, wantErr: "multiple JSON values"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseDeepSeekContent(test.role, test.content)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("parse error = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.role == roleSearch && result.Text != "evidence" {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestDeepSeekAdapterRejectsProviderFailuresWithoutRetry(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		wantErr   string
		wantUsage bool
	}{
		{name: "rate limited", status: http.StatusTooManyRequests, body: `secret provider error`, wantErr: "HTTP status 429"},
		{name: "invalid envelope", status: http.StatusOK, body: `{`, wantErr: "decode DeepSeek response"},
		{name: "no choices", status: http.StatusOK, body: `{"usage":{"prompt_tokens":4}}`, wantErr: "no choices", wantUsage: true},
		{name: "empty content", status: http.StatusOK, body: `{"choices":[{"message":{"content":" "},"finish_reason":"stop"}],"usage":{"prompt_tokens":1}}`, wantErr: "content is empty", wantUsage: true},
		{name: "truncated", status: http.StatusOK, body: `{"choices":[{"message":{"content":"{}"},"finish_reason":"length"}],"usage":{"prompt_tokens":5,"completion_tokens":6,"total_tokens":11}}`, wantErr: "output token limit", wantUsage: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			var usage bytes.Buffer
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			adapter := mustDeepSeekAdapter(t, "key", deepSeekConfig{baseURL: server.URL, usageWriter: &usage})
			_, err := adapter.Generate(context.Background(), graph.CallInfo{}, ModelRequest{Role: roleSearch})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Generate error = %v, want containing %q", err, test.wantErr)
			}
			if calls != 1 {
				t.Fatalf("request count = %d, expected one attempt", calls)
			}
			if got := usage.Len() > 0; got != test.wantUsage {
				t.Fatalf("usage logged = %t, want %t (%s)", got, test.wantUsage, usage.String())
			}
			if test.status != http.StatusOK && strings.Contains(err.Error(), "secret provider error") {
				t.Fatalf("provider body leaked into error: %v", err)
			}
		})
	}
}

func TestDeepSeekAdapterHonorsCancellationAndTimeout(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		time.Sleep(100 * time.Millisecond)
		writeDeepSeekResponse(w, `{"text":"late"}`, "stop", false)
	}))
	defer server.Close()
	adapter := mustDeepSeekAdapter(t, "key", deepSeekConfig{baseURL: server.URL, requestTimeout: 30 * time.Millisecond})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := adapter.Generate(canceled, graph.CallInfo{}, ModelRequest{Role: roleSearch}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("pre-canceled request reached server %d times", calls)
	}
	if _, err := adapter.Generate(context.Background(), graph.CallInfo{}, ModelRequest{Role: roleSearch}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed request error = %v", err)
	}
}

func TestDeepSeekAdapterValidatesConfigurationAndRole(t *testing.T) {
	for _, config := range []deepSeekConfig{
		{requestTimeout: -time.Second},
		{maxTokens: -1},
		{baseURL: "   "},
	} {
		if _, err := newDeepSeekAdapter("key", config); err == nil {
			t.Fatalf("accepted config %+v", config)
		}
	}
	if _, err := newDeepSeekAdapter("  ", deepSeekConfig{}); err == nil || !strings.Contains(err.Error(), "DEEPSEEK_API_KEY") {
		t.Fatalf("empty API key error = %v", err)
	}
	if _, err := parseDeepSeekContent("unknown", `{}`); err == nil {
		t.Fatal("accepted unknown role")
	}
	if _, err := deepSeekMessages(ModelRequest{Role: "unknown"}); err == nil {
		t.Fatal("built prompt for unknown role")
	}
}

func TestDeepSeekTeamGraphRunsLeadAndRecursiveWorkers(t *testing.T) {
	var mu sync.Mutex
	requests := make(map[string]int)
	var usage bytes.Buffer
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request deepSeekRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		role := ""
		if len(request.Messages) > 0 {
			system := request.Messages[0].Content
			switch {
			case strings.Contains(system, "You are the Lead"):
				role = roleLead
			case strings.Contains(system, "You are a Search"):
				role = roleSearch
			case strings.Contains(system, "You are a Planner"):
				role = rolePlanner
			}
		}
		input, err := deepSeekRequestInput(request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		requests[role]++
		mu.Unlock()
		var content string
		switch role {
		case roleLead:
			content = `{"assignments":[{"target":"search","id":"research-1","model_id":"deepseek/deepseek-flash","instruction":"research"},{"target":"planner","id":"plan-1","model_id":"deepseek/deepseek-flash","instruction":"plan"}]}`
		case roleSearch:
			content = fmt.Sprintf(`{"text":"research:%s"}`, input.TaskID)
		case rolePlanner:
			if input.Depth == 0 {
				content = `{"assignments":[{"target":"search","id":"plan-1/research","model_id":"deepseek/deepseek-flash","instruction":"research plan"},{"target":"planner","id":"plan-1/child-plan","model_id":"deepseek/deepseek-flash","instruction":"finish plan"}]}`
			} else {
				content = fmt.Sprintf(`{"text":"plan:%s"}`, input.TaskID)
			}
		default:
			http.Error(w, "unknown role", http.StatusBadRequest)
			return
		}
		writeDeepSeekResponse(w, content, "stop", true)
	}))
	defer server.Close()
	models, err := deepSeekModelRegistry("test-key", deepSeekConfig{baseURL: server.URL, maxTokens: 192, usageWriter: &usage})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := newTeamRunnerWithID(models, "team-deepseek-test-v1")
	if err != nil {
		t.Fatal(err)
	}
	execution, waiting, err := runner.Start(context.Background(), "deepseek-run", teamState{
		Goal: "research and plan", RequestedTasks: 2, ModelID: modelDeepSeekFlash, MaxDepth: 1,
	}, nil, graph.Options{MaxConcurrency: 4})
	if err != nil || waiting.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", waiting, err)
	}
	pending := execution.Pending()
	if len(pending) != 1 || pending[0].Continuation != "approval" {
		t.Fatalf("pending = %+v", pending)
	}
	completed, err := execution.Advance(context.Background(), []graph.ResumeInput{{
		InvocationID: pending[0].InvocationID, CallID: pending[0].CallID, Payload: []byte("true"),
	}}, graph.Options{})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Final == nil {
		t.Fatalf("advance = %+v, %v", completed, err)
	}
	wantResults := []string{"research:research-1", "research:plan-1/research", "plan:plan-1/child-plan"}
	if strings.Join(completed.Final.Results, "|") != strings.Join(wantResults, "|") {
		t.Fatalf("results = %v, want %v", completed.Final.Results, wantResults)
	}
	mu.Lock()
	gotRequests := map[string]int{roleLead: requests[roleLead], roleSearch: requests[roleSearch], rolePlanner: requests[rolePlanner]}
	mu.Unlock()
	if gotRequests[roleLead] != 1 || gotRequests[roleSearch] != 2 || gotRequests[rolePlanner] != 2 {
		t.Fatalf("requests by role = %+v", gotRequests)
	}
	lines := strings.Split(strings.TrimSpace(usage.String()), "\n")
	if len(lines) != 5 {
		t.Fatalf("usage lines = %d, want one per model call: %s", len(lines), usage.String())
	}
	seenCalls := make(map[string]bool)
	for _, line := range lines {
		var record UsageRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("usage line %q: %v", line, err)
		}
		if record.RunID != "deepseek-run" || record.CallID == "" || !record.UsageAvailable || record.TotalTokens != 26 {
			t.Fatalf("usage record = %+v", record)
		}
		seenCalls[record.CallID] = true
	}
	if len(seenCalls) != 5 {
		t.Fatalf("usage did not identify 5 calls: %+v", seenCalls)
	}
}

func TestTeamCLIProviderValidationAndDeepSeekKeyPrecedesDatabase(t *testing.T) {
	db := t.TempDir() + "/not-created.db"
	t.Setenv("DEEPSEEK_API_KEY", "")
	var output, diagnostics bytes.Buffer
	err := run(context.Background(), []string{"start", "-provider", "deepseek", "-db", db}, &output, &diagnostics)
	if err == nil || !strings.Contains(err.Error(), "DEEPSEEK_API_KEY") {
		t.Fatalf("missing key error = %v", err)
	}
	if _, statErr := os.Stat(db); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("database state after missing key = %v, want not created", statErr)
	}
	if err := run(context.Background(), []string{"start", "-provider", "other"}, &output, &diagnostics); err == nil || !strings.Contains(err.Error(), "unknown provider") {
		t.Fatalf("unknown provider error = %v", err)
	}
	if err := run(context.Background(), []string{"start", "-provider", "deepseek", "-request-timeout", "0s"}, &output, &diagnostics); err == nil || !strings.Contains(err.Error(), "request-timeout") {
		t.Fatalf("invalid timeout error = %v", err)
	}
	if err := run(context.Background(), []string{"start", "-provider", "deepseek", "-max-tokens", "0"}, &output, &diagnostics); err == nil || !strings.Contains(err.Error(), "max-tokens") {
		t.Fatalf("invalid token limit error = %v", err)
	}
}

func mustDeepSeekAdapter(t *testing.T, apiKey string, config deepSeekConfig) *deepSeekAdapter {
	t.Helper()
	adapter, err := newDeepSeekAdapter(apiKey, config)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func writeDeepSeekResponse(w http.ResponseWriter, content, finishReason string, withUsage bool) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]any{
		"choices": []any{map[string]any{
			"message": map[string]string{"content": content}, "finish_reason": finishReason,
		}},
	}
	if withUsage {
		response["usage"] = map[string]int{"prompt_tokens": 17, "completion_tokens": 9, "total_tokens": 26}
	}
	_ = json.NewEncoder(w).Encode(response)
}

func deepSeekRequestInput(request deepSeekRequest) (ModelRequest, error) {
	if len(request.Messages) < 2 {
		return ModelRequest{}, errors.New("request is missing its user message")
	}
	const prefix = "Task details as JSON:\n"
	content := request.Messages[1].Content
	if !strings.HasPrefix(content, prefix) {
		return ModelRequest{}, errors.New("user message omitted task JSON")
	}
	content = strings.TrimPrefix(content, prefix)
	content, _, _ = strings.Cut(content, "\nReturn exactly one JSON object")
	var input ModelRequest
	err := json.Unmarshal([]byte(content), &input)
	return input, err
}
