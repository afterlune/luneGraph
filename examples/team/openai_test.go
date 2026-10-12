package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func TestOpenAIResponsesAdapterBuildsStructuredRequestAndNormalizesUsage(t *testing.T) {
	var usage bytes.Buffer
	models := []ModelOption{
		{ModelID: "deepseek/fast", Provider: "deepseek", Model: "deepseek-flash", Description: "fast research"},
		{ModelID: "openai/reasoning", Provider: "openai", Model: "gpt-reasoning", Description: "careful planning"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != openAIResponsesPath {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer openai-test-key" {
			t.Errorf("Authorization = %q", got)
		}
		var request openAIResponsesRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		if request.Model != "test-reasoning" || request.MaxOutputTokens != 320 || request.Store {
			t.Errorf("request fields = %+v", request)
		}
		if request.Text.Format.Type != "json_schema" || request.Text.Format.Name != "team_task_response" || !request.Text.Format.Strict {
			t.Errorf("format = %+v", request.Text.Format)
		}
		if !strings.Contains(request.Instructions, "available_models") || !strings.Contains(request.Input, `"model_id":"deepseek/fast"`) || !strings.Contains(request.Input, "fast research") {
			t.Errorf("instructions/input omitted model choices: %+v", request)
		}
		if strings.Contains(request.Instructions+request.Input, "openai-test-key") {
			t.Error("API key appeared in prompt")
		}
		encodedSchema, err := json.Marshal(request.Text.Format.Schema)
		if err != nil {
			t.Errorf("encode schema: %v", err)
		} else if !bytes.Contains(encodedSchema, []byte(`"deepseek/fast"`)) || !bytes.Contains(encodedSchema, []byte(`"openai/reasoning"`)) {
			t.Errorf("schema omitted allow-list model IDs: %s", encodedSchema)
		}
		writeOpenAIResponse(w, "{\"text\":\"plan\",\"assignments\":[]}", "completed", "", true)
	}))
	defer server.Close()
	adapter := mustOpenAIAdapter(t, "openai-test-key", openAIConfig{
		baseURL: server.URL, modelID: "openai/reasoning", model: "test-reasoning",
		requestTimeout: time.Second, maxOutputTokens: 320, usageWriter: &usage,
	})
	result, err := adapter.Generate(context.Background(), graph.CallInfo{RunID: "mixed-1", CallID: "openai-call"}, ModelRequest{
		Role: rolePlanner, Goal: "make a plan", AvailableModels: models,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "plan" || len(result.Assignments) != 0 {
		t.Fatalf("result = %+v", result)
	}
	var record UsageRecord
	if err := json.Unmarshal(bytes.TrimSpace(usage.Bytes()), &record); err != nil {
		t.Fatalf("usage record %q: %v", usage.String(), err)
	}
	if record.Provider != "openai" || record.ModelID != "openai/reasoning" || record.Model != "test-reasoning" || record.Role != rolePlanner || record.RunID != "mixed-1" || record.CallID != "openai-call" || !record.UsageAvailable || record.PromptTokens != 23 || record.CompletionTokens != 11 || record.TotalTokens != 34 {
		t.Fatalf("usage record = %+v", record)
	}
	if strings.Contains(usage.String(), "openai-test-key") || strings.Contains(usage.String(), `"text":"plan"`) {
		t.Fatalf("usage output contains prompt/output or key: %s", usage.String())
	}
}

func TestOpenAIResponsesAdapterHandlesOutputItemsAndFailures(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		wantErr   string
		wantUsage bool
	}{
		{name: "http error", status: http.StatusTooManyRequests, body: `secret error`, wantErr: "HTTP status 429"},
		{name: "malformed response", status: http.StatusOK, body: `{`, wantErr: "decode OpenAI Responses response"},
		{name: "incomplete output limit", status: http.StatusOK, body: `{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":3,"output_tokens":4,"total_tokens":7}}`, wantErr: "output token limit", wantUsage: true},
		{name: "failed status", status: http.StatusOK, body: `{"status":"failed","error":{"code":"server_error"}}`, wantErr: "code \"server_error\"", wantUsage: true},
		{name: "refusal", status: http.StatusOK, body: `{"status":"completed","output":[{"type":"message","content":[{"type":"refusal","refusal":"private refusal text"}]}]}`, wantErr: "refused", wantUsage: true},
		{name: "no output text", status: http.StatusOK, body: `{"status":"completed","output":[{"type":"reasoning","content":[]}]}`, wantErr: "no output text", wantUsage: true},
		{name: "invalid team JSON", status: http.StatusOK, body: `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"not json"}]}]}`, wantErr: "invalid JSON", wantUsage: true},
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
			adapter := mustOpenAIAdapter(t, "key", openAIConfig{
				baseURL: server.URL, modelID: "openai/test", model: "test-model", usageWriter: &usage,
			})
			_, err := adapter.Generate(context.Background(), graph.CallInfo{}, ModelRequest{Role: roleSearch})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Generate error = %v, want containing %q", err, test.wantErr)
			}
			if calls != 1 {
				t.Fatalf("request count = %d, want one attempt", calls)
			}
			if got := usage.Len() > 0; got != test.wantUsage {
				t.Fatalf("usage logged = %t, want %t (%s)", got, test.wantUsage, usage.String())
			}
			if strings.Contains(err.Error(), "secret error") || strings.Contains(err.Error(), "private refusal text") {
				t.Fatalf("provider body leaked into error: %v", err)
			}
		})
	}
}

func TestOpenAIResponsesAdapterHonorsCancellationAndTimeout(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		time.Sleep(100 * time.Millisecond)
		writeOpenAIResponse(w, `{"text":"late"}`, "completed", "", false)
	}))
	defer server.Close()
	adapter := mustOpenAIAdapter(t, "key", openAIConfig{
		baseURL: server.URL, modelID: "openai/test", model: "test-model", requestTimeout: 30 * time.Millisecond,
	})
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

func TestOpenAIResponsesAdapterValidatesConfigurationAndSchemas(t *testing.T) {
	for _, config := range []openAIConfig{
		{modelID: "openai/test", model: "test", requestTimeout: -time.Second},
		{modelID: "openai/test", model: "test", maxOutputTokens: -1},
		{modelID: "openai/test", model: "test", baseURL: "not-a-url"},
		{modelID: "openai/test", model: " "},
		{modelID: " ", model: "test"},
	} {
		if _, err := newOpenAIResponsesAdapter("key", config); err == nil {
			t.Fatalf("accepted config %+v", config)
		}
	}
	if _, err := newOpenAIResponsesAdapter(" ", openAIConfig{modelID: "id", model: "model"}); err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Fatalf("missing API key error = %v", err)
	}
	models := []ModelOption{{ModelID: "worker/a"}, {ModelID: "worker/b"}}
	schema, err := teamResponseSchema(roleLead, models)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"assignments", "worker/a", "worker/b", "search", "planner"} {
		if !bytes.Contains(encoded, []byte(value)) {
			t.Fatalf("schema %s omitted %q", encoded, value)
		}
	}
	if _, err := teamResponseSchema(roleLead, nil); err == nil {
		t.Fatal("built assignment schema without an allow-list")
	}
	if _, err := teamResponseSchema("unknown", models); err == nil {
		t.Fatal("built schema for unknown role")
	}
}

func TestMixedProviderTeamRecoversFromSQLiteWithStableModelIDs(t *testing.T) {
	var openAICalls, deepSeekCalls int
	var countMu = make(chan struct{}, 1)
	countMu <- struct{}{}
	var usage bytes.Buffer
	serializedUsage := newSerializedWriter(&usage)
	openAIServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request openAIResponsesRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		input, err := openAIRequestInput(request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		<-countMu
		openAICalls++
		countMu <- struct{}{}
		var content string
		switch {
		case strings.Contains(request.Instructions, "You are the Lead"):
			content = `{"assignments":[{"target":"search","id":"search-openai","model_id":"deepseek/search","instruction":"research"},{"target":"planner","id":"plan-openai","model_id":"openai/worker","instruction":"plan"}]}`
		case strings.Contains(request.Instructions, "You are a Planner") && input.Depth == 0:
			content = `{"text":"","assignments":[{"target":"search","id":"plan-openai/research","model_id":"openai/worker","instruction":"gather evidence"},{"target":"planner","id":"plan-openai/subplan","model_id":"deepseek/search","instruction":"finish the plan"}]}`
		case strings.Contains(request.Instructions, "You are a Search"):
			content = `{"text":"openai research"}`
		default:
			http.Error(w, "unexpected OpenAI role", http.StatusBadRequest)
			return
		}
		writeOpenAIResponse(w, content, "completed", "", true)
	}))
	defer openAIServer.Close()
	deepSeekServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request deepSeekRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		input, err := deepSeekRequestInput(request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		<-countMu
		deepSeekCalls++
		countMu <- struct{}{}
		var content string
		switch input.Role {
		case roleSearch:
			content = `{"text":"deepseek research"}`
		case rolePlanner:
			content = `{"text":"deepseek final plan","assignments":[]}`
		default:
			http.Error(w, "unexpected DeepSeek role", http.StatusBadRequest)
			return
		}
		writeDeepSeekResponse(w, content, "stop", true)
	}))
	defer deepSeekServer.Close()

	openAILead, err := newOpenAIResponsesAdapter("openai-key", openAIConfig{
		baseURL: openAIServer.URL, modelID: "openai/lead", model: "test-openai", requestTimeout: time.Second,
		maxOutputTokens: 256, usageWriter: serializedUsage,
	})
	if err != nil {
		t.Fatal(err)
	}
	openAIWorker, err := newOpenAIResponsesAdapter("openai-key", openAIConfig{
		baseURL: openAIServer.URL, modelID: "openai/worker", model: "test-openai", requestTimeout: time.Second,
		maxOutputTokens: 256, usageWriter: serializedUsage,
	})
	if err != nil {
		t.Fatal(err)
	}
	deepSeekWorker, err := newDeepSeekAdapter("deepseek-key", deepSeekConfig{
		baseURL: deepSeekServer.URL, providerModel: "test-deepseek", requestTimeout: time.Second,
		maxTokens: 256, usageWriter: serializedUsage,
	})
	if err != nil {
		t.Fatal(err)
	}
	deepSeekWorker.modelID = "deepseek/search"
	workerModels := []ModelOption{
		{ModelID: "deepseek/search", Provider: "deepseek", Model: "test-deepseek", Description: "research"},
		{ModelID: "openai/worker", Provider: "openai", Model: "test-openai", Description: "planning"},
	}
	models := ModelRegistry{
		"openai/lead": openAILead, "openai/worker": openAIWorker, "deepseek/search": deepSeekWorker,
	}
	registryConfig, err := decodeTeamRegistryConfig(strings.NewReader(mixedRegistryJSON))
	if err != nil {
		t.Fatal(err)
	}
	machineID, err := teamRegistryMachineID(registryConfig)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := newTeamRunnerWithModelOptions(models, machineID, workerModels)
	if err != nil {
		t.Fatal(err)
	}
	db := t.TempDir() + "/mixed.db"
	store, err := sqlite.Open(context.Background(), db, checkpoint.JSON[teamState]{})
	if err != nil {
		t.Fatal(err)
	}
	execution, waiting, err := runner.Start(context.Background(), "mixed-provider-run", teamState{
		Goal: "research and plan", RequestedTasks: 2, ModelID: "openai/lead", MaxDepth: 1,
	}, store, graph.Options{MaxConcurrency: 4})
	if err != nil || waiting.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", waiting, err)
	}
	var openCallsBefore, deepCallsBefore int
	<-countMu
	openCallsBefore, deepCallsBefore = openAICalls, deepSeekCalls
	countMu <- struct{}{}
	if openCallsBefore != 3 || deepCallsBefore != 2 {
		t.Fatalf("calls before recovery: OpenAI=%d DeepSeek=%d", openCallsBefore, deepCallsBefore)
	}
	pending := execution.Pending()
	if len(pending) != 1 {
		t.Fatalf("pending = %+v", pending)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	recoveredStore, err := sqlite.Open(context.Background(), db, checkpoint.JSON[teamState]{})
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredStore.Close()
	differentConfigRunner, err := newTeamRunnerWithModelOptions(models, machineID+"-changed", workerModels)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := differentConfigRunner.Recover(context.Background(), "mixed-provider-run", nil, recoveredStore, graph.Options{}); err == nil {
		t.Fatal("recovered a registry checkpoint with a different MachineID")
	}
	recoveredRunner, err := newTeamRunnerWithModelOptions(models, machineID, workerModels)
	if err != nil {
		t.Fatal(err)
	}
	_, completed, err := recoveredRunner.Recover(context.Background(), "mixed-provider-run", []graph.ResumeInput{{
		InvocationID: pending[0].InvocationID, CallID: pending[0].CallID, Payload: []byte("true"),
	}}, recoveredStore, graph.Options{})
	if err != nil || completed.Status != graph.StatusCompleted || completed.Final == nil {
		t.Fatalf("recover = %+v, %v", completed, err)
	}
	if strings.Join(completed.Final.Results, "|") != "deepseek research|openai research|deepseek final plan" {
		t.Fatalf("mixed provider results = %v", completed.Final.Results)
	}
	<-countMu
	if openAICalls != openCallsBefore || deepSeekCalls != deepCallsBefore {
		countMu <- struct{}{}
		t.Fatal("approval recovery replayed already committed provider calls")
	}
	countMu <- struct{}{}
	usageLines := strings.Split(strings.TrimSpace(usage.String()), "\n")
	if len(usageLines) != 5 {
		t.Fatalf("usage records = %d, want one per provider call: %s", len(usageLines), usage.String())
	}
}

func mustOpenAIAdapter(t *testing.T, apiKey string, config openAIConfig) *openAIResponsesAdapter {
	t.Helper()
	adapter, err := newOpenAIResponsesAdapter(apiKey, config)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func writeOpenAIResponse(w http.ResponseWriter, text, status, incompleteReason string, withUsage bool) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]any{
		"status": status,
		"output": []any{
			map[string]any{"type": "reasoning", "status": "completed"},
			map[string]any{"type": "message", "status": "completed", "content": []any{
				map[string]string{"type": "output_text", "text": text},
			}},
		},
	}
	if incompleteReason != "" {
		response["incomplete_details"] = map[string]string{"reason": incompleteReason}
	}
	if withUsage {
		response["usage"] = map[string]int{"input_tokens": 23, "output_tokens": 11, "total_tokens": 34}
	}
	_ = json.NewEncoder(w).Encode(response)
}

func openAIRequestInput(request openAIResponsesRequest) (ModelRequest, error) {
	const prefix = "Task details as JSON:\n"
	if !strings.HasPrefix(request.Input, prefix) {
		return ModelRequest{}, errors.New("OpenAI input omitted task JSON")
	}
	var input ModelRequest
	err := json.Unmarshal([]byte(strings.TrimPrefix(request.Input, prefix)), &input)
	return input, err
}
