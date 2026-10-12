package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	graph "github.com/afterlune/luneGraph"
)

const (
	deepSeekDefaultBaseURL     = "https://api.deepseek.com"
	deepSeekDefaultTimeout     = 120 * time.Second
	deepSeekDefaultMaxTokens   = 4096
	deepSeekMaxResponseBytes   = 4 << 20
	deepSeekProviderModel      = "deepseek-flash"
	deepSeekChatCompletionsURL = "/chat/completions"
)

type deepSeekConfig struct {
	baseURL        string
	requestTimeout time.Duration
	maxTokens      int
	client         *http.Client
	usageWriter    io.Writer
}

type deepSeekUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// UsageRecord is diagnostic-only call metadata. It deliberately excludes
// prompts, model output, and credentials so it can be written to stderr.
type UsageRecord struct {
	Provider         string `json:"provider"`
	Model            string `json:"model"`
	Role             string `json:"role"`
	RunID            string `json:"run_id"`
	CallID           string `json:"call_id"`
	UsageAvailable   bool   `json:"usage_available"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	TotalTokens      int    `json:"total_tokens"`
}

// deepSeekAdapter uses the non-streaming Chat Completions API. Its instances
// are injected into the runner and never enter execution state or checkpoints.
type deepSeekAdapter struct {
	apiKey         string
	endpoint       string
	requestTimeout time.Duration
	maxTokens      int
	client         *http.Client
	usageWriter    io.Writer
	usageMu        sync.Mutex
}

func newDeepSeekAdapter(apiKey string, config deepSeekConfig) (*deepSeekAdapter, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, errors.New("DEEPSEEK_API_KEY is required when -provider=deepseek")
	}
	if config.baseURL == "" {
		config.baseURL = deepSeekDefaultBaseURL
	}
	if config.requestTimeout == 0 {
		config.requestTimeout = deepSeekDefaultTimeout
	}
	if config.maxTokens == 0 {
		config.maxTokens = deepSeekDefaultMaxTokens
	}
	if config.requestTimeout < 0 {
		return nil, errors.New("request timeout must be positive")
	}
	if config.maxTokens < 1 {
		return nil, errors.New("max output tokens must be positive")
	}
	config.baseURL = strings.TrimSpace(config.baseURL)
	parsedURL, err := url.Parse(config.baseURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" || parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		return nil, errors.New("DeepSeek base URL must be an HTTP(S) origin or base path")
	}
	client := config.client
	if client == nil {
		client = http.DefaultClient
	}
	return &deepSeekAdapter{
		apiKey:         apiKey,
		endpoint:       strings.TrimRight(config.baseURL, "/") + deepSeekChatCompletionsURL,
		requestTimeout: config.requestTimeout,
		maxTokens:      config.maxTokens,
		client:         client,
		usageWriter:    config.usageWriter,
	}, nil
}

func deepSeekModelRegistry(apiKey string, config deepSeekConfig) (ModelRegistry, error) {
	adapter, err := newDeepSeekAdapter(apiKey, config)
	if err != nil {
		return nil, err
	}
	return ModelRegistry{modelDeepSeekFlash: adapter}, nil
}

type deepSeekMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type deepSeekRequest struct {
	Model          string            `json:"model"`
	Messages       []deepSeekMessage `json:"messages"`
	MaxTokens      int               `json:"max_tokens"`
	ResponseFormat struct {
		Type string `json:"type"`
	} `json:"response_format"`
}

type deepSeekResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *deepSeekUsage `json:"usage"`
}

func (a *deepSeekAdapter) Generate(ctx context.Context, call graph.CallInfo, request ModelRequest) (ModelResponse, error) {
	messages, err := deepSeekMessages(request)
	if err != nil {
		return ModelResponse{}, err
	}
	requestBody := deepSeekRequest{
		Model:     deepSeekProviderModel,
		Messages:  messages,
		MaxTokens: a.maxTokens,
	}
	requestBody.ResponseFormat.Type = "json_object"
	encoded, err := json.Marshal(requestBody)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("encode DeepSeek request: %w", err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, a.requestTimeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodPost, a.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return ModelResponse{}, fmt.Errorf("create DeepSeek request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+a.apiKey)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	response, err := a.client.Do(httpRequest)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("DeepSeek request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return ModelResponse{}, fmt.Errorf("DeepSeek API returned HTTP status %s", response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, deepSeekMaxResponseBytes+1))
	if err != nil {
		return ModelResponse{}, fmt.Errorf("read DeepSeek response: %w", err)
	}
	if len(body) > deepSeekMaxResponseBytes {
		return ModelResponse{}, fmt.Errorf("DeepSeek response exceeds %d bytes", deepSeekMaxResponseBytes)
	}
	var decoded deepSeekResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return ModelResponse{}, fmt.Errorf("decode DeepSeek response: %w", err)
	}
	a.writeUsage(call, request.Role, decoded.Usage)
	if len(decoded.Choices) == 0 {
		return ModelResponse{}, errors.New("DeepSeek response has no choices")
	}
	choice := decoded.Choices[0]
	if choice.FinishReason == "length" {
		return ModelResponse{}, errors.New("DeepSeek response reached the output token limit; increase -max-tokens")
	}
	content := strings.TrimSpace(choice.Message.Content)
	if content == "" {
		return ModelResponse{}, errors.New("DeepSeek response content is empty")
	}
	result, err := parseDeepSeekContent(request.Role, content)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("parse DeepSeek %s response: %w", request.Role, err)
	}
	return result, nil
}

func deepSeekMessages(request ModelRequest) ([]deepSeekMessage, error) {
	system, err := deepSeekSystemPrompt(request.Role)
	if err != nil {
		return nil, err
	}
	input, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode model task: %w", err)
	}
	user := "Task details as JSON:\n" + string(input) + "\nReturn exactly one JSON object matching the required schema. Do not add markdown fences."
	return []deepSeekMessage{{Role: "system", Content: system}, {Role: "user", Content: user}}, nil
}

func deepSeekSystemPrompt(role string) (string, error) {
	const modelID = modelDeepSeekFlash
	switch role {
	case roleLead:
		return "You are the Lead in a task team. Break the requested goal into the requested number of independent tasks, unless the requested count is zero. Return JSON only with an assignments array. Every assignment must include target (search or planner), a unique id, model_id set to \"" + modelID + "\", and a concise instruction. Example JSON: {\"assignments\":[{\"target\":\"search\",\"id\":\"research-1\",\"model_id\":\"" + modelID + "\",\"instruction\":\"Find reliable evidence\"}]}. The output must be a JSON object.", nil
	case rolePlanner:
		return "You are a Planner worker. Either complete this task with a non-empty text field, or delegate smaller tasks with a non-empty assignments array. Return JSON only. Every assignment must include target (search or planner), a unique id, model_id set to \"" + modelID + "\", and an instruction. Respect the provided current depth and maximum depth; do not delegate when current depth is at the maximum. Examples: {\"text\":\"A concise plan\"} or {\"assignments\":[{\"target\":\"search\",\"id\":\"evidence\",\"model_id\":\"" + modelID + "\",\"instruction\":\"Find supporting evidence\"}]}. The output must be a JSON object.", nil
	case roleSearch:
		return "You are a Search worker. Complete the assigned research task and return JSON only with a non-empty text field containing findings. Do not delegate tasks. Example JSON: {\"text\":\"Relevant findings with concise evidence\"}. The output must be a JSON object.", nil
	default:
		return "", fmt.Errorf("unknown DeepSeek role %q", role)
	}
}

func parseDeepSeekContent(role, content string) (ModelResponse, error) {
	var decoded struct {
		Text        string          `json:"text"`
		Assignments json.RawMessage `json:"assignments"`
	}
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return ModelResponse{}, fmt.Errorf("invalid JSON object: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return ModelResponse{}, errors.New("multiple JSON values are not allowed")
		}
		return ModelResponse{}, fmt.Errorf("invalid trailing JSON: %w", err)
	}
	text := strings.TrimSpace(decoded.Text)
	var assignments []TaskAssignment
	if len(decoded.Assignments) != 0 {
		if bytes.Equal(bytes.TrimSpace(decoded.Assignments), []byte("null")) {
			return ModelResponse{}, errors.New("assignments must be an array")
		}
		if err := json.Unmarshal(decoded.Assignments, &assignments); err != nil {
			return ModelResponse{}, fmt.Errorf("assignments must be an array: %w", err)
		}
	}
	switch role {
	case roleLead:
		if text != "" {
			return ModelResponse{}, errors.New("Lead must return assignments, not text")
		}
		if len(decoded.Assignments) == 0 {
			return ModelResponse{}, errors.New("Lead response is missing assignments")
		}
	case rolePlanner:
		if len(assignments) != 0 && text != "" {
			return ModelResponse{}, errors.New("Planner must return either text or assignments")
		}
		if len(assignments) == 0 && text == "" {
			return ModelResponse{}, errors.New("Planner response needs text or assignments")
		}
	case roleSearch:
		if len(assignments) != 0 {
			return ModelResponse{}, errors.New("Search must not return assignments")
		}
		if text == "" {
			return ModelResponse{}, errors.New("Search response needs non-empty text")
		}
	default:
		return ModelResponse{}, fmt.Errorf("unknown DeepSeek role %q", role)
	}
	return ModelResponse{Text: text, Assignments: assignments}, nil
}

func (a *deepSeekAdapter) writeUsage(call graph.CallInfo, role string, usage *deepSeekUsage) {
	if a.usageWriter == nil {
		return
	}
	record := UsageRecord{
		Provider: "deepseek", Model: deepSeekProviderModel, Role: role,
		RunID: call.RunID, CallID: call.CallID, UsageAvailable: usage != nil,
	}
	if usage != nil {
		record.PromptTokens = usage.PromptTokens
		record.CompletionTokens = usage.CompletionTokens
		record.TotalTokens = usage.TotalTokens
	}
	a.usageMu.Lock()
	defer a.usageMu.Unlock()
	_ = json.NewEncoder(a.usageWriter).Encode(record)
}
