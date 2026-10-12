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
	openAIDefaultBaseURL = "https://api.openai.com/v1"
	openAIResponsesPath  = "/responses"
)

type openAIConfig struct {
	baseURL         string
	modelID         string
	model           string
	requestTimeout  time.Duration
	maxOutputTokens int
	client          *http.Client
	usageWriter     io.Writer
}

type openAIResponsesAdapter struct {
	apiKey          string
	endpoint        string
	modelID         string
	model           string
	requestTimeout  time.Duration
	maxOutputTokens int
	client          *http.Client
	usageWriter     io.Writer
	usageMu         sync.Mutex
}

func newOpenAIResponsesAdapter(apiKey string, config openAIConfig) (*openAIResponsesAdapter, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, errors.New("OPENAI_API_KEY is required by the configured model registry")
	}
	if config.modelID == "" || config.modelID != strings.TrimSpace(config.modelID) {
		return nil, errors.New("OpenAI ModelID must be non-empty and trimmed")
	}
	if config.model == "" || config.model != strings.TrimSpace(config.model) {
		return nil, errors.New("OpenAI model name must be non-empty and trimmed")
	}
	if config.baseURL == "" {
		config.baseURL = openAIDefaultBaseURL
	}
	if config.requestTimeout == 0 {
		config.requestTimeout = deepSeekDefaultTimeout
	}
	if config.maxOutputTokens == 0 {
		config.maxOutputTokens = deepSeekDefaultMaxTokens
	}
	if config.requestTimeout < 0 {
		return nil, errors.New("request timeout must be positive")
	}
	if config.maxOutputTokens < 1 {
		return nil, errors.New("maximum output tokens must be positive")
	}
	config.baseURL = strings.TrimSpace(config.baseURL)
	parsedURL, err := url.Parse(config.baseURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" || parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		return nil, errors.New("OpenAI base URL must be an HTTP(S) origin or base path")
	}
	client := config.client
	if client == nil {
		client = http.DefaultClient
	}
	return &openAIResponsesAdapter{
		apiKey: apiKey, endpoint: strings.TrimRight(config.baseURL, "/") + openAIResponsesPath,
		modelID: config.modelID, model: config.model, requestTimeout: config.requestTimeout,
		maxOutputTokens: config.maxOutputTokens, client: client, usageWriter: config.usageWriter,
	}, nil
}

type openAIResponsesRequest struct {
	Model           string `json:"model"`
	Instructions    string `json:"instructions"`
	Input           string `json:"input"`
	MaxOutputTokens int    `json:"max_output_tokens"`
	Store           bool   `json:"store"`
	Text            struct {
		Format struct {
			Type   string         `json:"type"`
			Name   string         `json:"name"`
			Strict bool           `json:"strict"`
			Schema map[string]any `json:"schema"`
		} `json:"format"`
	} `json:"text"`
}

type openAIResponsesUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type openAIResponsesResponse struct {
	Status string `json:"status"`
	Error  *struct {
		Code string `json:"code"`
	} `json:"error"`
	IncompleteDetails *struct {
		Reason string `json:"reason"`
	} `json:"incomplete_details"`
	Output []struct {
		Type    string `json:"type"`
		Status  string `json:"status"`
		Content []struct {
			Type    string `json:"type"`
			Text    string `json:"text"`
			Refusal string `json:"refusal"`
		} `json:"content"`
	} `json:"output"`
	Usage *openAIResponsesUsage `json:"usage"`
}

func (a *openAIResponsesAdapter) Generate(ctx context.Context, call graph.CallInfo, request ModelRequest) (ModelResponse, error) {
	instructions, err := teamSystemPrompt(request.Role, request.AvailableModels)
	if err != nil {
		return ModelResponse{}, err
	}
	schema, err := teamResponseSchema(request.Role, request.AvailableModels)
	if err != nil {
		return ModelResponse{}, err
	}
	input, err := json.Marshal(request)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("encode model task: %w", err)
	}
	body := openAIResponsesRequest{
		Model: a.model, Instructions: instructions,
		Input:           "Task details as JSON:\n" + string(input),
		MaxOutputTokens: a.maxOutputTokens,
		Store:           false,
	}
	body.Text.Format.Type = "json_schema"
	body.Text.Format.Name = "team_task_response"
	body.Text.Format.Strict = true
	body.Text.Format.Schema = schema
	encoded, err := json.Marshal(body)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("encode OpenAI Responses request: %w", err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, a.requestTimeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodPost, a.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return ModelResponse{}, fmt.Errorf("create OpenAI Responses request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+a.apiKey)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	response, err := a.client.Do(httpRequest)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("OpenAI Responses request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return ModelResponse{}, fmt.Errorf("OpenAI API returned HTTP status %s", response.Status)
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, deepSeekMaxResponseBytes+1))
	if err != nil {
		return ModelResponse{}, fmt.Errorf("read OpenAI Responses response: %w", err)
	}
	if len(responseBody) > deepSeekMaxResponseBytes {
		return ModelResponse{}, fmt.Errorf("OpenAI Responses response exceeds %d bytes", deepSeekMaxResponseBytes)
	}
	var decoded openAIResponsesResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return ModelResponse{}, fmt.Errorf("decode OpenAI Responses response: %w", err)
	}
	a.writeUsage(call, request.Role, decoded.Usage)
	if decoded.Error != nil {
		return ModelResponse{}, fmt.Errorf("OpenAI Responses API failed (code %q)", decoded.Error.Code)
	}
	if decoded.Status != "completed" {
		if decoded.IncompleteDetails != nil && decoded.IncompleteDetails.Reason == "max_output_tokens" {
			return ModelResponse{}, errors.New("OpenAI response reached the output token limit; increase -max-tokens")
		}
		return ModelResponse{}, fmt.Errorf("OpenAI response status is %q", decoded.Status)
	}
	content := make([]string, 0, 1)
	for _, item := range decoded.Output {
		if item.Type == "refusal" {
			return ModelResponse{}, errors.New("OpenAI model refused the request")
		}
		for _, part := range item.Content {
			switch part.Type {
			case "output_text":
				content = append(content, part.Text)
			case "refusal":
				return ModelResponse{}, errors.New("OpenAI model refused the request")
			}
		}
	}
	text := strings.TrimSpace(strings.Join(content, ""))
	if text == "" {
		return ModelResponse{}, errors.New("OpenAI response has no output text")
	}
	result, err := parseTeamModelContent(request.Role, text)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("parse OpenAI %s response: %w", request.Role, err)
	}
	return result, nil
}

func (a *openAIResponsesAdapter) writeUsage(call graph.CallInfo, role string, usage *openAIResponsesUsage) {
	if a.usageWriter == nil {
		return
	}
	record := UsageRecord{
		Provider: "openai", ModelID: a.modelID, Model: a.model, Role: role,
		RunID: call.RunID, CallID: call.CallID, UsageAvailable: usage != nil,
	}
	if usage != nil {
		record.PromptTokens = usage.InputTokens
		record.CompletionTokens = usage.OutputTokens
		record.TotalTokens = usage.TotalTokens
	}
	a.usageMu.Lock()
	defer a.usageMu.Unlock()
	_ = json.NewEncoder(a.usageWriter).Encode(record)
}
