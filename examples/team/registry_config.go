package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"
)

type teamRegistryConfig struct {
	LeadModelID    string                         `json:"lead_model_id"`
	WorkerModelIDs []string                       `json:"worker_model_ids"`
	Models         map[string]teamModelDefinition `json:"models"`
}

type teamModelDefinition struct {
	Provider    string `json:"provider"`
	Model       string `json:"model"`
	Description string `json:"description,omitempty"`
}

func loadTeamRegistryConfig(path string) (teamRegistryConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return teamRegistryConfig{}, fmt.Errorf("open model registry config: %w", err)
	}
	defer file.Close()
	return decodeTeamRegistryConfig(file)
}

func decodeTeamRegistryConfig(reader io.Reader) (teamRegistryConfig, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var config teamRegistryConfig
	if err := decoder.Decode(&config); err != nil {
		return teamRegistryConfig{}, fmt.Errorf("decode model registry config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return teamRegistryConfig{}, errors.New("model registry config must contain one JSON value")
		}
		return teamRegistryConfig{}, fmt.Errorf("decode trailing model registry config: %w", err)
	}
	if err := normalizeTeamRegistryConfig(&config); err != nil {
		return teamRegistryConfig{}, err
	}
	return config, nil
}

func normalizeTeamRegistryConfig(config *teamRegistryConfig) error {
	if config.LeadModelID == "" || config.LeadModelID != strings.TrimSpace(config.LeadModelID) {
		return errors.New("lead_model_id must be a non-empty, trimmed ModelID")
	}
	if len(config.Models) == 0 {
		return errors.New("models must contain at least one model definition")
	}
	if _, exists := config.Models[config.LeadModelID]; !exists {
		return fmt.Errorf("lead_model_id %q is not defined in models", config.LeadModelID)
	}
	if len(config.WorkerModelIDs) == 0 {
		return errors.New("worker_model_ids must contain at least one ModelID")
	}
	for id, definition := range config.Models {
		if id == "" || id != strings.TrimSpace(id) {
			return errors.New("models contains an empty or untrimmed ModelID")
		}
		if definition.Model == "" || definition.Model != strings.TrimSpace(definition.Model) {
			return fmt.Errorf("model %q must have a non-empty, trimmed provider model name", id)
		}
		if definition.Description != strings.TrimSpace(definition.Description) {
			return fmt.Errorf("model %q description must be trimmed", id)
		}
		switch definition.Provider {
		case "deepseek", "openai":
		default:
			return fmt.Errorf("model %q has unsupported provider %q", id, definition.Provider)
		}
	}
	seen := make(map[string]struct{}, len(config.WorkerModelIDs))
	for _, id := range config.WorkerModelIDs {
		if id == "" || id != strings.TrimSpace(id) {
			return errors.New("worker_model_ids contains an empty or untrimmed ModelID")
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("worker_model_ids contains duplicate ModelID %q", id)
		}
		seen[id] = struct{}{}
		if _, exists := config.Models[id]; !exists {
			return fmt.Errorf("worker_model_ids references undefined ModelID %q", id)
		}
	}
	slices.Sort(config.WorkerModelIDs)
	return nil
}

func buildTeamModelRegistry(config teamRegistryConfig, requestTimeout time.Duration, maxOutputTokens int, usageWriter io.Writer) (ModelRegistry, []ModelOption, string, error) {
	if err := normalizeTeamRegistryConfig(&config); err != nil {
		return nil, nil, "", err
	}
	if requestTimeout <= 0 {
		return nil, nil, "", errors.New("request timeout must be positive")
	}
	if maxOutputTokens <= 0 {
		return nil, nil, "", errors.New("maximum output tokens must be positive")
	}
	usageWriter = newSerializedWriter(usageWriter)
	options := make([]ModelOption, 0, len(config.WorkerModelIDs))
	registry := make(ModelRegistry, len(config.Models))
	for _, id := range config.WorkerModelIDs {
		definition := config.Models[id]
		options = append(options, ModelOption{
			ModelID: id, Provider: definition.Provider, Model: definition.Model, Description: definition.Description,
		})
	}
	modelIDs := make([]string, 0, len(config.Models))
	for id := range config.Models {
		modelIDs = append(modelIDs, id)
	}
	slices.Sort(modelIDs)
	for _, id := range modelIDs {
		definition := config.Models[id]
		switch definition.Provider {
		case "deepseek":
			apiKey := strings.TrimSpace(os.Getenv("DEEPSEEK_API_KEY"))
			if apiKey == "" {
				return nil, nil, "", errors.New("DEEPSEEK_API_KEY is required by the configured model registry")
			}
			adapter, err := newDeepSeekAdapter(apiKey, deepSeekConfig{
				providerModel: definition.Model, requestTimeout: requestTimeout,
				maxTokens: maxOutputTokens, usageWriter: usageWriter,
			})
			if err != nil {
				return nil, nil, "", fmt.Errorf("configure model %q: %w", id, err)
			}
			adapter.modelID = id
			registry[id] = adapter
		case "openai":
			apiKey := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
			if apiKey == "" {
				return nil, nil, "", errors.New("OPENAI_API_KEY is required by the configured model registry")
			}
			adapter, err := newOpenAIResponsesAdapter(apiKey, openAIConfig{
				modelID: id, model: definition.Model, requestTimeout: requestTimeout,
				maxOutputTokens: maxOutputTokens, usageWriter: usageWriter,
			})
			if err != nil {
				return nil, nil, "", fmt.Errorf("configure model %q: %w", id, err)
			}
			registry[id] = adapter
		}
	}
	machineID, err := teamRegistryMachineID(config)
	if err != nil {
		return nil, nil, "", err
	}
	return registry, options, machineID, nil
}

func teamRegistryMachineID(config teamRegistryConfig) (string, error) {
	if err := normalizeTeamRegistryConfig(&config); err != nil {
		return "", err
	}
	type fingerprintModel struct {
		ID          string `json:"id"`
		Provider    string `json:"provider"`
		Model       string `json:"model"`
		Description string `json:"description,omitempty"`
	}
	type fingerprintConfig struct {
		Version        string             `json:"version"`
		LeadModelID    string             `json:"lead_model_id"`
		WorkerModelIDs []string           `json:"worker_model_ids"`
		Models         []fingerprintModel `json:"models"`
	}
	fingerprint := fingerprintConfig{
		Version: "team-provider-registry-v1", LeadModelID: config.LeadModelID,
		WorkerModelIDs: slices.Clone(config.WorkerModelIDs),
	}
	modelIDs := make([]string, 0, len(config.Models))
	for id := range config.Models {
		modelIDs = append(modelIDs, id)
	}
	slices.Sort(modelIDs)
	for _, id := range modelIDs {
		definition := config.Models[id]
		fingerprint.Models = append(fingerprint.Models, fingerprintModel{
			ID: id, Provider: definition.Provider, Model: definition.Model, Description: definition.Description,
		})
	}
	encoded, err := json.Marshal(fingerprint)
	if err != nil {
		return "", fmt.Errorf("encode model registry identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return fingerprint.Version + "-" + hex.EncodeToString(digest[:]), nil
}
