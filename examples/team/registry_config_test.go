package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const mixedRegistryJSON = `{
	"lead_model_id": "openai/lead",
	"worker_model_ids": ["openai/worker", "deepseek/search"],
	"models": {
		"openai/lead": {"provider": "openai", "model": "test-openai", "description": "lead"},
		"openai/worker": {"provider": "openai", "model": "test-openai", "description": "planning"},
		"deepseek/search": {"provider": "deepseek", "model": "test-deepseek", "description": "research"}
	}
}`

func TestDecodeAndBuildMixedTeamRegistry(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "openai-test-key")
	t.Setenv("DEEPSEEK_API_KEY", "deepseek-test-key")
	config, err := decodeTeamRegistryConfig(strings.NewReader(mixedRegistryJSON))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(config.WorkerModelIDs, ",") != "deepseek/search,openai/worker" {
		t.Fatalf("normalized worker IDs = %v", config.WorkerModelIDs)
	}
	var usage bytes.Buffer
	registry, options, machineID, err := buildTeamModelRegistry(config, 3*time.Second, 700, &usage)
	if err != nil {
		t.Fatal(err)
	}
	if len(registry) != 3 || len(options) != 2 {
		t.Fatalf("registry=%d options=%+v", len(registry), options)
	}
	if _, ok := registry["deepseek/search"].(*deepSeekAdapter); !ok {
		t.Fatalf("deepseek adapter type = %T", registry["deepseek/search"])
	}
	if _, ok := registry["openai/lead"].(*openAIResponsesAdapter); !ok {
		t.Fatalf("openai lead adapter type = %T", registry["openai/lead"])
	}
	if _, ok := registry["openai/worker"].(*openAIResponsesAdapter); !ok {
		t.Fatalf("openai worker adapter type = %T", registry["openai/worker"])
	}
	if !strings.HasPrefix(machineID, "team-provider-registry-v1-") || strings.Contains(machineID, "test-key") {
		t.Fatalf("MachineID = %q", machineID)
	}
	if options[0].ModelID != "deepseek/search" || options[0].Description != "research" || options[1].ModelID != "openai/worker" {
		t.Fatalf("worker options = %+v", options)
	}
	if usage.Len() != 0 {
		t.Fatalf("registry construction unexpectedly logged usage: %s", usage.String())
	}
}

func TestTeamRegistryMachineIDIsStableAndTracksMeaning(t *testing.T) {
	first, err := decodeTeamRegistryConfig(strings.NewReader(mixedRegistryJSON))
	if err != nil {
		t.Fatal(err)
	}
	secondJSON := `{"models":{"deepseek/search":{"provider":"deepseek","model":"test-deepseek","description":"research"},"openai/worker":{"provider":"openai","model":"test-openai","description":"planning"},"openai/lead":{"provider":"openai","model":"test-openai","description":"lead"}},"worker_model_ids":["deepseek/search","openai/worker"],"lead_model_id":"openai/lead"}`
	second, err := decodeTeamRegistryConfig(strings.NewReader(secondJSON))
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := teamRegistryMachineID(first)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := teamRegistryMachineID(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstID != secondID {
		t.Fatalf("equivalent config IDs differ: %q vs %q", firstID, secondID)
	}
	second.Models["openai/worker"] = teamModelDefinition{Provider: "openai", Model: "changed-model", Description: "planning"}
	changedID, err := teamRegistryMachineID(second)
	if err != nil {
		t.Fatal(err)
	}
	if changedID == firstID {
		t.Fatal("MachineID did not change with provider model mapping")
	}
	second.Models["openai/worker"] = teamModelDefinition{Provider: "openai", Model: "test-openai", Description: "planning"}
	second.WorkerModelIDs = []string{"deepseek/search"}
	changedID, err = teamRegistryMachineID(second)
	if err != nil {
		t.Fatal(err)
	}
	if changedID == firstID {
		t.Fatal("MachineID did not change with worker allow-list")
	}
}

func TestTeamRegistryConfigRejectsInvalidAndSecretFields(t *testing.T) {
	for _, test := range []struct {
		name string
		json string
		want string
	}{
		{name: "empty", json: `{}`, want: "lead_model_id"},
		{name: "unknown lead", json: `{"lead_model_id":"missing","worker_model_ids":["worker"],"models":{"worker":{"provider":"openai","model":"test"}}}`, want: "not defined"},
		{name: "unknown worker", json: `{"lead_model_id":"lead","worker_model_ids":["missing"],"models":{"lead":{"provider":"openai","model":"test"}}}`, want: "undefined ModelID"},
		{name: "duplicate workers", json: `{"lead_model_id":"lead","worker_model_ids":["worker","worker"],"models":{"lead":{"provider":"openai","model":"test"},"worker":{"provider":"deepseek","model":"test"}}}`, want: "duplicate ModelID"},
		{name: "unknown provider", json: `{"lead_model_id":"lead","worker_model_ids":["worker"],"models":{"lead":{"provider":"openai","model":"test"},"worker":{"provider":"other","model":"test"}}}`, want: "unsupported provider"},
		{name: "missing api model", json: `{"lead_model_id":"lead","worker_model_ids":["lead"],"models":{"lead":{"provider":"openai","model":" "}}}`, want: "provider model name"},
		{name: "api key in config", json: `{"lead_model_id":"lead","worker_model_ids":["lead"],"models":{"lead":{"provider":"openai","model":"test","api_key":"secret"}}}`, want: "unknown field"},
		{name: "trailing value", json: mixedRegistryJSON + ` {}`, want: "one JSON value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := decodeTeamRegistryConfig(strings.NewReader(test.json))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("decode error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestTeamRegistryRequiresKeysForConfiguredProviders(t *testing.T) {
	config, err := decodeTeamRegistryConfig(strings.NewReader(mixedRegistryJSON))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("DEEPSEEK_API_KEY", "deepseek-key")
	if _, _, _, err := buildTeamModelRegistry(config, time.Second, 128, nil); err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Fatalf("missing OpenAI key error = %v", err)
	}
	t.Setenv("OPENAI_API_KEY", "openai-key")
	t.Setenv("DEEPSEEK_API_KEY", "")
	if _, _, _, err := buildTeamModelRegistry(config, time.Second, 128, nil); err == nil || !strings.Contains(err.Error(), "DEEPSEEK_API_KEY") {
		t.Fatalf("missing DeepSeek key error = %v", err)
	}
}

func TestRegistryCLIRequiresConfigAndCredentialsBeforeOpeningDatabase(t *testing.T) {
	var output, diagnostics bytes.Buffer
	db := filepath.Join(t.TempDir(), "registry.db")
	if err := run(context.Background(), []string{"start", "-provider", "registry", "-db", db}, &output, &diagnostics); err == nil || !strings.Contains(err.Error(), "requires -config") {
		t.Fatalf("missing config error = %v", err)
	}
	t.Setenv("DEEPSEEK_API_KEY", "deepseek-key")
	t.Setenv("OPENAI_API_KEY", "")
	configPath := filepath.Join(t.TempDir(), "models.json")
	if err := os.WriteFile(configPath, []byte(mixedRegistryJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), []string{"start", "-provider", "registry", "-config", configPath, "-db", db}, &output, &diagnostics)
	if err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Fatalf("missing provider key error = %v", err)
	}
	if _, statErr := os.Stat(db); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("database state after missing provider key = %v, want not created", statErr)
	}
	if err := run(context.Background(), []string{"start", "-provider", "offline", "-config", configPath, "-db", db}, &output, &diagnostics); err == nil || !strings.Contains(err.Error(), "only accepted") {
		t.Fatalf("config with non-registry mode error = %v", err)
	}
	if err := run(context.Background(), []string{"start", "-provider", "registry", "-config", filepath.Join(t.TempDir(), "missing.json"), "-db", db}, &output, &diagnostics); err == nil {
		t.Fatal("accepted missing registry file")
	}
}
