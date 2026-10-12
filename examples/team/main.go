package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output, diagnostics io.Writer) (err error) {
	if len(args) == 0 || (args[0] != "start" && args[0] != "resume" && args[0] != "continue") {
		return errors.New("usage: team {start|resume|continue} -db runs.db -run demo [-provider offline|deepseek|registry] [-config models.json] [-tasks 2] [-max-invocations N] [-invocation iN -call cN -approved=true]")
	}
	command := args[0]
	flags := flag.NewFlagSet("team "+command, flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	dbPath := flags.String("db", "runs.db", "SQLite checkpoint path (parent directory must exist)")
	runID := flags.String("run", "team-demo", "execution ID")
	provider := flags.String("provider", "offline", "model mode: offline, deepseek, or registry")
	modelConfigPath := flags.String("config", "", "JSON model registry config (required by provider=registry)")
	taskCount := flags.Int("tasks", 2, "number of tasks requested from the Lead (start only, 0-16)")
	maxInvocations := flags.Int("max-invocations", 0, "maximum invocation records in the execution (0 = unlimited)")
	requestTimeout := flags.Duration("request-timeout", deepSeekDefaultTimeout, "provider HTTP request timeout")
	maxTokens := flags.Int("max-tokens", deepSeekDefaultMaxTokens, "maximum output tokens per provider call")
	invocationID := flags.String("invocation", "", "original waiting invocation ID (resume)")
	callID := flags.String("call", "", "original waiting CallID (resume)")
	approved := flags.Bool("approved", true, "approval decision (resume)")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if command == "start" && (*taskCount < 0 || *taskCount > 16) {
		return errors.New("-tasks must be between 0 and 16")
	}
	if *maxInvocations < 0 {
		return errors.New("-max-invocations must not be negative")
	}
	if *requestTimeout <= 0 {
		return errors.New("-request-timeout must be positive")
	}
	if *maxTokens <= 0 {
		return errors.New("-max-tokens must be positive")
	}
	if command == "resume" && (*invocationID == "" || *callID == "") {
		return errors.New("resume requires the original -invocation and -call values")
	}
	if command != "resume" && (*invocationID != "" || *callID != "") {
		return errors.New("-invocation and -call are only accepted by resume")
	}
	if *provider == "registry" && *modelConfigPath == "" {
		return errors.New("-provider registry requires -config")
	}
	if *provider != "registry" && *modelConfigPath != "" {
		return errors.New("-config is only accepted with -provider registry")
	}
	var models ModelRegistry
	var workerModels []ModelOption
	var machineID, startModelID string
	switch *provider {
	case "offline":
		models = offlineModelRegistry()
		workerModels = modelOptionsFromRegistry(models)
		machineID = "team-model-adapters-v2"
		startModelID = modelLead
	case "deepseek":
		apiKey := strings.TrimSpace(os.Getenv("DEEPSEEK_API_KEY"))
		if apiKey == "" {
			return errors.New("DEEPSEEK_API_KEY is required when -provider=deepseek")
		}
		models, err = deepSeekModelRegistry(apiKey, deepSeekConfig{
			requestTimeout: *requestTimeout,
			maxTokens:      *maxTokens,
			usageWriter:    diagnostics,
		})
		if err != nil {
			return err
		}
		workerModels = []ModelOption{{ModelID: modelDeepSeekFlash, Provider: "deepseek", Model: deepSeekProviderModel}}
		machineID = "team-deepseek-flash-v1"
		startModelID = modelDeepSeekFlash
	case "registry":
		config, configErr := loadTeamRegistryConfig(*modelConfigPath)
		if configErr != nil {
			return configErr
		}
		models, workerModels, machineID, err = buildTeamModelRegistry(config, *requestTimeout, *maxTokens, diagnostics)
		if err != nil {
			return err
		}
		startModelID = config.LeadModelID
	default:
		return fmt.Errorf("unknown provider %q; expected offline, deepseek, or registry", *provider)
	}
	runner, err := newTeamRunnerWithModelOptions(models, machineID, workerModels)
	if err != nil {
		return err
	}
	store, err := sqlite.Open(ctx, *dbPath, checkpoint.JSON[teamState]{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	var execution *graph.Execution[teamState]
	var result graph.Result[teamState]
	options := graph.Options{MaxInvocations: *maxInvocations}
	if command == "start" {
		execution, result, err = runner.Start(ctx, *runID, teamState{
			Goal:           "Research a question and produce a concise plan.",
			RequestedTasks: *taskCount,
			ModelID:        startModelID,
			MaxDepth:       1,
		}, store, options)
	} else if command == "resume" {
		payload := "false"
		if *approved {
			payload = "true"
		}
		execution, result, err = runner.Recover(ctx, *runID, []graph.ResumeInput{{InvocationID: *invocationID, CallID: *callID, Payload: []byte(payload)}}, store, options)
	} else {
		execution, result, err = runner.Recover(ctx, *runID, nil, store, options)
	}
	if err != nil {
		return err
	}
	if result.Status == graph.StatusWaiting {
		found := false
		for _, invocation := range execution.Pending() {
			if invocation.Continuation == "approval" {
				found = true
				if _, err := fmt.Fprintf(output, "status=waiting run=%s invocation=%s call=%s\n", *runID, invocation.InvocationID, invocation.CallID); err != nil {
					return err
				}
			}
		}
		if !found {
			return errors.New("approval continuation is not pending")
		}
		return nil
	}
	if result.Final != nil {
		if _, err := fmt.Fprintf(output, "status=%s run=%s approved=%t results=%s\n", result.Status, *runID, result.Final.Approved, strings.Join(result.Final.Results, ",")); err != nil {
			return err
		}
		return nil
	}
	if _, err := fmt.Fprintf(output, "status=%s run=%s\n", result.Status, *runID); err != nil {
		return err
	}
	return nil
}
