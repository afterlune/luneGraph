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
	if len(args) == 0 || (args[0] != "start" && args[0] != "resume") {
		return errors.New("usage: team {start|resume} -db runs.db -run demo [-tasks 2] [-invocation iN -call cN -approved=true]")
	}
	command := args[0]
	flags := flag.NewFlagSet("team "+command, flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	dbPath := flags.String("db", "runs.db", "SQLite checkpoint path (parent directory must exist)")
	runID := flags.String("run", "team-demo", "execution ID")
	taskCount := flags.Int("tasks", 2, "number of tasks requested from the offline Lead (start only, 0-16)")
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
	if command == "resume" && (*invocationID == "" || *callID == "") {
		return errors.New("resume requires the original -invocation and -call values")
	}
	runner, err := newTeamRunner(offlineModelRegistry())
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
	if command == "start" {
		execution, result, err = runner.Start(ctx, *runID, teamState{
			Goal:           "Research a question and produce a concise plan.",
			RequestedTasks: *taskCount,
			ModelID:        modelLead,
			MaxDepth:       1,
		}, store, graph.Options{})
	} else {
		payload := "false"
		if *approved {
			payload = "true"
		}
		execution, result, err = runner.Recover(ctx, *runID, []graph.ResumeInput{{InvocationID: *invocationID, CallID: *callID, Payload: []byte(payload)}}, store, graph.Options{})
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
