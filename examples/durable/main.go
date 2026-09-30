package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"

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
		return errors.New("usage: durable {start|resume} -db runs.db -run demo [-value 3]")
	}
	command := args[0]
	flags := flag.NewFlagSet("durable "+command, flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	dbPath := flags.String("db", "runs.db", "checkpoint database path (parent directory must exist)")
	runID := flags.String("run", "demo", "execution ID")
	var value int
	if command == "resume" {
		flags.IntVar(&value, "value", 0, "integer input for the waiting invocation")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	runner, err := newRunner()
	if err != nil {
		return err
	}
	store, err := sqlite.Open(ctx, *dbPath, checkpoint.JSON[state]{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	opts := graph.Options[state]{Store: store}
	var result graph.Result[state]
	if command == "start" {
		result, err = runner.Start(ctx, *runID, state{}, opts)
	} else {
		saved, loadErr := store.Load(ctx, *runID)
		if loadErr != nil {
			return loadErr
		}
		id, waitingErr := waitingInvocation(saved)
		if waitingErr != nil {
			return waitingErr
		}
		result, err = runner.Recover(ctx, *runID, []graph.ResumeInput{{
			InvocationID: id,
			Payload:      []byte(strconv.Itoa(value)),
		}}, opts)
	}
	if err != nil {
		return err
	}
	if result.Checkpoint.Final != nil {
		_, err = fmt.Fprintf(output, "status=%s run=%s value=%d\n", result.Status, result.Checkpoint.RunID, result.Checkpoint.Final.Value)
	} else {
		_, err = fmt.Fprintf(output, "status=%s run=%s\n", result.Status, result.Checkpoint.RunID)
	}
	return err
}

func waitingInvocation(saved graph.Checkpoint[state]) (string, error) {
	if saved.Completed {
		return "", graph.ErrRunCompleted
	}
	for _, invocation := range saved.Invocations {
		if invocation.Status == graph.InvocationWaiting && invocation.Continuation == "value" {
			return invocation.ID, nil
		}
	}
	return "", fmt.Errorf("run %q has no waiting value invocation", saved.RunID)
}
