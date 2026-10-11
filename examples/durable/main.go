package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
	"github.com/afterlune/luneGraph/observe"
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
	observed := flags.Bool("observe", false, "write execution events to stderr")
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
	opts := graph.Options{}
	if *observed {
		opts.Observer = observe.NewSlog(slog.New(slog.NewJSONHandler(diagnostics, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}
	var result graph.Result[state]
	if command == "start" {
		_, result, err = runner.Start(ctx, *runID, state{}, store, opts)
	} else {
		var execution *graph.Execution[state]
		execution, result, err = runner.Recover(ctx, *runID, nil, store, opts)
		if err != nil {
			return err
		}
		if result.Header.Completed {
			return graph.ErrRunCompleted
		}
		var id string
		for _, waiting := range execution.Pending() {
			if waiting.Continuation == "value" {
				id = waiting.InvocationID
				break
			}
		}
		if id == "" {
			return fmt.Errorf("run %q has no waiting value invocation", *runID)
		}
		result, err = execution.Advance(ctx, []graph.ResumeInput{{InvocationID: id, Payload: []byte(strconv.Itoa(value))}}, opts)
	}
	if err != nil {
		return err
	}
	if result.Final != nil {
		_, err = fmt.Fprintf(output, "status=%s run=%s value=%d\n", result.Status, result.Header.RunID, result.Final.Value)
	} else {
		_, err = fmt.Fprintf(output, "status=%s run=%s\n", result.Status, result.Header.RunID)
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
