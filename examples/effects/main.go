package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
	"github.com/afterlune/luneGraph/examples/effects/internal/ledger"
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
	if len(args) == 0 || (args[0] != "start" && args[0] != "recover") {
		return errors.New("usage: effects {start|recover} -checkpoints runs.db -effects effects.db -run demo [-delta 3]")
	}
	flags := flag.NewFlagSet("effects "+args[0], flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	checkpoints := flags.String("checkpoints", "runs.db", "checkpoint database")
	effects := flags.String("effects", "effects.db", "application effect database")
	runID := flags.String("run", "demo", "execution ID")
	var delta int64
	if args[0] == "start" {
		flags.Int64Var(&delta, "delta", 1, "counter increment")
	}
	if err = flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	cpPath, err := filepath.Abs(*checkpoints)
	if err != nil {
		return err
	}
	effectPath, err := filepath.Abs(*effects)
	if err != nil {
		return err
	}
	// Separate files make the two independent commit boundaries explicit.
	if strings.EqualFold(cpPath, effectPath) {
		return errors.New("checkpoint and effect databases must use separate files")
	}
	store, err := sqlite.Open(ctx, cpPath, checkpoint.JSON[state]{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	l, err := ledger.Open(ctx, effectPath)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, l.Close()) }()
	runner, err := newRunner(effect(l))
	if err != nil {
		return err
	}
	options := graph.Options[state]{Store: store}
	var result graph.Result[state]
	if args[0] == "start" {
		result, err = runner.Start(ctx, *runID, state{Delta: delta}, options)
	} else {
		result, err = runner.Recover(ctx, *runID, nil, options)
	}
	if err != nil {
		return err
	}
	if result.Checkpoint.Final == nil {
		return errors.New("execution has no final value")
	}
	_, err = fmt.Fprintf(output, "status=%s run=%s value=%d\n", result.Status, *runID, result.Checkpoint.Final.Value)
	return err
}
