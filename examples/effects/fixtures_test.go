package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
	"github.com/afterlune/luneGraph/examples/effects/internal/ledger"
)

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func openFixture(t *testing.T) (*sqlite.Store[state], *ledger.Ledger, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := sqlite.Open(context.Background(), filepath.Join(dir, "runs.db"), checkpoint.JSON[state]{})
	check(t, err)
	t.Cleanup(func() { check(t, store.Close()) })
	path := filepath.Join(dir, "effects.db")
	l, err := ledger.Open(context.Background(), path)
	check(t, err)
	t.Cleanup(func() { check(t, l.Close()) })
	return store, l, path
}

func assertEffects(t *testing.T, path string, wantValue, wantReceipts int64) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	check(t, err)
	defer db.Close()
	var value, receipts int64
	check(t, db.QueryRow("SELECT value FROM counters WHERE namespace=?", namespace).Scan(&value))
	check(t, db.QueryRow("SELECT count(*) FROM receipts WHERE namespace=?", namespace).Scan(&receipts))
	if value != wantValue || receipts != wantReceipts {
		t.Fatalf("effects: value=%d receipts=%d; want %d/%d", value, receipts, wantValue, wantReceipts)
	}
}

func inputsFor(cp graph.Checkpoint[state]) []graph.ResumeInput {
	for _, inv := range cp.Invocations {
		if inv.Status == graph.InvocationWaiting {
			return []graph.ResumeInput{{InvocationID: inv.ID, Payload: []byte("3")}}
		}
	}
	return nil
}

func callbackRunner(t *testing.T, kind string, add addEffect) *graph.Runner[state] {
	t.Helper()
	if kind == "node" {
		r, err := newRunner(add)
		check(t, err)
		return r
	}
	entry := "fork"
	if kind == "continuation" {
		entry = "wait"
	}
	g := graph.New[state](entry)
	check(t, g.AddNode(graph.NodeSpec[state]{Name: "done", Run: func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
		return graph.EndExecution(s), nil
	}}))
	if kind == "continuation" {
		check(t, g.AddNode(graph.NodeSpec[state]{Name: "wait", OnError: graph.FailExecution, Run: func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
			return graph.Wait(s, "add", "done"), nil
		}}))
		check(t, g.AddEdge("wait", "done"))
		check(t, graph.RegisterContinuation(g, "add", func(p []byte) (int64, error) { return strconv.ParseInt(string(p), 10, 64) }, func(ctx context.Context, call graph.CallInfo, s state, delta int64) (state, error) {
			value, err := add(ctx, call, delta)
			s.Value = value
			return s, err
		}))
	} else {
		check(t, g.AddNode(graph.NodeSpec[state]{Name: "fork", Run: func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
			return graph.To(s, "left", "right"), nil
		}}))
		for _, name := range []string{"left", "right"} {
			check(t, g.AddNode(graph.NodeSpec[state]{Name: name, Run: func(_ context.Context, _ graph.CallInfo, s state) (graph.Transition[state], error) {
				return graph.To(s, "join"), nil
			}}))
		}
		check(t, g.AddJoin(graph.JoinSpec[state]{Name: "join", From: "fork", OnError: graph.FailExecution, Merge: func(ctx context.Context, call graph.CallInfo, states []state) (state, error) {
			s := states[0]
			value, err := add(ctx, call, s.Delta)
			s.Value = value
			return s, err
		}}))
		for _, name := range []string{"left", "right"} {
			check(t, g.AddEdge("fork", name))
			check(t, g.AddEdge(name, "join"))
		}
		check(t, g.AddEdge("join", "done"))
	}
	r, err := g.Compile(graph.Config[state]{MachineID: "effects-test-" + kind, Clone: func(s state) (state, error) { return s, nil }})
	check(t, err)
	return r
}
