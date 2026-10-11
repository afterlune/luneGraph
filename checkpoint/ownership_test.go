package checkpoint_test

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/memory"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func TestStoreTerminalOwnershipAndCreateShape(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var store graph.Store[map[string]int]
			if kind == "memory" {
				s, err := memory.New(func(s map[string]int) (map[string]int, error) { return maps.Clone(s), nil })
				if err != nil {
					t.Fatal(err)
				}
				store = s
			} else {
				s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "owned.db"), checkpoint.JSON[map[string]int]{})
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				store = s
			}
			final := map[string]int{"n": 1}
			cp := graph.Checkpoint[map[string]int]{FormatVersion: graph.CheckpointFormatVersion, RunID: "final", MachineID: "m", Revision: 1, Completed: true, Final: &final}
			if err := store.Create(ctx, cp); err != nil {
				t.Fatal(err)
			}
			final["n"] = 9
			got, err := store.Load(ctx, cp.RunID)
			if err != nil || (*got.Final)["n"] != 1 {
				t.Fatal(got, err)
			}
			(*got.Final)["n"] = 8
			got, err = store.Load(ctx, cp.RunID)
			if err != nil || (*got.Final)["n"] != 1 {
				t.Fatal(got, err)
			}
			header := cp.Header()
			header.Revision = 2
			if err := store.Commit(ctx, 1, graph.Change[map[string]int]{Header: header, Final: &final}); !errors.Is(err, graph.ErrRunCompleted) {
				t.Fatal(err)
			}
			cp.RunID = "failed"
			cp.Final = nil
			cp.Failure = &graph.Failure{Message: "failure", PanicStack: "stack"}
			if err := store.Create(ctx, cp); err != nil {
				t.Fatal(err)
			}
			cp.Failure.Message = "mutated"
			got, err = store.Load(ctx, cp.RunID)
			if err != nil || got.Failure.Message != "failure" {
				t.Fatal(got, err)
			}
			got.Failure.PanicStack = "mutated"
			got, err = store.Load(ctx, cp.RunID)
			if err != nil || got.Failure.PanicStack != "stack" {
				t.Fatal(got, err)
			}
			for _, change := range []func(*graph.Checkpoint[map[string]int]){
				func(c *graph.Checkpoint[map[string]int]) { c.Completed = false; c.Final = &final },
				func(c *graph.Checkpoint[map[string]int]) { c.Final = &final },
				func(c *graph.Checkpoint[map[string]int]) {
					c.Invocations = []graph.Invocation[map[string]int]{{ID: "i1"}}
				},
				func(c *graph.Checkpoint[map[string]int]) {
					c.Completed = false
					c.Failure = nil
					c.Invocations = []graph.Invocation[map[string]int]{{ID: "i1"}, {ID: "i1"}}
				},
			} {
				bad := cp
				bad.RunID = "bad"
				change(&bad)
				if err := store.Create(ctx, bad); !errors.Is(err, graph.ErrInvalidCheckpoint) {
					t.Fatal("malformed create", err)
				}
			}
		})
	}
}
