package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

func TestExecutionFailureSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "failed.db")
	store, err := sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("persistent failure")
	calls := 0
	g := graph.New[int]("work")
	if err := g.AddNode(graph.NodeSpec[int]{Name: "work", OnError: graph.FailExecution, Run: func(context.Context, graph.CallInfo, int) (graph.Transition[int], error) {
		calls++
		return graph.Transition[int]{}, boom
	}}); err != nil {
		t.Fatal(err)
	}
	runner, err := g.Compile(graph.Config[int]{MachineID: "failure-v1", Clone: func(value int) (int, error) { return value, nil }})
	if err != nil {
		t.Fatal(err)
	}
	first, err := runner.Start(ctx, "failed", 0, graph.Options[int]{Store: store})
	if !errors.Is(err, boom) || first.Status != graph.StatusFailed || first.Checkpoint.Revision != 2 {
		t.Fatalf("failed run = %+v, %v", first, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, path, checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	recovered, err := runner.Recover(ctx, "failed", nil, graph.Options[int]{Store: store})
	if !errors.Is(err, graph.ErrRunFailed) || recovered.Status != graph.StatusFailed || recovered.Checkpoint.Revision != 2 || recovered.Checkpoint.Failure.Message != boom.Error() || calls != 1 {
		t.Fatalf("reopened failure = %+v, %v; calls=%d", recovered, err, calls)
	}
}
