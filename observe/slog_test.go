package observe_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/observe"
)

func TestSlogLevelsAndMetadata(t *testing.T) {
	var output bytes.Buffer
	observer := observe.NewSlog(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	for _, operation := range []graph.EventOperation{graph.OperationStart, graph.OperationResume, graph.OperationRecover, graph.OperationNode, graph.OperationJoin, graph.OperationDecode, graph.OperationApply, graph.OperationCreate, graph.OperationLoad, graph.OperationCompareAndSwap} {
		for _, failed := range []bool{false, true} {
			output.Reset()
			event := graph.Event{Operation: operation, Phase: graph.PhaseFinished, OperationID: 7, RunID: "run", MachineID: "machine", InvocationID: "i1", CallID: "c2", Node: "worker/work", Continuation: "worker/input", Revision: 9, Time: time.Unix(123, 0), Duration: time.Second, Status: graph.StatusWaiting, Action: graph.ActionWait}
			if failed {
				event.Err = errors.New("write acknowledgement missing")
			}
			observer.Observe(context.Background(), event)
			var fields map[string]any
			if err := json.Unmarshal(output.Bytes(), &fields); err != nil {
				t.Fatal(err)
			}
			wantLevel := "DEBUG"
			if operation == graph.OperationStart || operation == graph.OperationResume || operation == graph.OperationRecover {
				wantLevel = "INFO"
			}
			if failed {
				wantLevel = "ERROR"
			}
			if fields["level"] != wantLevel || fields["operation"] != string(operation) || fields["phase"] != "finished" || fields["operation_id"] != float64(7) || fields["run_id"] != "run" || fields["machine_id"] != "machine" || fields["invocation_id"] != "i1" || fields["call_id"] != "c2" || fields["node"] != "worker/work" || fields["continuation"] != "worker/input" || fields["revision"] != float64(9) || fields["duration"] != float64(time.Second) || fields["status"] != "waiting" || fields["action"] != float64(graph.ActionWait) || fields["event_time"] != event.Time.Format(time.RFC3339Nano) {
				t.Fatalf("metadata = %+v", fields)
			}
			if failed && fields["error"] != event.Err.Error() {
				t.Fatalf("error = %+v", fields)
			}
		}
	}
}

func TestSlogFilteringAndDefault(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	previous := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(previous) })
	observer := observe.NewSlog(nil)
	observer.Observe(context.Background(), graph.Event{Operation: graph.OperationNode, Phase: graph.PhaseStarted})
	if output.Len() != 0 {
		t.Fatal("Debug emitted at default Info level")
	}
	observer.Observe(context.Background(), graph.Event{Operation: graph.OperationRecover, Phase: graph.PhaseStarted})
	var fields map[string]any
	if err := json.Unmarshal(output.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if fields["operation"] != "recover" {
		t.Fatal("nil logger did not use default")
	}
	for _, field := range []string{"duration", "status", "action", "error", "call_id", "invocation_id", "node", "continuation"} {
		if _, exists := fields[field]; exists {
			t.Fatalf("unexpected optional field %s", field)
		}
	}
	filtered := observe.NewSlog(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	if allocs := testing.AllocsPerRun(100, func() { filtered.Observe(context.Background(), graph.Event{Operation: graph.OperationNode}) }); allocs != 0 {
		t.Fatalf("filtered event allocated: %v", allocs)
	}
}

func TestSlogResolvedOutcome(t *testing.T) {
	var output bytes.Buffer
	observer := observe.NewSlog(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	observer.Observe(context.Background(), graph.Event{Operation: graph.OperationJoin, Phase: graph.PhaseResolved, Outcome: graph.OutcomeUnknown, CallID: "c2", Revision: 4, Err: errors.New("unconfirmed")})
	var fields map[string]any
	if err := json.Unmarshal(output.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if fields["phase"] != "resolved" || fields["outcome"] != "unknown" || fields["level"] != "ERROR" || fields["call_id"] != "c2" || fields["revision"] != float64(4) {
		t.Fatalf("resolution fields=%+v", fields)
	}
	if _, exists := fields["duration"]; exists {
		t.Fatal("resolution has callback duration")
	}
}
