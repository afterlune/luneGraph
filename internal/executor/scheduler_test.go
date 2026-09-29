package executor

import (
	"context"
	"errors"
	"math"
	"reflect"
	"runtime"
	"testing"
	"time"

	"lune-graph/internal/model"
)

func TestSchedulerHelpers(t *testing.T) {
	options, err := normalizeOptions(Options[int]{})
	if err != nil || options.MaxSteps != defaultMaxSteps || options.MaxConcurrency != runtime.GOMAXPROCS(0) {
		t.Fatalf("normalized defaults = %+v, %v", options, err)
	}
	for _, invalid := range []Options[int]{{MaxSteps: -1}, {MaxConcurrency: -1}, {FailureOverride: scopePointer(FailureScope(99))}} {
		if _, err := normalizeOptions(invalid); err == nil {
			t.Fatalf("invalid options accepted: %+v", invalid)
		}
	}
	if _, err := normalizeOptions(Options[int]{FailureOverride: scopePointer(FailExecution)}); err != nil {
		t.Fatalf("valid failure override rejected: %v", err)
	}

	if errorStatus(context.Canceled) != StatusCancelled || errorStatus(context.DeadlineExceeded) != StatusCancelled || errorStatus(errors.New("failed")) != StatusFailed {
		t.Fatal("errorStatus returned an incorrect status")
	}
	if statusOf(Checkpoint[int]{Completed: true, Failures: []Failure{{Scope: FailExecution}}}, false) != StatusFailed {
		t.Fatal("terminal execution failure was not reported")
	}
	if statusOf(Checkpoint[int]{Completed: true, Failures: []Failure{{Scope: FailInvocation}}}, false) != StatusCompletedWithFailures {
		t.Fatal("local failure was not reported")
	}
	if statusOf(Checkpoint[int]{Completed: true}, false) != StatusCompleted {
		t.Fatal("completed checkpoint status incorrect")
	}
	ready := Checkpoint[int]{Invocations: []Invocation[int]{{Status: InvocationReady}}}
	if statusOf(ready, true) != StatusBudget || statusOf(ready, false) != StatusWaiting {
		t.Fatal("ready checkpoint status incorrect")
	}
	waiting := Checkpoint[int]{Invocations: []Invocation[int]{{Status: InvocationWaiting}}}
	if statusOf(waiting, true) != StatusWaiting {
		t.Fatal("waiting checkpoint status incorrect")
	}

	for _, checkpoint := range []Checkpoint[int]{
		{},
		{Invocations: []Invocation[int]{{Status: InvocationEnded}}},
	} {
		markCompleted(&checkpoint)
		if !checkpoint.Completed {
			t.Fatalf("terminal checkpoint was not completed: %+v", checkpoint)
		}
	}
	for _, checkpoint := range []Checkpoint[int]{
		{Groups: []ActivationGroup{{ID: "g1"}}},
		{Invocations: []Invocation[int]{{Status: InvocationReady}}},
		{Invocations: []Invocation[int]{{Status: InvocationWaiting}}},
		{Invocations: []Invocation[int]{{Status: InvocationGroup}}},
	} {
		markCompleted(&checkpoint)
		if checkpoint.Completed {
			t.Fatalf("active checkpoint completed: %+v", checkpoint)
		}
	}

	if invocationNumber("i12") != 12 || invocationNumber("bad") != 0 || availableCommits(Checkpoint[int]{Steps: 2, Revision: 10}) != math.MaxUint64-10 {
		t.Fatal("counter helper returned an incorrect value")
	}
	if availableCommits(Checkpoint[int]{Steps: math.MaxUint64 - 1, Revision: 1}) != 1 {
		t.Fatal("availableCommits did not select the smaller limit")
	}
	items := Checkpoint[int]{Invocations: []Invocation[int]{
		{ID: "i9", Status: InvocationReady},
		{ID: "i2", Status: InvocationReady},
		{ID: "i5", Status: InvocationWaiting},
	}}
	running := map[string]context.CancelFunc{}
	if next := nextReady(items, running, 2); next == nil || next.ID != "i9" {
		t.Fatalf("next after cursor = %+v", next)
	}
	if next := nextReady(items, running, 9); next == nil || next.ID != "i2" {
		t.Fatalf("wrapped next = %+v", next)
	}
	running["i2"] = func() {}
	if next := nextReady(items, running, 9); next == nil || next.ID != "i9" {
		t.Fatalf("running invocation selected: %+v", next)
	}
	if hasReady(items) != true || hasReady(Checkpoint[int]{}) {
		t.Fatal("hasReady returned an incorrect result")
	}
	if !reflect.DeepEqual([]string{nextReady(items, nil, 2).ID}, []string{"i9"}) {
		t.Fatal("round-robin selection was unstable")
	}
}

func TestRunnerStartBudgetAndInputErrors(t *testing.T) {
	runner := executorTestRunner(t, "loop", []NodeSpec[int]{executorTestNode("loop", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
		if value == 2 {
			return model.EndExecution(value), nil
		}
		return model.To(value+1, "loop"), nil
	}, FailInvocation)}, nil, map[string][]string{"loop": {"loop"}}, nil, nil)
	first, err := runner.Start(context.Background(), "budget", 0, Options[int]{MaxSteps: 1})
	if err != nil || first.Status != StatusBudget || first.Checkpoint.Steps != 1 || first.Checkpoint.Invocations[0].State != 1 {
		t.Fatalf("budget start = %+v, %v", first, err)
	}
	last, err := runner.Resume(context.Background(), first.Checkpoint, nil, Options[int]{})
	if err != nil || last.Status != StatusCompleted || last.Checkpoint.Final == nil || *last.Checkpoint.Final != 2 {
		t.Fatalf("resumed run = %+v, %v", last, err)
	}

	if _, err := runner.Start(nil, "bad", 0, Options[int]{}); err == nil {
		t.Fatal("Start accepted nil context")
	}
	if _, err := runner.Start(context.Background(), " padded", 0, Options[int]{}); err == nil {
		t.Fatal("Start accepted invalid run ID")
	}
	if _, err := runner.Start(context.Background(), "negative", 0, Options[int]{MaxSteps: -1}); err == nil {
		t.Fatal("Start accepted negative step limit")
	}
	if _, err := (*Runner[int])(nil).Start(context.Background(), "nil-runner", 0, Options[int]{}); err == nil {
		t.Fatal("Start accepted nil runner")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := runner.Start(ctx, "cancelled", 0, Options[int]{}); !errors.Is(err, context.Canceled) || result.Status != StatusCancelled {
		t.Fatalf("cancelled start = %+v, %v", result, err)
	}

	cloneFailure, err := New(Machine[int]{ID: "clone", Entry: "node", Clone: func(int) (int, error) { return 0, errors.New("clone") }, Nodes: map[string]NodeSpec[int]{"node": executorTestNode("node", executorTestEnd, FailInvocation)}})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := cloneFailure.Start(context.Background(), "clone-failure", 0, Options[int]{}); err == nil || result.Checkpoint.Revision != 0 {
		t.Fatalf("clone failure = %+v, %v", result, err)
	}
	store := newExecutorTestStore()
	store.createErr = errors.New("create failed")
	if result, err := runner.Start(context.Background(), "create-failure", 0, Options[int]{Store: store}); err == nil || result.Status != StatusFailed {
		t.Fatalf("store create failure = %+v, %v", result, err)
	}
}

func TestRunnerSchedulesParallelCallbacksAndDrainsCancellation(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	nodes := []NodeSpec[int]{
		executorTestNode("fork", func(_ context.Context, _ CallInfo, value int) (Transition[int], error) {
			return model.To(value, "a", "b"), nil
		}, FailInvocation),
	}
	edges := map[string][]string{"fork": {"a", "b"}}
	for _, name := range []string{"a", "b"} {
		name := name
		nodes = append(nodes, executorTestNode(name, func(ctx context.Context, _ CallInfo, value int) (Transition[int], error) {
			started <- name
			select {
			case <-release:
				return model.EndBranch(value), nil
			case <-ctx.Done():
				return Transition[int]{}, ctx.Err()
			}
		}, FailInvocation))
	}
	runner := executorTestRunner(t, "fork", nodes, nil, edges, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan Result[int], 1)
	go func() {
		result, _ := runner.Start(ctx, "parallel", 0, Options[int]{MaxConcurrency: 2})
		finished <- result
	}()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			close(release)
			t.Fatal("scheduler did not start both callbacks")
		}
	}
	close(release)
	if result := <-finished; result.Status != StatusCompleted || len(result.Checkpoint.Terminals) != 2 {
		t.Fatalf("parallel result = %+v", result)
	}

	startedCancel := make(chan struct{})
	cancelRunner := executorTestRunner(t, "block", []NodeSpec[int]{executorTestNode("block", func(ctx context.Context, _ CallInfo, value int) (Transition[int], error) {
		close(startedCancel)
		<-ctx.Done()
		return Transition[int]{}, ctx.Err()
	}, FailInvocation)}, nil, nil, nil, nil)
	cancelCtx, cancelRun := context.WithCancel(context.Background())
	resultChannel := make(chan struct {
		result Result[int]
		err    error
	}, 1)
	go func() {
		result, err := cancelRunner.Start(cancelCtx, "cancel-drain", 0, Options[int]{})
		resultChannel <- struct {
			result Result[int]
			err    error
		}{result: result, err: err}
	}()
	<-startedCancel
	cancelRun()
	select {
	case output := <-resultChannel:
		if !errors.Is(output.err, context.Canceled) || output.result.Status != StatusCancelled {
			t.Fatalf("cancelled run = %+v, %v", output.result, output.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("scheduler did not drain cancelled callback")
	}
}

func scopePointer(scope FailureScope) *FailureScope { return &scope }
