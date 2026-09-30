package executor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
)

func executorTestWaitingRunner(t *testing.T, apply func(context.Context, CallInfo, int, int) (int, error)) *Runner[int] {
	t.Helper()
	if apply == nil {
		apply = func(_ context.Context, _ CallInfo, state, value int) (int, error) { return state + value, nil }
	}
	continuation := Continuation[int]{
		Decode: func(payload []byte) (any, error) {
			if string(payload) == "bad" {
				return nil, errors.New("invalid payload")
			}
			var value int
			if _, err := fmt.Sscan(string(payload), &value); err != nil {
				return nil, err
			}
			return value, nil
		},
		Apply: func(ctx context.Context, call CallInfo, state int, value any) (int, error) {
			return apply(ctx, call, state, value.(int))
		},
	}
	return executorTestRunner(t, "pause", []NodeSpec[int]{
		executorTestNode("pause", func(_ context.Context, _ CallInfo, state int) (Transition[int], error) {
			return model.Wait(state, "resume", "done"), nil
		}, FailInvocation),
		executorTestNode("done", executorTestEnd, FailInvocation),
	}, nil, map[string][]string{"pause": {"done"}}, map[string]Continuation[int]{"resume": continuation}, nil)
}

func executorTestWaitingCheckpoint(t *testing.T, runner *Runner[int]) Checkpoint[int] {
	t.Helper()
	result, err := runner.Start(context.Background(), "resume-run", 7, Options[int]{MaxConcurrency: 1})
	if err != nil || result.Status != StatusWaiting {
		t.Fatalf("create waiting checkpoint = %+v, %v", result, err)
	}
	return result.Checkpoint
}

func TestResumeWaitingInvocationAndDecodeBoundaries(t *testing.T) {
	runner := executorTestWaitingRunner(t, nil)
	checkpoint := executorTestWaitingCheckpoint(t, runner)
	inv := checkpoint.Invocations[0]
	if inv.Status != InvocationWaiting || inv.Continuation != "resume" || inv.CallID == "" || len(inv.Next) != 1 {
		t.Fatalf("waiting invocation = %+v", inv)
	}

	decoded := []ResumeInput{{InvocationID: inv.ID, Payload: []byte("bad")}}
	result, err := runner.Resume(context.Background(), checkpoint, decoded, Options[int]{})
	if err == nil || result.Status != StatusWaiting || result.Checkpoint.Revision != checkpoint.Revision || result.Checkpoint.Invocations[0].State != 7 {
		t.Fatalf("decode failure changed checkpoint: %+v, %v", result, err)
	}

	for name, inputs := range map[string][]ResumeInput{
		"duplicate invocation": {decoded[0], decoded[0]},
		"not waiting":          {{InvocationID: "missing", Payload: []byte("1")}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := runner.Resume(context.Background(), checkpoint, inputs, Options[int]{}); err == nil {
				t.Fatal("invalid resume inputs accepted")
			}
		})
	}

	result, err = runner.Resume(context.Background(), checkpoint, []ResumeInput{{InvocationID: inv.ID, Payload: []byte("5")}}, Options[int]{})
	if err != nil || result.Status != StatusCompleted || result.Checkpoint.Final == nil || *result.Checkpoint.Final != 12 {
		t.Fatalf("resumed execution = %+v, %v", result, err)
	}
}

func TestResumeValidationStoreAndCommitErrors(t *testing.T) {
	runner := executorTestWaitingRunner(t, nil)
	checkpoint := executorTestWaitingCheckpoint(t, runner)
	input := []ResumeInput{{InvocationID: checkpoint.Invocations[0].ID, Payload: []byte("1")}}

	if _, err := runner.Resume(nil, checkpoint, nil, Options[int]{}); err == nil {
		t.Fatal("Resume accepted nil context")
	}
	if _, err := (*Runner[int])(nil).Resume(context.Background(), checkpoint, nil, Options[int]{}); err == nil {
		t.Fatal("Resume accepted nil runner")
	}
	if _, err := runner.Resume(context.Background(), checkpoint, nil, Options[int]{MaxSteps: -1}); err == nil {
		t.Fatal("Resume accepted invalid options")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := runner.Resume(cancelled, checkpoint, nil, Options[int]{}); !errors.Is(err, context.Canceled) || result.Status != StatusCancelled {
		t.Fatalf("cancelled Resume = %+v, %v", result, err)
	}

	invalid := checkpoint
	invalid.FormatVersion++
	if result, err := runner.Resume(context.Background(), invalid, nil, Options[int]{}); !errors.Is(err, ErrInvalidCheckpoint) || result.Status != StatusFailed {
		t.Fatalf("invalid checkpoint Resume = %+v, %v", result, err)
	}
	completedRunner := executorTestRunner(t, "done", []NodeSpec[int]{executorTestNode("done", executorTestEnd, FailInvocation)}, nil, nil, nil, nil)
	completed, err := completedRunner.Start(context.Background(), "completed", 1, Options[int]{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := completedRunner.Resume(context.Background(), completed.Checkpoint, nil, Options[int]{}); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("completed checkpoint Resume error = %v", err)
	}

	store := newExecutorTestStore()
	store.values[checkpoint.RunID] = copyCheckpoint(checkpoint)
	stale := checkpoint
	stale.Revision--
	if _, err := runner.Resume(context.Background(), stale, nil, Options[int]{Store: store}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Resume reference error = %v", err)
	}
	store.loadErr = errors.New("load failed")
	if result, err := runner.Resume(context.Background(), checkpoint, nil, Options[int]{Store: store}); err == nil || result.Status != StatusFailed {
		t.Fatalf("Resume load error = %+v, %v", result, err)
	}
	store.loadErr = nil
	store.compareErr = errors.New("write failed")
	result, err := runner.Resume(context.Background(), checkpoint, input, Options[int]{Store: store})
	if err == nil || result.Status != StatusFailed || result.Checkpoint.Revision != checkpoint.Revision {
		t.Fatalf("Resume CAS error = %+v, %v", result, err)
	}
}

func TestResumeInputApplyFailureAndRevisionLimit(t *testing.T) {
	boom := errors.New("continuation failed")
	runner := executorTestWaitingRunner(t, func(context.Context, CallInfo, int, int) (int, error) { return 0, boom })
	checkpoint := executorTestWaitingCheckpoint(t, runner)
	input := []ResumeInput{{InvocationID: checkpoint.Invocations[0].ID, Payload: []byte("3")}}

	local, err := runner.Resume(context.Background(), checkpoint, input, Options[int]{})
	if err != nil || local.Status != StatusCompletedWithFailures || len(local.Checkpoint.Failures) != 1 || local.Checkpoint.Failures[0].Scope != FailInvocation {
		t.Fatalf("local continuation failure = %+v, %v", local, err)
	}
	override := FailExecution
	terminal, err := runner.Resume(context.Background(), checkpoint, input, Options[int]{FailureOverride: &override})
	if !errors.Is(err, boom) || terminal.Status != StatusFailed || !terminal.Checkpoint.Completed || terminal.Checkpoint.Revision != checkpoint.Revision+1 {
		t.Fatalf("terminal continuation failure = %+v, %v", terminal, err)
	}

	checkpoint.Revision = ^uint64(0)
	runner = executorTestWaitingRunner(t, nil)
	input[0].InvocationID = checkpoint.Invocations[0].ID
	result, err := runner.Resume(context.Background(), checkpoint, input, Options[int]{})
	if !errors.Is(err, ErrExecutionLimit) || result.Checkpoint.Revision != checkpoint.Revision {
		t.Fatalf("resume revision overflow = %+v, %v", result, err)
	}
}

type executorCancellingStore struct {
	*executorTestStore
	cancel context.CancelFunc
}

func (s executorCancellingStore) Load(ctx context.Context, runID string) (Checkpoint[int], error) {
	checkpoint, err := s.executorTestStore.Load(ctx, runID)
	s.cancel()
	return checkpoint, err
}

func TestRecoverValidationAndTerminalCheckpoints(t *testing.T) {
	runner := executorTestWaitingRunner(t, nil)
	checkpoint := executorTestWaitingCheckpoint(t, runner)
	store := newExecutorTestStore()
	store.values[checkpoint.RunID] = copyCheckpoint(checkpoint)
	options := Options[int]{Store: store}

	if _, err := runner.Recover(nil, "resume-run", nil, options); err == nil {
		t.Fatal("Recover accepted nil context")
	}
	if _, err := (*Runner[int])(nil).Recover(context.Background(), "resume-run", nil, options); err == nil {
		t.Fatal("Recover accepted nil runner")
	}
	if _, err := runner.Recover(context.Background(), " padded", nil, options); err == nil {
		t.Fatal("Recover accepted invalid run ID")
	}
	if _, err := runner.Recover(context.Background(), "resume-run", nil, Options[int]{}); err == nil {
		t.Fatal("Recover accepted missing store")
	}
	if _, err := runner.Recover(context.Background(), "resume-run", nil, Options[int]{Store: store, MaxConcurrency: -1}); err == nil {
		t.Fatal("Recover accepted invalid options")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := runner.Recover(cancelled, "resume-run", nil, options); !errors.Is(err, context.Canceled) || result.Status != StatusCancelled {
		t.Fatalf("cancelled Recover = %+v, %v", result, err)
	}

	store.loadErr = errors.New("load failed")
	if result, err := runner.Recover(context.Background(), "resume-run", nil, options); err == nil || result.Status != StatusFailed {
		t.Fatalf("Recover load error = %+v, %v", result, err)
	}
	store.loadErr = nil
	ctx, cancelAfterLoad := context.WithCancel(context.Background())
	cancelling := executorCancellingStore{executorTestStore: store, cancel: cancelAfterLoad}
	if result, err := runner.Recover(ctx, "resume-run", nil, Options[int]{Store: cancelling}); !errors.Is(err, context.Canceled) || result.Status != StatusCancelled {
		t.Fatalf("post-load cancellation = %+v, %v", result, err)
	}

	store.values["resume-run"] = copyCheckpoint(checkpoint)
	wrongRun := copyCheckpoint(checkpoint)
	wrongRun.RunID = "other-run"
	store.values["resume-run"] = wrongRun
	if result, err := runner.Recover(context.Background(), "resume-run", nil, options); !errors.Is(err, ErrInvalidCheckpoint) || result.Status != StatusFailed {
		t.Fatalf("mismatched run ID = %+v, %v", result, err)
	}
	invalid := copyCheckpoint(checkpoint)
	invalid.MachineID = "old-machine"
	store.values["resume-run"] = invalid
	if _, err := runner.Recover(context.Background(), "resume-run", nil, options); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("invalid recovered checkpoint error = %v", err)
	}

	completedRunner := executorTestRunner(t, "done", []NodeSpec[int]{executorTestNode("done", executorTestEnd, FailInvocation)}, nil, nil, nil, nil)
	completed, err := completedRunner.Start(context.Background(), "terminal-run", 4, Options[int]{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	result, err := completedRunner.Recover(context.Background(), "terminal-run", nil, Options[int]{Store: store})
	if err != nil || result.Status != StatusCompleted || result.Checkpoint.Revision != completed.Checkpoint.Revision {
		t.Fatalf("completed recovery = %+v, %v", result, err)
	}
	if _, err := completedRunner.Recover(context.Background(), "terminal-run", []ResumeInput{{InvocationID: "i1"}}, Options[int]{Store: store}); !errors.Is(err, ErrRunCompleted) {
		t.Fatalf("completed run accepted inputs: %v", err)
	}

	failedRunner := executorTestRunner(t, "fail", []NodeSpec[int]{executorTestNode("fail", func(context.Context, CallInfo, int) (Transition[int], error) {
		return Transition[int]{}, errors.New("original")
	}, FailExecution)}, nil, nil, nil, nil)
	failed, err := failedRunner.Start(context.Background(), "failed-run", 1, Options[int]{Store: store})
	if err == nil || failed.Status != StatusFailed {
		t.Fatalf("failed Start = %+v, %v", failed, err)
	}
	recovered, err := failedRunner.Recover(context.Background(), "failed-run", nil, Options[int]{Store: store})
	if !errors.Is(err, ErrRunFailed) || recovered.Status != StatusFailed || len(recovered.Checkpoint.Failures) != 1 {
		t.Fatalf("failed recovery = %+v, %v", recovered, err)
	}

	store.loadErr = errors.New("missing")
	if _, err := runner.Recover(context.Background(), "absent", nil, options); err == nil {
		t.Fatal("Recover ignored store load error")
	}
}

func TestRecoverResumesActiveCheckpoint(t *testing.T) {
	runner := executorTestWaitingRunner(t, nil)
	initial := executorTestWaitingCheckpoint(t, runner)
	store := newExecutorTestStore()
	store.values[initial.RunID] = copyCheckpoint(initial)
	result, err := runner.Recover(context.Background(), initial.RunID, []ResumeInput{{InvocationID: initial.Invocations[0].ID, Payload: []byte("2")}}, Options[int]{Store: store})
	if err != nil || result.Status != StatusCompleted || result.Checkpoint.Final == nil || *result.Checkpoint.Final != 9 {
		t.Fatalf("active recovery = %+v, %v", result, err)
	}
}
