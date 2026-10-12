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
	resultExecution, result, err := runner.Start(context.Background(), "resume-run", 7, nil, Options{MaxConcurrency: 1})
	if err != nil || result.Status != StatusWaiting {
		t.Fatalf("create waiting checkpoint = %+v, %v", result, err)
	}
	return mustCheckpoint(resultExecution)
}

func TestResumeWaitingInvocationAndDecodeBoundaries(t *testing.T) {
	runner := executorTestWaitingRunner(t, nil)
	checkpoint := executorTestWaitingCheckpoint(t, runner)
	inv := checkpoint.Invocations[0]
	if inv.Status != InvocationWaiting || inv.Continuation != "resume" || inv.CallID == "" || len(inv.Next) != 1 {
		t.Fatalf("waiting invocation = %+v", inv)
	}

	decoded := []ResumeInput{{InvocationID: inv.ID, CallID: inv.CallID, Payload: []byte("bad")}}
	resultExecution, result, err := runner.Resume(context.Background(), checkpoint, decoded, nil, Options{})
	if err == nil || result.Status != StatusWaiting || result.Header.Revision != checkpoint.Revision || mustCheckpoint(resultExecution).Invocations[0].State != 7 {
		t.Fatalf("decode failure changed checkpoint: %+v, %v", result, err)
	}

	for name, inputs := range map[string][]ResumeInput{
		"duplicate invocation": {decoded[0], decoded[0]},
		"not waiting":          {{InvocationID: "missing", Payload: []byte("1")}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := runner.Resume(context.Background(), checkpoint, inputs, nil, Options{}); err == nil {
				t.Fatal("invalid resume inputs accepted")
			}
		})
	}
	_, result, err = runner.Resume(context.Background(), checkpoint, []ResumeInput{{InvocationID: inv.ID, CallID: inv.CallID, Payload: []byte("5")}}, nil, Options{})
	if err != nil || result.Status != StatusCompleted || result.Final == nil || *result.Final != 12 {
		t.Fatalf("resumed execution = %+v, %v", result, err)
	}
}

func TestResumeValidationStoreAndCommitErrors(t *testing.T) {
	runner := executorTestWaitingRunner(t, nil)
	checkpoint := executorTestWaitingCheckpoint(t, runner)
	input := []ResumeInput{{InvocationID: checkpoint.Invocations[0].ID, CallID: checkpoint.Invocations[0].CallID, Payload: []byte("1")}}

	if _, _, err := runner.Resume(nil, checkpoint, nil, nil, Options{}); err == nil {
		t.Fatal("Resume accepted nil context")
	}
	if _, _, err := (*Runner[int])(nil).Resume(context.Background(), checkpoint, nil, nil, Options{}); err == nil {
		t.Fatal("Resume accepted nil runner")
	}
	if _, _, err := runner.Resume(context.Background(), checkpoint, nil, nil, Options{MaxSteps: -1}); err == nil {
		t.Fatal("Resume accepted invalid options")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, result, err := runner.Resume(cancelled, checkpoint, nil, nil, Options{}); !errors.Is(err, context.Canceled) || result.Status != StatusCancelled {
		t.Fatalf("cancelled Resume = %+v, %v", result, err)
	}

	invalid := checkpoint
	invalid.FormatVersion++
	if _, result, err := runner.Resume(context.Background(), invalid, nil, nil, Options{}); !errors.Is(err, ErrInvalidCheckpoint) || result.Status != StatusFailed {
		t.Fatalf("invalid checkpoint Resume = %+v, %v", result, err)
	}
	completedRunner := executorTestRunner(t, "done", []NodeSpec[int]{executorTestNode("done", executorTestEnd, FailInvocation)}, nil, nil, nil, nil)
	completedExecution, _, err := completedRunner.Start(context.Background(), "completed", 1, nil, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := completedRunner.Resume(context.Background(), mustCheckpoint(completedExecution), nil, nil, Options{}); !errors.Is(err, ErrRunCompleted) {
		t.Fatalf("completed checkpoint Resume error = %v", err)
	}

	store := newExecutorTestStore()
	store.values[checkpoint.RunID] = copyCheckpoint(checkpoint)
	stale := checkpoint
	stale.Revision--
	if _, _, err := runner.Resume(context.Background(), stale, nil, store, Options{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Resume reference error = %v", err)
	}
	store.loadErr = errors.New("load failed")
	if _, result, err := runner.Resume(context.Background(), checkpoint, nil, store, Options{}); err == nil || result.Status != StatusFailed {
		t.Fatalf("Resume load error = %+v, %v", result, err)
	}
	store.loadErr = nil
	store.compareErr = errors.New("write failed")
	_, result, err := runner.Resume(context.Background(), checkpoint, input, store, Options{})
	if err == nil || result.Status != StatusFailed || result.Header.Revision != checkpoint.Revision {
		t.Fatalf("Resume CAS error = %+v, %v", result, err)
	}
}

func TestResumeInputApplyFailureAndRevisionLimit(t *testing.T) {
	boom := errors.New("continuation failed")
	runner := executorTestWaitingRunner(t, func(context.Context, CallInfo, int, int) (int, error) { return 0, boom })
	checkpoint := executorTestWaitingCheckpoint(t, runner)
	input := []ResumeInput{{InvocationID: checkpoint.Invocations[0].ID, CallID: checkpoint.Invocations[0].CallID, Payload: []byte("3")}}
	_, local, err := runner.Resume(context.Background(), checkpoint, input, nil, Options{})
	if err != nil || local.Status != StatusCompletedWithFailures || (!local.Header.HadLocalFailures || local.Header.Failure != nil) {
		t.Fatalf("local continuation failure = %+v, %v", local, err)
	}
	override := FailExecution
	_, terminal, err := runner.Resume(context.Background(), checkpoint, input, nil, Options{FailureOverride: &override})
	if !errors.Is(err, boom) || terminal.Status != StatusFailed || !terminal.Header.Completed || terminal.Header.Revision != checkpoint.Revision+1 {
		t.Fatalf("terminal continuation failure = %+v, %v", terminal, err)
	}

	checkpoint.Revision = ^uint64(0)
	runner = executorTestWaitingRunner(t, nil)
	input[0].InvocationID = checkpoint.Invocations[0].ID
	_, result, err := runner.Resume(context.Background(), checkpoint, input, nil, Options{})
	if !errors.Is(err, ErrExecutionLimit) || result.Header.Revision != checkpoint.Revision {
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
	options := Options{}
	optionsStore := store

	if _, _, err := runner.Recover(nil, "resume-run", nil, optionsStore, options); err == nil {
		t.Fatal("Recover accepted nil context")
	}
	if _, _, err := (*Runner[int])(nil).Recover(context.Background(), "resume-run", nil, optionsStore, options); err == nil {
		t.Fatal("Recover accepted nil runner")
	}
	if _, _, err := runner.Recover(context.Background(), " padded", nil, optionsStore, options); err == nil {
		t.Fatal("Recover accepted invalid run ID")
	}
	if _, _, err := runner.Recover(context.Background(), "resume-run", nil, nil, Options{}); err == nil {
		t.Fatal("Recover accepted missing store")
	}
	if _, _, err := runner.Recover(context.Background(), "resume-run", nil, store, Options{MaxConcurrency: -1}); err == nil {
		t.Fatal("Recover accepted invalid options")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, result, err := runner.Recover(cancelled, "resume-run", nil, optionsStore, options); !errors.Is(err, context.Canceled) || result.Status != StatusCancelled {
		t.Fatalf("cancelled Recover = %+v, %v", result, err)
	}

	store.loadErr = errors.New("load failed")
	if _, result, err := runner.Recover(context.Background(), "resume-run", nil, optionsStore, options); err == nil || result.Status != StatusFailed {
		t.Fatalf("Recover load error = %+v, %v", result, err)
	}
	store.loadErr = nil
	ctx, cancelAfterLoad := context.WithCancel(context.Background())
	cancelling := executorCancellingStore{executorTestStore: store, cancel: cancelAfterLoad}
	if _, result, err := runner.Recover(ctx, "resume-run", nil, cancelling, Options{}); !errors.Is(err, context.Canceled) || result.Status != StatusCancelled {
		t.Fatalf("post-load cancellation = %+v, %v", result, err)
	}

	store.values["resume-run"] = copyCheckpoint(checkpoint)
	wrongRun := copyCheckpoint(checkpoint)
	wrongRun.RunID = "other-run"
	store.values["resume-run"] = wrongRun
	if _, result, err := runner.Recover(context.Background(), "resume-run", nil, optionsStore, options); !errors.Is(err, ErrInvalidCheckpoint) || result.Status != StatusFailed {
		t.Fatalf("mismatched run ID = %+v, %v", result, err)
	}
	invalid := copyCheckpoint(checkpoint)
	invalid.MachineID = "old-machine"
	store.values["resume-run"] = invalid
	if _, _, err := runner.Recover(context.Background(), "resume-run", nil, optionsStore, options); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("invalid recovered checkpoint error = %v", err)
	}

	completedRunner := executorTestRunner(t, "done", []NodeSpec[int]{executorTestNode("done", executorTestEnd, FailInvocation)}, nil, nil, nil, nil)
	_, completed, err := completedRunner.Start(context.Background(), "terminal-run", 4, store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := completedRunner.Recover(context.Background(), "terminal-run", nil, store, Options{})
	if err != nil || result.Status != StatusCompleted || result.Header.Revision != completed.Header.Revision {
		t.Fatalf("completed recovery = %+v, %v", result, err)
	}
	if _, _, err := completedRunner.Recover(context.Background(), "terminal-run", []ResumeInput{{InvocationID: "i1", CallID: "unused"}}, store, Options{}); !errors.Is(err, ErrRunCompleted) {
		t.Fatalf("completed run accepted inputs: %v", err)
	}

	failedRunner := executorTestRunner(t, "fail", []NodeSpec[int]{executorTestNode("fail", func(context.Context, CallInfo, int) (Transition[int], error) {
		return Transition[int]{}, errors.New("original")
	}, FailExecution)}, nil, nil, nil, nil)
	_, failed, err := failedRunner.Start(context.Background(), "failed-run", 1, store, Options{})
	if err == nil || failed.Status != StatusFailed {
		t.Fatalf("failed Start = %+v, %v", failed, err)
	}
	_, recovered, err := failedRunner.Recover(context.Background(), "failed-run", nil, store, Options{})
	if !errors.Is(err, ErrRunFailed) || recovered.Status != StatusFailed || recovered.Header.Failure == nil {
		t.Fatalf("failed recovery = %+v, %v", recovered, err)
	}

	store.loadErr = errors.New("missing")
	if _, _, err := runner.Recover(context.Background(), "absent", nil, optionsStore, options); err == nil {
		t.Fatal("Recover ignored store load error")
	}
}

func TestRecoverResumesActiveCheckpoint(t *testing.T) {
	runner := executorTestWaitingRunner(t, nil)
	initial := executorTestWaitingCheckpoint(t, runner)
	store := newExecutorTestStore()
	store.values[initial.RunID] = copyCheckpoint(initial)
	_, result, err := runner.Recover(context.Background(), initial.RunID, []ResumeInput{{InvocationID: initial.Invocations[0].ID, CallID: initial.Invocations[0].CallID, Payload: []byte("2")}}, store, Options{})
	if err != nil || result.Status != StatusCompleted || result.Final == nil || *result.Final != 9 {
		t.Fatalf("active recovery = %+v, %v", result, err)
	}
}
