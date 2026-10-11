package executor

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/afterlune/luneGraph/internal/model"
	"github.com/afterlune/luneGraph/internal/observation"
)

// Resume applies inputs to waiting invocations and advances a saved execution.
func (r *Runner[S]) Resume(ctx context.Context, checkpoint Checkpoint[S], inputs []ResumeInput, opts Options[S]) (result Result[S], retErr error) {
	if ctx == nil {
		return resultWith(checkpoint, StatusFailed), errors.New("context must not be nil")
	}
	if r == nil {
		return resultWith(checkpoint, StatusFailed), errors.New("runner is nil")
	}
	obs := observation.New(opts.Observer, r.id, checkpoint.RunID)
	if obs != nil {
		span := obs.Begin(ctx, model.Event{Operation: model.OperationResume, Revision: checkpoint.Revision})
		defer func() {
			if result.Status != "" {
				obs.DiscardPending(ctx, retErr)
				span.End(ctx, model.Event{Revision: result.Checkpoint.Revision, Status: result.Status, Err: retErr})
			}
		}()
		opts.Store = observation.WrapStore(opts.Store, obs)
	}
	var err error
	if opts, err = normalizeOptions(opts); err != nil {
		return resultWith(checkpoint, StatusFailed), err
	}
	if err = ctx.Err(); err != nil {
		return resultWith(checkpoint, StatusCancelled), err
	}
	if opts.Store != nil {
		if !validName(checkpoint.RunID) || checkpoint.MachineID != r.id || checkpoint.Revision == 0 || checkpoint.FormatVersion != CheckpointFormatVersion {
			return resultWith(checkpoint, StatusFailed), invalidCheckpoint("invalid resume reference")
		}
		latest, loadErr := opts.Store.Load(ctx, checkpoint.RunID)
		if loadErr != nil {
			return resultWith(checkpoint, errorStatus(loadErr)), loadErr
		}
		if latest.MachineID != checkpoint.MachineID || latest.Revision != checkpoint.Revision || latest.RunID != checkpoint.RunID {
			return resultWith(checkpoint, StatusFailed), ErrConflict
		}
		checkpoint = latest
	}
	if err := r.validateCheckpoint(checkpoint); err != nil {
		return resultWith(checkpoint, StatusFailed), err
	}
	if checkpoint.Completed {
		return resultWith(checkpoint, StatusFailed), ErrRunCompleted
	}
	return r.resumeValidated(ctx, checkpoint, inputs, opts, obs)
}

// resumeValidated advances a loaded active checkpoint without reading the Store again.
func (r *Runner[S]) resumeValidated(ctx context.Context, checkpoint Checkpoint[S], inputs []ResumeInput, opts Options[S], obs *observation.Session) (Result[S], error) {
	l := newLive(checkpoint)
	l.failureOverride = opts.FailureOverride
	if len(inputs) > 0 && l.header.Revision == math.MaxUint64 {
		return resultWith(l.snapshot(), StatusFailed), ErrExecutionLimit
	}
	type decoded struct {
		id    string
		value any
	}
	decodedInputs := make([]decoded, 0, len(inputs))
	seen := make(map[string]bool)
	for _, input := range inputs {
		if seen[input.InvocationID] {
			return resultWith(l.snapshot(), StatusFailed), fmt.Errorf("duplicate resume input for %q", input.InvocationID)
		}
		seen[input.InvocationID] = true
		inv, ok := l.invocation(input.InvocationID)
		if !ok || inv.Status != InvocationWaiting {
			return resultWith(l.snapshot(), StatusFailed), fmt.Errorf("invocation %q is not waiting", input.InvocationID)
		}
		value, err := r.observedDecode(ctx, obs, l.header.Revision, inv, input.Payload)
		if err != nil {
			return resultWith(l.snapshot(), StatusWaiting), fmt.Errorf("decode input for %q: %w", inv.ID, err)
		}
		decodedInputs = append(decodedInputs, decoded{inv.ID, value})
	}
	for _, input := range decodedInputs {
		if l.header.Revision == math.MaxUint64 {
			return resultWith(l.snapshot(), StatusFailed), ErrExecutionLimit
		}
		inv, ok := l.invocation(input.id)
		if !ok || inv.Status != InvocationWaiting {
			return resultWith(l.snapshot(), StatusFailed), fmt.Errorf("invocation %q is no longer waiting", input.id)
		}
		state, err := r.cloneState(inv.State, inv.ID)
		if err != nil {
			return resultWith(l.snapshot(), StatusFailed), fmt.Errorf("clone waiting state: %w", err)
		}
		call := CallInfo{RunID: l.header.RunID, InvocationID: inv.ID, CallID: inv.CallID, Node: inv.Node, Step: l.header.Steps, BranchIndex: inv.BranchIndex}
		updated, err := r.limitedApply(ctx, opts.Limiter, obs, l.header.Revision, call, inv, state, input.value)
		if cancelled := ctx.Err(); cancelled != nil {
			return resultWith(l.snapshot(), StatusCancelled), cancelled
		}
		if isCallbackInterruption(err) {
			return interruptedResult(ctx, l.snapshot(), err)
		}
		out := workResult[S]{ref: taskRef{id: inv.ID}, transition: model.To(updated, inv.Next...), err: err}
		c, terminalErr, buildErr := r.resultChange(l, out)
		if buildErr != nil {
			return resultWith(l.snapshot(), errorStatus(buildErr)), buildErr
		}
		c.Header.Steps = l.header.Steps
		if err := r.submit(ctx, l, c, opts.Store, obs); err != nil {
			return resultWith(l.snapshot(), errorStatus(err)), err
		}
		if terminalErr != nil {
			return resultWith(l.snapshot(), StatusFailed), terminalErr
		}
	}
	if l.header.Completed {
		return resultWith(l.snapshot(), statusOf(l.header, false)), nil
	}
	return r.driveLive(ctx, l, opts, obs)
}
