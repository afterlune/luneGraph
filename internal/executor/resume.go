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
// Resume opens a new live execution from checkpoint. When store is non-nil, it
// loads authoritative state and rejects a stale checkpoint revision.
func (r *Runner[S]) Resume(ctx context.Context, checkpoint Checkpoint[S], inputs []ResumeInput, store Store[S], opts Options) (execution *Execution[S], result Result[S], retErr error) {
	if ctx == nil {
		return nil, Result[S]{Status: StatusFailed}, errors.New("context must not be nil")
	}
	if r == nil {
		return nil, Result[S]{Status: StatusFailed}, errors.New("runner is nil")
	}
	obs := observation.New(opts.Observer, r.id, checkpoint.RunID)
	if obs != nil {
		span := obs.Begin(ctx, model.Event{Operation: model.OperationResume, Revision: checkpoint.Revision})
		defer func() {
			if result.Status != "" {
				obs.DiscardPending(ctx, retErr)
				span.End(ctx, model.Event{Revision: result.Header.Revision, Status: result.Status, Err: retErr})
			}
		}()
	}
	var err error
	if opts, err = normalizeOptions(opts); err != nil {
		return nil, Result[S]{Status: StatusFailed}, err
	}
	if err = ctx.Err(); err != nil {
		result, resultErr := r.resultFromCheckpoint(checkpoint, StatusCancelled)
		return nil, result, errors.Join(err, resultErr)
	}
	activeStore := observation.WrapStore(store, obs)
	if activeStore != nil {
		if !validName(checkpoint.RunID) || checkpoint.MachineID != r.id || checkpoint.Revision == 0 || checkpoint.FormatVersion != CheckpointFormatVersion {
			result, resultErr := r.resultFromCheckpoint(checkpoint, StatusFailed)
			return nil, result, errors.Join(invalidCheckpoint("invalid resume reference"), resultErr)
		}
		latest, loadErr := activeStore.Load(ctx, checkpoint.RunID)
		if loadErr != nil {
			result, resultErr := r.resultFromCheckpoint(checkpoint, errorStatus(loadErr))
			return nil, result, errors.Join(loadErr, resultErr)
		}
		if latest.MachineID != checkpoint.MachineID || latest.Revision != checkpoint.Revision || latest.RunID != checkpoint.RunID {
			result, resultErr := r.resultFromCheckpoint(checkpoint, StatusFailed)
			return nil, result, errors.Join(ErrConflict, resultErr)
		}
		checkpoint = latest
	}
	if err := r.validateCheckpoint(checkpoint); err != nil {
		result, resultErr := r.resultFromCheckpoint(checkpoint, StatusFailed)
		return nil, result, errors.Join(err, resultErr)
	}
	if checkpoint.Completed {
		result, resultErr := r.resultFromCheckpoint(checkpoint, StatusFailed)
		return nil, result, errors.Join(ErrRunCompleted, resultErr)
	}
	owned, err := checkpoint.Clone(r.clone)
	if err != nil {
		result, resultErr := r.resultFromCheckpoint(checkpoint, StatusFailed)
		return nil, result, errors.Join(fmt.Errorf("clone resume checkpoint: %w", err), resultErr)
	}
	l := newLive(owned)
	execution = &Execution[S]{runner: r, live: l, store: store}
	result, retErr = r.advanceLive(ctx, l, inputs, opts, activeStore, obs)
	return execution, result, retErr
}

// resumeValidated advances a loaded active checkpoint without reading the Store again.
func (r *Runner[S]) advanceLive(ctx context.Context, l *live[S], inputs []ResumeInput, opts Options, store Store[S], obs *observation.Session) (Result[S], error) {
	l.failureOverride = opts.FailureOverride
	if len(inputs) > 0 && l.header.Revision == math.MaxUint64 {
		return r.resultFromLiveOrEmpty(l, StatusFailed, ErrExecutionLimit)
	}
	type decoded struct {
		id    string
		value any
	}
	decodedInputs := make([]decoded, 0, len(inputs))
	seen := make(map[string]bool)
	for _, input := range inputs {
		if seen[input.InvocationID] {
			return r.resultFromLiveOrEmpty(l, StatusFailed, fmt.Errorf("%w: duplicate input for %q", ErrInvalidResumeInput, input.InvocationID))
		}
		seen[input.InvocationID] = true
		inv, ok := l.invocation(input.InvocationID)
		if !ok || inv.Status != InvocationWaiting {
			return r.resultFromLiveOrEmpty(l, StatusFailed, fmt.Errorf("%w: invocation %q is not waiting", ErrInvalidResumeInput, input.InvocationID))
		}
		if input.CallID == "" || input.CallID != inv.CallID {
			return r.resultFromLiveOrEmpty(l, StatusFailed, fmt.Errorf("%w: waiting call does not match invocation %q", ErrInvalidResumeInput, input.InvocationID))
		}
		value, err := r.observedDecode(ctx, obs, l.header.Revision, inv, input.Payload)
		if err != nil {
			return r.resultFromLiveOrEmpty(l, StatusWaiting, fmt.Errorf("decode input for %q: %w", inv.ID, err))
		}
		decodedInputs = append(decodedInputs, decoded{inv.ID, value})
	}
	for _, input := range decodedInputs {
		if l.header.Revision == math.MaxUint64 {
			return r.resultFromLiveOrEmpty(l, StatusFailed, ErrExecutionLimit)
		}
		inv, ok := l.invocation(input.id)
		if !ok || inv.Status != InvocationWaiting {
			return r.resultFromLiveOrEmpty(l, StatusFailed, fmt.Errorf("%w: invocation %q is no longer waiting", ErrInvalidResumeInput, input.id))
		}
		state, err := r.cloneState(inv.State, inv.ID)
		if err != nil {
			return r.resultFromLiveOrEmpty(l, StatusFailed, fmt.Errorf("clone waiting state: %w", err))
		}
		call := CallInfo{RunID: l.header.RunID, InvocationID: inv.ID, CallID: inv.CallID, Node: inv.Node, Step: l.header.Steps, BranchIndex: inv.BranchIndex}
		updated, err := r.limitedApply(ctx, opts.Limiter, obs, l.header.Revision, call, inv, state, input.value)
		if cancelled := ctx.Err(); cancelled != nil {
			return r.resultFromLiveOrEmpty(l, StatusCancelled, cancelled)
		}
		if isCallbackInterruption(err) {
			status := StatusInterrupted
			if ctx.Err() != nil {
				status = StatusCancelled
				err = ctx.Err()
			}
			return r.resultFromLiveOrEmpty(l, status, err)
		}
		out := workResult[S]{ref: taskRef{id: inv.ID}, transition: model.To(updated, inv.Next...), err: err}
		c, terminalErr, buildErr := r.resultChange(l, out)
		if buildErr != nil {
			return r.resultFromLiveOrEmpty(l, errorStatus(buildErr), buildErr)
		}
		c.Header.Steps = l.header.Steps
		if err := r.submit(ctx, l, c, store, obs); err != nil {
			return r.resultFromLiveOrEmpty(l, errorStatus(err), err)
		}
		if terminalErr != nil {
			return r.resultFromLiveOrEmpty(l, StatusFailed, terminalErr)
		}
	}
	if l.header.Completed {
		return r.resultFromLiveOrEmpty(l, statusOf(l.header.Header(), l.readyNodes, false), nil)
	}
	return r.driveLive(ctx, l, opts, obs, store)
}
