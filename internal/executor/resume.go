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
		return resultWith(checkpoint, StatusFailed), invalidCheckpoint("execution is already completed")
	}
	return r.resumeValidated(ctx, checkpoint, inputs, opts, obs)
}

// resumeValidated advances a loaded active checkpoint without reading the Store again.
func (r *Runner[S]) resumeValidated(ctx context.Context, checkpoint Checkpoint[S], inputs []ResumeInput, opts Options[S], obs *observation.Session) (Result[S], error) {
	s := copyCheckpoint(checkpoint)
	index := newInvocationIndex(s)
	var spare Checkpoint[S]
	if len(inputs) != 0 && s.Revision == math.MaxUint64 {
		return resultWith(s, StatusFailed), fmt.Errorf("apply resume input: %w", ErrExecutionLimit)
	}
	type decoded struct {
		id    string
		value any
	}
	decodedInputs := make([]decoded, 0, len(inputs))
	seen := make(map[string]bool, len(inputs))
	for _, input := range inputs {
		if seen[input.InvocationID] {
			return resultWith(s, StatusFailed), fmt.Errorf("duplicate resume input for %q", input.InvocationID)
		}
		seen[input.InvocationID] = true
		_, inv := indexedInvocation(&index, &s, input.InvocationID)
		if inv == nil || inv.Status != InvocationWaiting {
			return resultWith(s, StatusFailed), fmt.Errorf("invocation %q is not waiting", input.InvocationID)
		}
		value, decodeErr := r.observedDecode(ctx, obs, s.Revision, *inv, input.Payload)
		if decodeErr != nil {
			return resultWith(s, StatusWaiting), fmt.Errorf("decode input for %q: %w", input.InvocationID, decodeErr)
		}
		decodedInputs = append(decodedInputs, decoded{id: input.InvocationID, value: value})
	}
	for _, input := range decodedInputs {
		if s.Revision == math.MaxUint64 {
			return resultWith(s, StatusFailed), fmt.Errorf("apply resume input: %w", ErrExecutionLimit)
		}
		_, inv := indexedInvocation(&index, &s, input.id)
		if inv == nil || inv.Status != InvocationWaiting {
			return resultWith(s, StatusFailed), fmt.Errorf("invocation %q is no longer waiting", input.id)
		}
		state, cloneErr := r.cloneState(inv.State, inv.ID)
		if cloneErr != nil {
			return resultWith(s, StatusFailed), fmt.Errorf("clone waiting state: %w", cloneErr)
		}
		call := CallInfo{RunID: s.RunID, InvocationID: inv.ID, CallID: inv.CallID}
		updated, applyErr := r.observedApply(ctx, obs, s.Revision, call, *inv, state, input.value)
		candidate := copyCheckpointInto(&spare, s)
		if applyErr != nil {
			scope := r.scope(r.nodes[inv.Node].OnError, opts)
			if failureErr := r.recordFailure(&candidate, &index, inv.ID, inv.Node, scope, applyErr); failureErr != nil {
				if terminalFailureRecord(candidate) != nil {
					return r.commitTerminalFailure(ctx, s, candidate, failureErr, opts.Store)
				}
				return resultWith(s, StatusFailed), failureErr
			}
		} else {
			if routeErr := r.route(&candidate, &index, inv.ID, inv.Node, updated, inv.Next); routeErr != nil {
				return resultWith(s, StatusFailed), routeErr
			}
		}
		if settleErr := r.settleGroups(ctx, &candidate, &index, opts.FailureOverride, obs); settleErr != nil {
			if terminalFailureRecord(candidate) != nil {
				return r.commitTerminalFailure(ctx, s, candidate, settleErr, opts.Store)
			}
			return resultWith(s, StatusFailed), settleErr
		}
		markCompleted(&candidate)
		previous := s
		committed, commitErr := r.commit(ctx, s, candidate, opts.Store)
		if commitErr != nil {
			return resultWith(previous, errorStatus(commitErr)), commitErr
		}
		s = committed
		spare = previous
		clearCheckpointStateValues(&spare)
	}
	if s.Completed {
		return resultWith(s, statusOf(s, false)), nil
	}
	return r.drive(ctx, s, opts, obs)
}
