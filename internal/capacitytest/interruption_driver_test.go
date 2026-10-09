package capacitytest

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	graph "github.com/afterlune/luneGraph"
)

type interruptionSlot struct {
	id         string
	checkpoint graph.Checkpoint[state]
}

func (s *interruptionSlot) remember(cp graph.Checkpoint[state], persisted bool) {
	if !persisted {
		s.checkpoint = cp
		return
	}
	// Persisted slots keep counters and input addresses, not application state.
	reference := graph.Checkpoint[state]{RunID: cp.RunID, Revision: cp.Revision, Steps: cp.Steps}
	for _, inv := range cp.Invocations {
		if inv.Status == graph.InvocationWaiting {
			reference.Invocations = append(reference.Invocations, graph.Invocation[state]{ID: inv.ID, Status: inv.Status})
		}
	}
	s.checkpoint = reference
}

func interruptionInputs(cp graph.Checkpoint[state]) []graph.ResumeInput {
	var inputs []graph.ResumeInput
	for _, inv := range cp.Invocations {
		if inv.Status == graph.InvocationWaiting {
			inputs = append(inputs, graph.ResumeInput{InvocationID: inv.ID, Payload: []byte("1")})
		}
	}
	return inputs
}

func checkInterruptionCheckpoint(cp graph.Checkpoint[state], width int) error {
	if cp.Completed || cp.Failure != nil || cp.HadLocalFailures || len(cp.Invocations) > width+1 || len(cp.Groups) > 1 {
		return fmt.Errorf("invalid active checkpoint: %+v", cp)
	}
	for _, inv := range cp.Invocations {
		if inv.State.Owner != cp.RunID {
			return fmt.Errorf("mixed owner: %+v", inv)
		}
		if _, ok := inv.State.Values["uncommitted"]; ok {
			return errors.New("interrupted map mutation committed")
		}
	}
	return nil
}

func interruptionCall(ctx context.Context, r *graph.Runner[state], store graph.Store[state], c *interruptionController, s *interruptionSlot, width, concurrency int) (graph.Result[state], error) {
	before := s.checkpoint
	opts := graph.Options[state]{Store: store, MaxConcurrency: concurrency, MaxSteps: width + 2}
	var out graph.Result[state]
	var err error
	if store == nil {
		out, err = r.Resume(ctx, before, interruptionInputs(before), opts)
	} else {
		out, err = r.Recover(ctx, s.id, interruptionInputs(before), opts)
	}
	if !c.quiescent(s.id) {
		return out, fmt.Errorf("callbacks outlived public call for %s", s.id)
	}
	if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return out, err
	}
	if err != nil && !(out.Status == graph.StatusInterrupted && errors.Is(err, graph.ErrInterrupted)) {
		return out, err
	}
	if out.Status != graph.StatusInterrupted && out.Status != graph.StatusWaiting && out.Status != graph.StatusBudget {
		return out, fmt.Errorf("unexpected status %s", out.Status)
	}
	if out.Checkpoint.Revision < before.Revision || out.Checkpoint.Steps < before.Steps {
		return out, errors.New("checkpoint counters regressed")
	}
	if err := checkInterruptionCheckpoint(out.Checkpoint, width); err != nil {
		return out, err
	}
	if count := c.reconcile(out.Checkpoint); count > width+1 {
		return out, fmt.Errorf("controller retained %d pending callbacks", count)
	}
	if out.Status == graph.StatusWaiting {
		v, stateErr := rootState(out.Checkpoint)
		if stateErr != nil {
			return out, stateErr
		}
		if v.Total != v.Round*width || v.Values["sum"] != v.Round*width*(width+1)/2 || v.Values["inputs"] != v.Round-1 || out.Checkpoint.Steps != uint64(v.Round*(width+2)) || out.Checkpoint.Revision != out.Checkpoint.Steps+uint64(v.Round) {
			return out, fmt.Errorf("invalid round boundary: %+v", out.Checkpoint)
		}
	}
	// CAS acknowledgements are certain in this fixture; persisted results must
	// agree exactly with the returned last committed checkpoint.
	if store != nil {
		saved, loadErr := store.Load(context.Background(), s.id)
		if loadErr != nil {
			return out, loadErr
		}
		if !reflect.DeepEqual(saved, out.Checkpoint) {
			return out, errors.New("returned and stored checkpoints differ")
		}
	}
	s.remember(out.Checkpoint, store != nil)
	return out, err
}

func advanceInterruptedRound(ctx context.Context, r *graph.Runner[state], store graph.Store[state], c *interruptionController, s *interruptionSlot, width, concurrency int) error {
	// One first attempt per pending callback, plus commits and parallel replay.
	// This fuse catches lost identities or a controller that cannot make progress.
	for range 4*width + 16 {
		out, err := interruptionCall(ctx, r, store, c, s, width, concurrency)
		if err != nil && !errors.Is(err, graph.ErrInterrupted) {
			return err
		}
		if out.Status == graph.StatusWaiting {
			return nil
		}
	}
	return errors.New("interruption recovery did not reach next round")
}
