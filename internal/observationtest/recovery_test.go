package observationtest

import (
	"context"
	"errors"
	"strconv"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
)

func mountedRunner(t *testing.T) *graph.Runner[int] {
	child := graph.New[int]("wait")
	node(t, child, "wait", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.Wait(s, "value", "return"), nil
	})
	node(t, child, "return", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.Return(s), nil
	})
	edge(t, child, "wait", "return")
	if err := graph.RegisterContinuation(child, "value", func(p []byte) (int, error) { return strconv.Atoi(string(p)) }, func(_ context.Context, _ graph.CallInfo, s, input int) (int, error) { return s + input, nil }); err != nil {
		t.Fatal(err)
	}
	parent := graph.New[int]("worker")
	if err := parent.AddSubgraph("worker", child); err != nil {
		t.Fatal(err)
	}
	node(t, parent, "done", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.EndExecution(s), nil
	})
	edge(t, parent, "worker", "done")
	return compile(t, parent)
}

func TestMountedContinuationAndTerminalRecoveryEvents(t *testing.T) {
	r, store, log := mountedRunner(t), memoryStore(t), &recorder{}
	opts := graph.Options{Observer: log}
	optsStore := store
	firstExecution, first, err := r.Start(context.Background(), "mounted", 0, optsStore, opts)
	if err != nil || first.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", first, err)
	}
	initial := log.snapshot()
	checkCall(t, initial, graph.OperationStart, first, err)
	waitNode := finished(initial, graph.OperationNode)
	if len(waitNode) != 1 || waitNode[0].Node != "worker/wait" || waitNode[0].Action != graph.ActionWait {
		t.Fatalf("wait node = %+v", waitNode)
	}
	inputs := []graph.ResumeInput{{InvocationID: mustCheckpoint(firstExecution).Invocations[0].ID, CallID: mustCheckpoint(firstExecution).Invocations[0].CallID, Payload: []byte("3")}}
	_, result, err := r.Recover(context.Background(), "mounted", inputs, optsStore, opts)
	if err != nil || result.Status != graph.StatusCompleted || *result.Final != 3 {
		t.Fatalf("recover = %+v, %v", result, err)
	}
	events := log.snapshot()[len(initial):]
	checkCall(t, events, graph.OperationRecover, result, err)
	decodes, applies := finished(events, graph.OperationDecode), finished(events, graph.OperationApply)
	if len(decodes) != 1 || decodes[0].Continuation != "worker/value" || decodes[0].CallID != "" {
		t.Fatalf("decode = %+v", decodes)
	}
	if len(applies) != 1 || applies[0].Continuation != "worker/value" || applies[0].CallID != mustCheckpoint(firstExecution).Invocations[0].CallID {
		t.Fatalf("apply = %+v", applies)
	}
	loads := finished(events, graph.OperationLoad)
	if len(loads) != 1 || loads[0].Revision != first.Header.Revision || len(finished(events, graph.OperationResume)) != 0 {
		t.Fatalf("nested recovery/load = %+v", events)
	}
	before := len(log.snapshot())
	_, result, err = r.Recover(context.Background(), "mounted", nil, optsStore, opts)
	events = log.snapshot()[before:]
	checkCall(t, events, graph.OperationRecover, result, err)
	if len(events) != 4 || len(finished(events, graph.OperationNode)) != 0 {
		t.Fatalf("terminal recovery executed work: %+v", events)
	}
}

type faultStore struct {
	graph.Store[int]
	fault    error
	write    bool
	failed   bool
	revision uint64
}

func (s *faultStore) Commit(ctx context.Context, expected uint64, change graph.Change[int]) error {
	next := change.Checkpoint()
	if !s.failed && (s.revision == 0 || next.Revision == s.revision) {
		s.failed = true
		if s.write {
			if err := s.Store.Commit(ctx, expected, change); err != nil {
				return err
			}
		}
		return s.fault
	}
	return s.Store.Commit(ctx, expected, change)
}

func TestCommitErrorAndNodeReplayEvents(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(strconv.FormatBool(acknowledged), func(t *testing.T) {
			fault := graph.ErrConflict
			if acknowledged {
				fault = errors.New("lost acknowledgement")
			}
			store := &faultStore{Store: memoryStore(t), fault: fault, write: acknowledged}
			r, log := singleRunner(t), &recorder{}
			opts := graph.Options{Observer: log}
			optsStore := store
			_, first, err := r.Start(context.Background(), "commit", 0, optsStore, opts)
			if !errors.Is(err, fault) || first.Header.Revision != 1 {
				t.Fatalf("start: %+v, %v", first, err)
			}
			initial := log.snapshot()
			checkCall(t, initial, graph.OperationStart, first, err)
			writes := finished(initial, graph.OperationCommit)
			if len(writes) != 1 || writes[0].Err != fault || writes[0].Revision != 2 {
				t.Fatalf("write event: %+v", writes)
			}
			firstNode := finished(initial, graph.OperationNode)[0]
			if firstNode.Err != nil {
				t.Fatal("callback failed instead of Store")
			}
			_, result, err := r.Recover(context.Background(), "commit", nil, optsStore, opts)
			if err != nil || result.Status != graph.StatusCompleted {
				t.Fatalf("recover: %+v, %v", result, err)
			}
			events := log.snapshot()[len(initial):]
			checkCall(t, events, graph.OperationRecover, result, err)
			nodes := finished(events, graph.OperationNode)
			if acknowledged {
				if len(nodes) != 0 || finished(events, graph.OperationLoad)[0].Revision != 2 {
					t.Fatal("committed callback replayed")
				}
			} else if len(nodes) != 1 || nodes[0].CallID != firstNode.CallID || nodes[0].OperationID == firstNode.OperationID {
				t.Fatalf("replay identity: %+v", nodes)
			}
		})
	}
}

func TestDecodeErrorAndMissingLoadEvents(t *testing.T) {
	r, log, store := mountedRunner(t), &recorder{}, memoryStore(t)
	firstExecution, first, err := r.Start(context.Background(), "decode", 0, store, graph.Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, result, err := r.Resume(context.Background(), mustCheckpoint(firstExecution), []graph.ResumeInput{{InvocationID: mustCheckpoint(firstExecution).Invocations[0].ID, CallID: mustCheckpoint(firstExecution).Invocations[0].CallID, Payload: []byte("invalid")}}, store, graph.Options{Observer: log})
	if err == nil || result.Status != graph.StatusWaiting || result.Header.Revision != first.Header.Revision {
		t.Fatalf("decode outcome = %+v, %v", result, err)
	}
	checkCall(t, log.snapshot(), graph.OperationResume, result, err)
	if decodes := finished(log.snapshot(), graph.OperationDecode); len(decodes) != 1 || decodes[0].Err == nil || len(finished(log.snapshot(), graph.OperationApply)) != 0 || len(finished(log.snapshot(), graph.OperationCommit)) != 0 {
		t.Fatal("decode failure applied or committed input")
	}
	log = &recorder{}
	_, result, err = r.Recover(context.Background(), "missing", nil, store, graph.Options{Observer: log})
	if !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatal(err)
	}
	checkCall(t, log.snapshot(), graph.OperationRecover, result, err)
	loads := finished(log.snapshot(), graph.OperationLoad)
	if len(loads) != 1 || loads[0].Err != err || loads[0].Revision != 0 {
		t.Fatalf("load event: %+v", loads)
	}
}
