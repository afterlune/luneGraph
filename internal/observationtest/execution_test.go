package observationtest

import (
	"context"
	"errors"
	"reflect"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestLoopBudgetAndResumeEvents(t *testing.T) {
	g := graph.New[int]("step")
	node(t, g, "step", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		if s == 2 {
			return graph.EndExecution(s + 1), nil
		}
		return graph.To(s+1, "step"), nil
	})
	edge(t, g, "step", "step")
	r := compile(t, g)
	log := &recorder{}
	first, err := r.Start(context.Background(), "loop", 0, graph.Options[int]{MaxSteps: 1, Observer: log})
	if err != nil || first.Status != graph.StatusBudget {
		t.Fatalf("start = %+v, %v", first, err)
	}
	initial := log.snapshot()
	checkCall(t, initial, graph.OperationStart, first, err)
	nodes := finished(initial, graph.OperationNode)
	if len(nodes) != 1 || nodes[0].CallID != "c2" || nodes[0].Revision != 1 || nodes[0].Action != graph.ActionContinue {
		t.Fatalf("node = %+v", nodes)
	}
	second, err := r.Resume(context.Background(), first.Checkpoint, nil, graph.Options[int]{Observer: log})
	if err != nil || second.Status != graph.StatusCompleted || *second.Checkpoint.Final != 3 {
		t.Fatalf("resume = %+v, %v", second, err)
	}
	remaining := log.snapshot()[len(initial):]
	checkCall(t, remaining, graph.OperationResume, second, err)
	if initial[0].OperationID == remaining[0].OperationID {
		t.Fatal("operation ID reused")
	}
	seen := map[string]bool{}
	for _, e := range finished(log.snapshot(), graph.OperationNode) {
		if seen[e.CallID] || e.InvocationID != "i1" {
			t.Fatalf("node identity = %+v", e)
		}
		seen[e.CallID] = true
	}
	if len(seen) != 3 || len(finished(log.snapshot(), graph.OperationCompareAndSwap)) != 0 {
		t.Fatal("wrong node or store count")
	}
}

func TestObserverPanicsDoNotChangeExecution(t *testing.T) {
	r := singleRunner(t)
	baseline, baseErr := r.Start(context.Background(), "same", 4, graph.Options[int]{Store: memoryStore(t)})
	log := &recorder{}
	opts := graph.Options[int]{Store: memoryStore(t), Observer: graph.ObserverFunc(func(ctx context.Context, e graph.Event) {
		log.Observe(ctx, e)
		panic("observer failure")
	})}
	actual, err := r.Start(context.Background(), "same", 4, opts)
	if err != baseErr || !reflect.DeepEqual(actual, baseline) {
		t.Fatalf("changed outcome: %+v, %v", actual, err)
	}
	checkCall(t, log.snapshot(), graph.OperationStart, actual, err)
	if len(log.snapshot()) != 9 {
		t.Fatalf("deliveries stopped after panic: %+v", log.snapshot())
	}
}

func TestFailureAndValidationEvents(t *testing.T) {
	for _, name := range []string{"panic", "invalid transition", "invalid options", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			g := graph.New[int]("work")
			spec := graph.NodeSpec[int]{Name: "work", OnError: graph.FailExecution, Run: func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
				if name == "panic" {
					panic("callback failure")
				}
				if name == "invalid transition" {
					return graph.To(s, "missing"), nil
				}
				return graph.EndExecution(s), nil
			}}
			if err := g.AddNode(spec); err != nil {
				t.Fatal(err)
			}
			log := &recorder{}
			opts := graph.Options[int]{Observer: log, Store: memoryStore(t)}
			ctx := context.Background()
			if name == "invalid options" {
				opts.MaxSteps = -1
			}
			if name == "cancelled" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			result, err := compile(t, g).Start(ctx, "failure", 0, opts)
			if err == nil {
				t.Fatal("missing error")
			}
			checkCall(t, log.snapshot(), graph.OperationStart, result, err)
			nodes := finished(log.snapshot(), graph.OperationNode)
			switch name {
			case "panic":
				var panicErr *graph.PanicError
				if len(nodes) != 1 || !errors.As(nodes[0].Err, &panicErr) || !result.Checkpoint.Completed {
					t.Fatalf("panic outcome: %+v", result)
				}
			case "invalid transition":
				var transitionErr *graph.TransitionError
				if !errors.As(err, &transitionErr) || len(nodes) != 1 || nodes[0].Err != nil || result.Checkpoint.Completed || result.Checkpoint.Revision != 1 {
					t.Fatalf("transition outcome: %+v", result)
				}
			default:
				if len(log.snapshot()) != 2 {
					t.Fatal("callbacks/store ran before validation")
				}
			}
		})
	}
}

func TestCloneFailureAndInvalidCheckpointEvents(t *testing.T) {
	g := graph.New[int]("work")
	node(t, g, "work", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.EndExecution(s), nil
	})
	boom := errors.New("clone failure")
	r, err := g.Compile(graph.Config[int]{MachineID: "observation-v1", Clone: func(int) (int, error) { return 0, boom }})
	if err != nil {
		t.Fatal(err)
	}
	log := &recorder{}
	result, err := r.Start(context.Background(), "clone", 0, graph.Options[int]{Observer: log})
	if !errors.Is(err, boom) || len(log.snapshot()) != 2 {
		t.Fatalf("clone result: %+v, %v", result, err)
	}
	checkCall(t, log.snapshot(), graph.OperationStart, result, err)
	log = &recorder{}
	result, err = singleRunner(t).Resume(context.Background(), graph.Checkpoint[int]{RunID: "invalid"}, nil, graph.Options[int]{Observer: log})
	if !errors.Is(err, graph.ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
	checkCall(t, log.snapshot(), graph.OperationResume, result, err)
}
