package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/memory"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

type teamTask struct {
	Target string
	ID     string
	Model  string
	Depth  int
}

type teamState struct {
	Plan       []teamTask
	TaskID     string
	Model      string
	Depth      int
	MaxDepth   int
	Trace      []string
	Approved   bool
	MergeCalls int
}

func cloneTeamState(s teamState) (teamState, error) {
	s.Plan = append([]teamTask(nil), s.Plan...)
	s.Trace = append([]string(nil), s.Trace...)
	return s, nil
}

func teamRunner(t *testing.T) *graph.Runner[teamState] {
	t.Helper()
	g := graph.New[teamState]("lead")
	node(t, g, "lead", func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		s.Model = "provider-lead"
		branches := make([]graph.Branch[teamState], 0, len(s.Plan))
		for _, task := range s.Plan {
			child := s
			child.TaskID, child.Model, child.Depth = task.ID, task.Model, task.Depth
			child.Trace = nil
			branches = append(branches, graph.Branch[teamState]{Target: task.Target, State: child})
		}
		return graph.FanOut(s, branches...), nil
	})
	node(t, g, "worker-a", func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		if s.Model != "provider-a" {
			return graph.Transition[teamState]{}, errors.New("worker-a received the wrong model")
		}
		s.Trace = []string{s.TaskID + "@" + s.Model}
		return graph.CompleteBranch(s), nil
	})
	node(t, g, "worker-b", func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		if s.Model != "provider-b" {
			return graph.Transition[teamState]{}, errors.New("worker-b received the wrong model")
		}
		if s.Depth < s.MaxDepth {
			left, right := s, s
			left.TaskID, left.Model, left.Depth, left.Trace = s.TaskID+"/a", "provider-a", s.Depth+1, nil
			right.TaskID, right.Model, right.Depth, right.Trace = s.TaskID+"/b", "provider-b", s.Depth+1, nil
			return graph.FanOut(s,
				graph.Branch[teamState]{Target: "worker-a", State: left},
				graph.Branch[teamState]{Target: "worker-b", State: right}), nil
		}
		s.Trace = []string{s.TaskID + "@" + s.Model}
		return graph.CompleteBranch(s), nil
	})
	if err := g.AddJoin(graph.JoinSpec[teamState]{Name: "worker-join", From: "worker-b", OnError: graph.FailExecution,
		Merge: func(_ context.Context, _ graph.CallInfo, parent teamState, branches []graph.BranchResult[teamState]) (teamState, error) {
			parent.Trace = nil
			parent.MergeCalls++
			for _, result := range branches {
				if result.OK() {
					parent.Trace = append(parent.Trace, result.State.Trace...)
					parent.MergeCalls += result.State.MergeCalls
				}
			}
			return parent, nil
		}}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "worker-return", func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		return graph.CompleteBranch(s), nil
	})
	if err := g.AddJoin(graph.JoinSpec[teamState]{Name: "lead-join", From: "lead", OnError: graph.FailExecution,
		Merge: func(_ context.Context, _ graph.CallInfo, parent teamState, branches []graph.BranchResult[teamState]) (teamState, error) {
			parent.Trace = nil
			parent.MergeCalls++
			for _, result := range branches {
				if result.OK() {
					parent.Trace = append(parent.Trace, result.State.Trace...)
					parent.MergeCalls += result.State.MergeCalls
				}
			}
			return parent, nil
		}}); err != nil {
		t.Fatal(err)
	}
	node(t, g, "approval", func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		return graph.Wait(s, "approval-input", "finish"), nil
	})
	node(t, g, "finish", func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		return graph.EndExecution(s), nil
	})
	for _, edgePair := range [][2]string{
		{"lead", "worker-a"}, {"lead", "worker-b"}, {"lead-join", "approval"},
		{"worker-b", "worker-a"}, {"worker-b", "worker-b"}, {"worker-join", "worker-return"},
		{"approval", "finish"},
	} {
		edge(t, g, edgePair[0], edgePair[1])
	}
	if err := graph.RegisterJSONContinuation(g, "approval-input", func(_ context.Context, _ graph.CallInfo, s teamState, approved bool) (teamState, error) {
		s.Approved = approved
		return s, nil
	}); err != nil {
		t.Fatal(err)
	}
	r, err := g.Compile(graph.Config[teamState]{MachineID: "dynamic-team-v1", Clone: cloneTeamState})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func teamInitial(maxDepth int, plan ...teamTask) teamState {
	return teamState{Plan: append([]teamTask(nil), plan...), MaxDepth: maxDepth}
}

func TestDynamicTeamFanOutAndRecursiveWorker(t *testing.T) {
	tests := []struct {
		name  string
		state teamState
		want  []string
	}{
		{name: "empty", state: teamInitial(0), want: []string{}},
		{name: "one", state: teamInitial(0, teamTask{Target: "worker-a", ID: "solo", Model: "provider-a"}), want: []string{"solo@provider-a"}},
		{name: "repeated mixed targets", state: teamInitial(0,
			teamTask{Target: "worker-a", ID: "a1", Model: "provider-a"},
			teamTask{Target: "worker-a", ID: "a2", Model: "provider-a"},
			teamTask{Target: "worker-b", ID: "b1", Model: "provider-b"}),
			want: []string{"a1@provider-a", "a2@provider-a", "b1@provider-b"}},
		{name: "recursive mixed workers", state: teamInitial(2, teamTask{Target: "worker-b", ID: "tree", Model: "provider-b"}),
			want: []string{"tree/a@provider-a", "tree/b/a@provider-a", "tree/b/b@provider-b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := teamRunner(t)
			execution, waiting, err := r.Start(context.Background(), "team-"+test.name, test.state, nil, graph.Options{MaxConcurrency: 4})
			if err != nil || waiting.Status != graph.StatusWaiting {
				t.Fatalf("start = %+v, %v", waiting, err)
			}
			if got := execution.Pending(); len(got) != 1 || got[0].Continuation != "approval-input" {
				t.Fatalf("pending = %+v", got)
			}
			input := execution.Pending()[0]
			completed, err := execution.Advance(context.Background(), []graph.ResumeInput{{
				InvocationID: input.InvocationID, CallID: input.CallID, Payload: []byte("true"),
			}}, graph.Options{MaxConcurrency: 4})
			if err != nil || completed.Status != graph.StatusCompleted || completed.Final == nil || !completed.Final.Approved || len(completed.Final.Trace) != len(test.want) || (len(test.want) != 0 && !reflect.DeepEqual(completed.Final.Trace, test.want)) {
				t.Fatalf("completed = %+v, %v; trace=%v want=%v", completed, err, completed.Final, test.want)
			}
			wantMerges := 1
			if test.name == "recursive mixed workers" {
				wantMerges = 3
			}
			if completed.Final.Model != "provider-lead" || len(completed.Final.Plan) != len(test.state.Plan) || completed.Final.MergeCalls != wantMerges {
				t.Fatalf("Lead parent state was not preserved through Merge: %+v", completed.Final)
			}
		})
	}
}

func TestDynamicTeamFanOutSQLiteRecovery(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "team.db")
	store, err := sqlite.Open(ctx, dbPath, checkpoint.JSON[teamState]{})
	if err != nil {
		t.Fatal(err)
	}
	r := teamRunner(t)
	plan := teamInitial(2, teamTask{Target: "worker-b", ID: "durable", Model: "provider-b"})
	execution, waiting, err := r.Start(ctx, "team-durable", plan, store, graph.Options{MaxConcurrency: 4})
	if err != nil || waiting.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", waiting, err)
	}
	original := execution.Pending()[0]
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(ctx, dbPath, checkpoint.JSON[teamState]{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	recovered, pending, err := r.Recover(ctx, "team-durable", nil, store, graph.Options{MaxConcurrency: 4})
	if err != nil || pending.Status != graph.StatusWaiting {
		t.Fatalf("recover = %+v, %v", pending, err)
	}
	current := recovered.Pending()[0]
	if current.InvocationID != original.InvocationID || current.CallID != original.CallID {
		t.Fatalf("waiting identity changed: %+v => %+v", original, current)
	}
	_, finished, err := r.Recover(ctx, "team-durable", []graph.ResumeInput{{InvocationID: current.InvocationID, CallID: current.CallID, Payload: []byte("true")}}, store, graph.Options{MaxConcurrency: 4})
	if err != nil || finished.Status != graph.StatusCompleted || finished.Final == nil || !finished.Final.Approved || !reflect.DeepEqual(finished.Final.Trace, []string{"durable/a@provider-a", "durable/b/a@provider-a", "durable/b/b@provider-b"}) {
		t.Fatalf("finished = %+v, %v", finished, err)
	}
}

func TestDynamicTeamFanOutMemoryStore(t *testing.T) {
	store, err := memory.New[teamState](cloneTeamState)
	if err != nil {
		t.Fatal(err)
	}
	r := teamRunner(t)
	e, result, err := r.Start(context.Background(), "team-memory", teamInitial(0), store, graph.Options{})
	if err != nil || result.Status != graph.StatusWaiting {
		t.Fatalf("start = %+v, %v", result, err)
	}
	pending := e.Pending()[0]
	_, result, err = r.Recover(context.Background(), "team-memory", []graph.ResumeInput{{InvocationID: pending.InvocationID, CallID: pending.CallID, Payload: []byte("true")}}, store, graph.Options{})
	if err != nil || result.Status != graph.StatusCompleted || result.Final == nil || !result.Final.Approved || len(result.Final.Trace) != 0 {
		t.Fatalf("recover = %+v, %v", result, err)
	}
}

func dynamicValidationRunner(t *testing.T, join bool, clone graph.Clone[int], targets []string) *graph.Runner[int] {
	t.Helper()
	g := graph.New[int]("dispatch")
	node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, _ int) (graph.Transition[int], error) {
		branches := make([]graph.Branch[int], len(targets))
		for i, target := range targets {
			branches[i] = graph.Branch[int]{Target: target, State: i + 1}
		}
		return graph.FanOut(0, branches...), nil
	})
	node(t, g, "worker", func(_ context.Context, _ graph.CallInfo, s int) (graph.Transition[int], error) {
		return graph.CompleteBranch(s), nil
	})
	if join {
		if err := g.AddJoin(graph.JoinSpec[int]{Name: "joined", From: "dispatch", OnError: graph.FailExecution, Merge: func(_ context.Context, _ graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
			return parent + len(branches), nil
		}}); err != nil {
			t.Fatal(err)
		}
		node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
			return graph.EndExecution(state), nil
		})
		if err := g.AddEdge("joined", "done"); err != nil {
			t.Fatal(err)
		}
	}
	edges := make(map[string]bool)
	for _, target := range targets {
		if target != "worker" && target != "joined" || edges[target] {
			continue
		}
		edges[target] = true
		if err := g.AddEdge("dispatch", target); err != nil {
			t.Fatal(err)
		}
	}
	r, err := g.Compile(graph.Config[int]{MachineID: "dynamic-validation-v1", Clone: clone})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestDynamicFanOutValidationAndAtomicCloneFailure(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		join    bool
		targets []string
		clone   graph.Clone[int]
	}{
		{name: "missing join", targets: []string{"worker"}},
		{name: "undeclared target", join: true, targets: []string{"worker", "unlinked"}},
		{name: "branch target is join", join: true, targets: []string{"joined"}},
		{name: "clone error after earlier branch", join: true, targets: []string{"worker", "worker"}, clone: func(v int) (int, error) {
			if v == 2 {
				return 0, errors.New("branch clone failed")
			}
			return v, nil
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			clone := scenario.clone
			if clone == nil {
				clone = graph.ValueClone[int]
			}
			r := dynamicValidationRunner(t, scenario.join, clone, scenario.targets)
			execution, result, err := r.Start(context.Background(), "dynamic-invalid", 0, nil, graph.Options{})
			if err == nil || result.Status != graph.StatusFailed || result.Header.Completed || result.Header.Failure != nil || result.Header.Revision != 1 || result.Header.Steps != 0 {
				t.Fatalf("invalid FanOut committed: result=%+v err=%v", result, err)
			}
			if execution == nil {
				t.Fatal("Start must return the unchanged live execution after a rejected callback")
			}
			position := mustCheckpoint(execution)
			if len(position.Invocations) != 1 || position.Invocations[0].Status != graph.InvocationReady || len(position.Groups) != 0 {
				t.Fatalf("rejected FanOut changed topology: %+v", position)
			}
		})
	}
}

func TestDynamicFanOutRejectsIDOverflowBeforeBranchCloning(t *testing.T) {
	var clones int
	r := dynamicValidationRunner(t, true, func(v int) (int, error) { clones++; return v, nil }, []string{"worker", "worker", "worker"})
	position := graph.Checkpoint[int]{
		FormatVersion: graph.CheckpointFormatVersion, RunID: "overflow", MachineID: "dynamic-validation-v1",
		Revision: 1, NextID: math.MaxUint64 - 7,
		Invocations: []graph.Invocation[int]{{ID: "i1", CallID: "c2", Node: "dispatch", Status: graph.InvocationReady}},
	}
	_, result, err := r.Resume(context.Background(), position, nil, nil, graph.Options{})
	if !errors.Is(err, graph.ErrExecutionLimit) || result.Header.Revision != 1 || result.Header.Failure != nil || clones != 2 {
		t.Fatalf("FanOut ID exhaustion = %+v, clones=%d, err=%v", result, clones, err)
	}
}

func TestDynamicRepeatedTargetsSurviveStoreReload(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			r := dynamicValidationRunner(t, true, graph.ValueClone[int], []string{"worker", "worker", "worker", "worker"})
			store := dynamicRetryStore(t, kind)
			e, budget, err := r.Start(context.Background(), "repeated-"+kind, 5, store, graph.Options{MaxSteps: 1, MaxConcurrency: 1})
			if err != nil || budget.Status != graph.StatusBudget {
				t.Fatalf("start = %+v, %v", budget, err)
			}
			position := mustCheckpoint(e)
			if len(position.Groups) != 1 || len(position.Groups[0].Children) != 4 {
				t.Fatalf("dynamic activation = %+v", position.Groups)
			}
			_, result, err := r.Recover(context.Background(), "repeated-"+kind, nil, store, graph.Options{MaxConcurrency: 2})
			if err != nil || result.Status != graph.StatusCompleted || result.Final == nil || *result.Final != 4 {
				var final int
				if result.Final != nil {
					final = *result.Final
				}
				t.Fatalf("recover = %+v final=%d, %v", result, final, err)
			}
		})
	}
}

func TestDynamicMergeParentCloneFailureKeepsCommittedChildren(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var failParentClone atomic.Bool
			var workerCalls, mergeCalls atomic.Int32
			var mergeIDsMu sync.Mutex
			var mergeIDs []string
			g := graph.New[int]("dispatch")
			node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, parent int) (graph.Transition[int], error) {
				return graph.FanOut(parent, graph.Branch[int]{Target: "worker", State: 10}), nil
			})
			node(t, g, "worker", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
				workerCalls.Add(1)
				return graph.CompleteBranch(state + 1), nil
			})
			if err := g.AddJoin(graph.JoinSpec[int]{Name: "joined", From: "dispatch", OnError: graph.FailExecution, Merge: func(_ context.Context, call graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
				mergeCalls.Add(1)
				mergeIDsMu.Lock()
				mergeIDs = append(mergeIDs, call.CallID)
				mergeIDsMu.Unlock()
				return parent + branches[0].State, nil
			}}); err != nil {
				t.Fatal(err)
			}
			node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
				return graph.EndExecution(state), nil
			})
			edge(t, g, "dispatch", "worker")
			edge(t, g, "joined", "done")
			r, err := g.Compile(graph.Config[int]{MachineID: "dynamic-parent-clone-v1", Clone: func(state int) (int, error) {
				if state == 5 && failParentClone.Load() {
					return 0, errors.New("parent clone failed")
				}
				return state, nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			store := dynamicRetryStore(t, kind)
			_, first, err := r.Start(context.Background(), "parent-clone-"+kind, 5, store, graph.Options{MaxSteps: 1, MaxConcurrency: 1})
			if err != nil || first.Status != graph.StatusBudget {
				t.Fatalf("start = %+v, %v", first, err)
			}
			failParentClone.Store(true)
			_, interrupted, err := r.Recover(context.Background(), "parent-clone-"+kind, nil, store, graph.Options{MaxSteps: 1, MaxConcurrency: 1})
			if err == nil || interrupted.Header.Failure != nil || interrupted.Header.Completed || workerCalls.Load() != 1 || mergeCalls.Load() != 0 {
				t.Fatalf("parent Clone failure = %+v, workers=%d merges=%d err=%v", interrupted, workerCalls.Load(), mergeCalls.Load(), err)
			}
			persisted, err := store.Load(context.Background(), "parent-clone-"+kind)
			if err != nil || len(persisted.Groups) != 1 || persisted.Invocations[1].Status != graph.InvocationJoined {
				t.Fatalf("committed child after Clone failure: %+v %v", persisted, err)
			}
			failParentClone.Store(false)
			_, result, err := r.Recover(context.Background(), persisted.RunID, nil, store, graph.Options{MaxConcurrency: 1})
			if err != nil || result.Status != graph.StatusCompleted || result.Final == nil || *result.Final != 16 || workerCalls.Load() != 1 || mergeCalls.Load() != 1 || len(mergeIDs) != 1 {
				t.Fatalf("parent Clone retry = %+v workers=%d merges=%d ids=%v err=%v", result, workerCalls.Load(), mergeCalls.Load(), mergeIDs, err)
			}
		})
	}
}

func TestConcurrentDynamicExecutionsShareRunnerAndStore(t *testing.T) {
	store, err := memory.New[teamState](cloneTeamState)
	if err != nil {
		t.Fatal(err)
	}
	r := teamRunner(t)
	const runs = 8
	type outcome struct {
		execution *graph.Execution[teamState]
		result    graph.Result[teamState]
		err       error
	}
	started := make([]outcome, runs)
	var starters sync.WaitGroup
	for i := range runs {
		starters.Add(1)
		go func(i int) {
			defer starters.Done()
			id := fmt.Sprintf("shared-dynamic-%d", i)
			plan := teamInitial(1, teamTask{Target: "worker-a", ID: id + "/a", Model: "provider-a"}, teamTask{Target: "worker-b", ID: id + "/b", Model: "provider-b"})
			started[i].execution, started[i].result, started[i].err = r.Start(context.Background(), id, plan, store, graph.Options{MaxConcurrency: 4})
		}(i)
	}
	starters.Wait()
	var advances sync.WaitGroup
	for i := range runs {
		if started[i].err != nil || started[i].result.Status != graph.StatusWaiting {
			t.Fatalf("start %d = %+v, %v", i, started[i].result, started[i].err)
		}
		waiting := started[i].execution.Pending()[0]
		advances.Add(1)
		go func(i int, pending graph.PendingInvocation) {
			defer advances.Done()
			result, err := started[i].execution.Advance(context.Background(), []graph.ResumeInput{{InvocationID: pending.InvocationID, CallID: pending.CallID, Payload: []byte("true")}}, graph.Options{MaxConcurrency: 4})
			if err != nil || result.Status != graph.StatusCompleted || result.Final == nil || !result.Final.Approved || len(result.Final.Trace) != 3 || result.Final.Trace[0] != fmt.Sprintf("shared-dynamic-%d/a@provider-a", i) || result.Final.Trace[2] != fmt.Sprintf("shared-dynamic-%d/b/b@provider-b", i) || result.Final.MergeCalls != 2 {
				t.Errorf("advance %d = %+v final=%+v, %v", i, result, result.Final, err)
			}
		}(i, waiting)
	}
	advances.Wait()
}

type rejectDynamicJoinStore struct {
	graph.Store[int]
	reject atomic.Bool
}

func (s *rejectDynamicJoinStore) Commit(ctx context.Context, expected uint64, change graph.Change[int]) error {
	if len(change.DeleteGroups) != 0 && s.reject.Swap(false) {
		return graph.ErrConflict
	}
	return s.Store.Commit(ctx, expected, change)
}

func dynamicRetryStore(t *testing.T, kind string) graph.Store[int] {
	t.Helper()
	if kind == "memory" {
		store, err := memory.New[int](graph.ValueClone[int])
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "retry.db"), checkpoint.JSON[int]{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func TestDynamicFanOutRejectedMergeReplaysOnlyMerge(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var workers atomic.Int32
			mergeCalls := make(chan graph.CallInfo, 2)
			g := graph.New[int]("dispatch")
			node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, parent int) (graph.Transition[int], error) {
				return graph.FanOut(parent, graph.Branch[int]{Target: "worker", State: 2}), nil
			})
			node(t, g, "worker", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
				workers.Add(1)
				return graph.CompleteBranch(state + 1), nil
			})
			if err := g.AddJoin(graph.JoinSpec[int]{Name: "joined", From: "dispatch", OnError: graph.FailExecution, Merge: func(_ context.Context, call graph.CallInfo, parent int, branches []graph.BranchResult[int]) (int, error) {
				mergeCalls <- call
				return parent + branches[0].State, nil
			}}); err != nil {
				t.Fatal(err)
			}
			node(t, g, "done", func(_ context.Context, _ graph.CallInfo, state int) (graph.Transition[int], error) {
				return graph.EndExecution(state), nil
			})
			edge(t, g, "dispatch", "worker")
			edge(t, g, "joined", "done")
			r, err := g.Compile(graph.Config[int]{MachineID: "dynamic-replay-v1", Clone: graph.ValueClone[int]})
			if err != nil {
				t.Fatal(err)
			}
			backing := dynamicRetryStore(t, kind)
			store := &rejectDynamicJoinStore{Store: backing}
			execution, budget, err := r.Start(context.Background(), "dynamic-replay", 5, store, graph.Options{MaxSteps: 1, MaxConcurrency: 1})
			if err != nil || budget.Status != graph.StatusBudget {
				t.Fatalf("start = %+v, %v", budget, err)
			}
			stored, err := backing.Load(context.Background(), "dynamic-replay")
			if err != nil || len(stored.Groups) != 1 || len(stored.Invocations) != 2 {
				t.Fatalf("FanOut was not committed atomically: %+v, %v", stored, err)
			}
			store.reject.Store(true)
			_, rejected, err := r.Recover(context.Background(), "dynamic-replay", nil, store, graph.Options{MaxConcurrency: 1})
			if !errors.Is(err, graph.ErrConflict) || rejected.Header.Failure != nil || workers.Load() != 1 {
				t.Fatalf("rejected Merge = %+v workers=%d err=%v", rejected, workers.Load(), err)
			}
			firstMerge := <-mergeCalls
			stored, err = backing.Load(context.Background(), "dynamic-replay")
			if err != nil || stored.Revision != rejected.Header.Revision || stored.Invocations[1].Status != graph.InvocationJoined {
				t.Fatalf("committed child lost after Merge conflict: %+v %v", stored, err)
			}
			_, completed, err := r.Recover(context.Background(), "dynamic-replay", nil, store, graph.Options{MaxConcurrency: 1})
			secondMerge := <-mergeCalls
			if err != nil || completed.Status != graph.StatusCompleted || completed.Final == nil || *completed.Final != 8 || firstMerge.CallID == "" || firstMerge.CallID != secondMerge.CallID || workers.Load() != 1 {
				t.Fatalf("Merge replay = %+v mergeIDs=%q/%q workers=%d err=%v", completed, firstMerge.CallID, secondMerge.CallID, workers.Load(), err)
			}
			if _, err := execution.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type tolerantTestState struct {
	Val      int
	Failures []string
	Branches []string
}

func cloneTolerantState(s tolerantTestState) (tolerantTestState, error) {
	return tolerantTestState{
		Val:      s.Val,
		Failures: append([]string(nil), s.Failures...),
		Branches: append([]string(nil), s.Branches...),
	}, nil
}

type rejectTolerantStore struct {
	graph.Store[tolerantTestState]
	reject atomic.Bool
}

func (s *rejectTolerantStore) Commit(ctx context.Context, expected uint64, change graph.Change[tolerantTestState]) error {
	if len(change.DeleteGroups) != 0 && s.reject.Swap(false) {
		return graph.ErrConflict
	}
	return s.Store.Commit(ctx, expected, change)
}

func tolerantStore(t *testing.T, kind string) graph.Store[tolerantTestState] {
	t.Helper()
	if kind == "memory" {
		store, err := memory.New[tolerantTestState](cloneTolerantState)
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "tolerant.db"), checkpoint.JSON[tolerantTestState]{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func TestDynamicFanOutTolerantJoinWithPartialFailures(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var mergeResults []graph.BranchResult[tolerantTestState]
			mergeCalls := make(chan graph.CallInfo, 4)

			g := graph.New[tolerantTestState]("dispatch")
			node(t, g, "dispatch", func(_ context.Context, _ graph.CallInfo, parent tolerantTestState) (graph.Transition[tolerantTestState], error) {
				return graph.FanOut(parent,
					graph.Branch[tolerantTestState]{Target: "worker-ok", State: tolerantTestState{Val: 10}},
					graph.Branch[tolerantTestState]{Target: "worker-end", State: tolerantTestState{Val: 20}},
					graph.Branch[tolerantTestState]{Target: "worker-fail", State: tolerantTestState{Val: 30}},
				), nil
			})
			node(t, g, "worker-ok", func(_ context.Context, _ graph.CallInfo, s tolerantTestState) (graph.Transition[tolerantTestState], error) {
				s.Val += 1
				return graph.CompleteBranch(s), nil
			})
			node(t, g, "worker-end", func(_ context.Context, _ graph.CallInfo, _ tolerantTestState) (graph.Transition[tolerantTestState], error) {
				return graph.EndBranch[tolerantTestState](), nil
			})
			if err := g.AddNode(graph.NodeSpec[tolerantTestState]{
				Name:    "worker-fail",
				OnError: graph.FailInvocation,
				Run: func(_ context.Context, _ graph.CallInfo, _ tolerantTestState) (graph.Transition[tolerantTestState], error) {
					return graph.Transition[tolerantTestState]{}, errors.New("worker-fail boom")
				},
			}); err != nil {
				t.Fatal(err)
			}
			if err := g.AddJoin(graph.JoinSpec[tolerantTestState]{
				Name:    "joined",
				From:    "dispatch",
				OnError: graph.FailExecution,
				Merge: func(_ context.Context, call graph.CallInfo, parent tolerantTestState, branches []graph.BranchResult[tolerantTestState]) (tolerantTestState, error) {
					mergeCalls <- call
					mergeResults = append([]graph.BranchResult[tolerantTestState](nil), branches...)
					values := graph.BranchValues(branches)
					if len(values) != 1 || values[0].Val != 11 {
						return tolerantTestState{}, fmt.Errorf("unexpected BranchValues: %+v", values)
					}
					for _, b := range branches {
						if b.OK() {
							parent.Val += b.State.Val
							parent.Branches = append(parent.Branches, fmt.Sprintf("%d:%s:ok", b.Index, b.Target))
						} else if b.Failed() {
							parent.Failures = append(parent.Failures, b.Error)
							parent.Branches = append(parent.Branches, fmt.Sprintf("%d:%s:failed:%s", b.Index, b.Target, b.Error))
						} else if b.Ended() {
							parent.Branches = append(parent.Branches, fmt.Sprintf("%d:%s:ended", b.Index, b.Target))
						}
					}
					return parent, nil
				},
			}); err != nil {
				t.Fatal(err)
			}
			node(t, g, "done", func(_ context.Context, _ graph.CallInfo, s tolerantTestState) (graph.Transition[tolerantTestState], error) {
				return graph.EndExecution(s), nil
			})
			edge(t, g, "dispatch", "worker-ok")
			edge(t, g, "dispatch", "worker-end")
			edge(t, g, "dispatch", "worker-fail")
			edge(t, g, "joined", "done")

			r, err := g.Compile(graph.Config[tolerantTestState]{MachineID: "tolerant-join-v1", Clone: cloneTolerantState})
			if err != nil {
				t.Fatal(err)
			}

			backing := tolerantStore(t, kind)
			store := &rejectTolerantStore{Store: backing}
			store.reject.Store(true)

			// Start with conflict injected on Join commit to verify reloaded branches on recovery
			execution, rejected, err := r.Start(context.Background(), "tolerant-run", tolerantTestState{Val: 100}, store, graph.Options{MaxConcurrency: 4})
			if !errors.Is(err, graph.ErrConflict) {
				t.Fatalf("expected conflict on first start, got: %+v, %v", rejected, err)
			}
			firstMergeCall := <-mergeCalls

			// Validate stored checkpoint before recovery
			stored, err := backing.Load(context.Background(), "tolerant-run")
			if err != nil {
				t.Fatalf("Load checkpoint error: %v", err)
			}
			var failedInv *graph.Invocation[tolerantTestState]
			for i := range stored.Invocations {
				if stored.Invocations[i].Status == graph.InvocationFailed {
					failedInv = &stored.Invocations[i]
					break
				}
			}
			if failedInv == nil {
				t.Fatalf("stored checkpoint missing InvocationFailed: %+v", stored.Invocations)
			}
			if failedInv.Target != "worker-fail" || failedInv.Error != "worker-fail boom" {
				t.Fatalf("failed invocation metadata mismatch: Target=%q Error=%q", failedInv.Target, failedInv.Error)
			}

			// Recover execution: Merge will re-run with branches reconstructed from loaded checkpoint
			mergeResults = nil
			_, completed, err := r.Recover(context.Background(), "tolerant-run", nil, store, graph.Options{MaxConcurrency: 4})
			if err != nil || completed.Status != graph.StatusCompletedWithFailures || completed.Final == nil {
				t.Fatalf("Recover = %+v, %v", completed, err)
			}
			secondMergeCall := <-mergeCalls
			if firstMergeCall.CallID == "" || firstMergeCall.CallID != secondMergeCall.CallID {
				t.Fatalf("Merge CallID did not remain stable: %q vs %q", firstMergeCall.CallID, secondMergeCall.CallID)
			}

			// Validate final state
			if completed.Final.Val != 111 {
				t.Fatalf("expected Val 111, got %d", completed.Final.Val)
			}
			if !reflect.DeepEqual(completed.Final.Failures, []string{"worker-fail boom"}) {
				t.Fatalf("unexpected Failures: %+v", completed.Final.Failures)
			}
			wantBranches := []string{
				"0:worker-ok:ok",
				"1:worker-end:ended",
				"2:worker-fail:failed:worker-fail boom",
			}
			if !reflect.DeepEqual(completed.Final.Branches, wantBranches) {
				t.Fatalf("unexpected Branches: %+v, want %+v", completed.Final.Branches, wantBranches)
			}
			if !completed.Header.HadLocalFailures || completed.Header.Failure != nil {
				t.Fatalf("unexpected Header: %+v", completed.Header)
			}

			// Validate recovered mergeResults
			if len(mergeResults) != 3 {
				t.Fatalf("expected 3 branch results, got %d", len(mergeResults))
			}
			b0 := mergeResults[0]
			if b0.Index != 0 || b0.Target != "worker-ok" || b0.Node != "joined" || b0.Status != graph.InvocationJoined || !b0.OK() || b0.Failed() || b0.Ended() || b0.State.Val != 11 || b0.Error != "" {
				t.Fatalf("unexpected b0: %+v", b0)
			}
			b1 := mergeResults[1]
			if b1.Index != 1 || b1.Target != "worker-end" || b1.Node != "worker-end" || b1.Status != graph.InvocationEnded || b1.OK() || b1.Failed() || !b1.Ended() || b1.Error != "" {
				t.Fatalf("unexpected b1: %+v", b1)
			}
			b2 := mergeResults[2]
			if b2.Index != 2 || b2.Target != "worker-fail" || b2.Node != "worker-fail" || b2.Status != graph.InvocationFailed || b2.OK() || !b2.Failed() || b2.Ended() || b2.Error != "worker-fail boom" {
				t.Fatalf("unexpected b2: %+v", b2)
			}

			if _, err := execution.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
