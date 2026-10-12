package modeltest

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	graph "github.com/afterlune/luneGraph"
	"github.com/afterlune/luneGraph/checkpoint"
	"github.com/afterlune/luneGraph/checkpoint/memory"
	"github.com/afterlune/luneGraph/checkpoint/sqlite"
)

type dynamicState struct {
	Owner, Task, Target string
	Depth, MaxDepth     int
	Total, Leaves       int
}

func cloneDynamicState(state dynamicState) (dynamicState, error) { return state, nil }

func dynamicModelRunner(t testing.TB) *graph.Runner[dynamicState] {
	t.Helper()
	g := graph.New[dynamicState]("lead")
	add := func(name string, run graph.Node[dynamicState]) {
		t.Helper()
		if err := g.AddNode(graph.NodeSpec[dynamicState]{Name: name, Run: run}); err != nil {
			t.Fatal(err)
		}
	}
	add("lead", func(_ context.Context, _ graph.CallInfo, parent dynamicState) (graph.Transition[dynamicState], error) {
		branches := make([]graph.Branch[dynamicState], parent.Total)
		for i := range branches {
			target := "linear"
			if i%2 == 1 {
				target = "split"
			}
			branches[i] = graph.Branch[dynamicState]{Target: target, State: dynamicState{Owner: parent.Owner, Task: fmt.Sprintf("t%d", i), Target: target, MaxDepth: parent.MaxDepth}}
		}
		return graph.FanOut(parent, branches...), nil
	})
	leaf := func(_ context.Context, _ graph.CallInfo, state dynamicState) (graph.Transition[dynamicState], error) {
		state.Total, state.Leaves = 1, 1
		return graph.CompleteBranch(state), nil
	}
	add("linear", leaf)
	add("split", func(_ context.Context, _ graph.CallInfo, state dynamicState) (graph.Transition[dynamicState], error) {
		if state.Depth >= state.MaxDepth {
			return leaf(context.Background(), graph.CallInfo{}, state)
		}
		left, right := state, state
		left.Task, left.Target, left.Depth = state.Task+"/a", "linear", state.Depth+1
		right.Task, right.Target, right.Depth = state.Task+"/b", "split", state.Depth+1
		return graph.FanOut(state,
			graph.Branch[dynamicState]{Target: "linear", State: left},
			graph.Branch[dynamicState]{Target: "split", State: right}), nil
	})
	merge := func(_ context.Context, _ graph.CallInfo, parent dynamicState, branches []dynamicState) (dynamicState, error) {
		parent.Total, parent.Leaves = 0, 0
		for _, branch := range branches {
			if branch.Owner != parent.Owner || branch.Total != branch.Leaves || branch.Leaves < 1 {
				return dynamicState{}, fmt.Errorf("invalid dynamic result for %s: %+v", parent.Task, branch)
			}
			parent.Total += branch.Total
			parent.Leaves += branch.Leaves
		}
		return parent, nil
	}
	for _, join := range []graph.JoinSpec[dynamicState]{{Name: "lead-join", From: "lead", Merge: merge}, {Name: "worker-join", From: "split", Merge: merge}} {
		if err := g.AddJoin(join); err != nil {
			t.Fatal(err)
		}
	}
	add("return", func(_ context.Context, _ graph.CallInfo, state dynamicState) (graph.Transition[dynamicState], error) {
		return graph.CompleteBranch(state), nil
	})
	add("finish", func(_ context.Context, _ graph.CallInfo, state dynamicState) (graph.Transition[dynamicState], error) {
		return graph.EndExecution(state), nil
	})
	for _, edge := range [][2]string{{"lead", "linear"}, {"lead", "split"}, {"split", "linear"}, {"split", "split"}, {"lead-join", "finish"}, {"worker-join", "return"}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	r, err := g.Compile(graph.Config[dynamicState]{MachineID: "dynamic-model-v1", Clone: cloneDynamicState})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func dynamicStore(t testing.TB, kind string) graph.Store[dynamicState] {
	t.Helper()
	if kind == "memory" {
		store, err := memory.New[dynamicState](cloneDynamicState)
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "dynamic.db"), checkpoint.JSON[dynamicState]{})
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

func checkDynamicPosition(cp graph.Checkpoint[dynamicState], tasks, maxDepth int) error {
	if cp.MachineID != "dynamic-model-v1" || cp.FormatVersion != graph.CheckpointFormatVersion || cp.Failure != nil || cp.HadLocalFailures {
		return fmt.Errorf("unexpected header: %+v", cp.Header())
	}
	invocations := make(map[string]graph.Invocation[dynamicState], len(cp.Invocations))
	for _, inv := range cp.Invocations {
		if inv.State.Owner != cp.RunID || inv.State.Depth < 0 || inv.State.Depth > maxDepth || inv.State.Total < 0 || inv.State.Leaves < 0 {
			return fmt.Errorf("invalid dynamic invocation state: %+v", inv)
		}
		if inv.Status == graph.InvocationJoined && (inv.State.Total < 1 || inv.State.Total != inv.State.Leaves) {
			return fmt.Errorf("invalid completed branch result: %+v", inv)
		}
		invocations[inv.ID] = inv
	}
	for _, activation := range cp.Groups {
		parent, ok := invocations[activation.ParentID]
		if !ok || parent.ChildGroupID != activation.ID || len(activation.Children) != 2 && activation.Source == "split" || len(activation.Children) != tasks && activation.Source == "lead" {
			return fmt.Errorf("invalid dynamic activation: %+v", activation)
		}
		for i, id := range activation.Children {
			child, ok := invocations[id]
			if !ok || child.GroupID != activation.ID || child.BranchIndex != i {
				return fmt.Errorf("invalid child %d of %s", i, activation.ID)
			}
			if activation.Source == "lead" {
				wantTarget := "linear"
				if i%2 == 1 {
					wantTarget = "split"
				}
				if child.State.Target != wantTarget || child.State.Task != fmt.Sprintf("t%d", i) {
					return fmt.Errorf("lead branch identity changed: %+v", child.State)
				}
			} else if child.State.Depth != parent.State.Depth+1 || child.State.Task != parent.State.Task+[]string{"/a", "/b"}[i] {
				return fmt.Errorf("nested branch identity changed: %+v", child.State)
			}
		}
	}
	if cp.Completed {
		want := 0
		for i := 0; i < tasks; i++ {
			if i%2 == 0 {
				want++
				continue
			}
			want += maxDepth + 1
		}
		if cp.Final == nil || cp.Final.Total != want || cp.Final.Leaves != want || cp.Final.Owner != cp.RunID {
			return fmt.Errorf("final result differs from independent count %d: %+v", want, cp.Final)
		}
	}
	return nil
}

func runDynamicModel(t testing.TB, kind string, tasks, depth int) {
	t.Helper()
	runner := dynamicModelRunner(t)
	store := dynamicStore(t, kind)
	runID := fmt.Sprintf("dynamic-%d-%d", tasks, depth)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, result, err := runner.Start(ctx, runID, dynamicState{Owner: runID, Task: "root", Target: "lead", Total: tasks, MaxDepth: depth}, store, graph.Options{MaxSteps: 1, MaxConcurrency: 4})
	if err != nil {
		t.Fatalf("start: %+v %v", result, err)
	}
	for call := 0; call < 8*tasks*(depth+2)+16 && result.Status != graph.StatusCompleted; call++ {
		position, err := store.Load(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkDynamicPosition(position, tasks, depth); err != nil {
			t.Fatal(err)
		}
		_, result, err = runner.Recover(ctx, runID, nil, store, graph.Options{MaxSteps: 1, MaxConcurrency: 4})
		if err != nil {
			t.Fatalf("recover call %d: %+v %v", call, result, err)
		}
	}
	final, err := store.Load(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkDynamicPosition(final, tasks, depth); err != nil {
		t.Fatal(err)
	}
	if !final.Completed || result.Status != graph.StatusCompleted {
		t.Fatalf("dynamic execution did not finish: status=%s checkpoint=%+v", result.Status, final.Header())
	}
}

func TestDynamicFanOutModelSequences(t *testing.T) {
	round := 0
	for _, kind := range []string{"memory", "sqlite"} {
		for tasks := 0; tasks <= 4; tasks++ {
			for depth := 0; depth <= 3; depth++ {
				t.Run(fmt.Sprintf("%s/tasks=%d/depth=%d", kind, tasks, depth), func(t *testing.T) {
					runDynamicModel(t, kind, tasks, depth)
				})
				round++
			}
		}
	}
	if round != 40 {
		t.Fatalf("model coverage rounds = %d", round)
	}
}

func FuzzDynamicFanOut(f *testing.F) {
	for _, seed := range [][]byte{{0, 0}, {1, 0}, {4, 3}, {3, 2}, {255, 254}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var tasks, depth int
		if len(data) > 0 {
			tasks = int(data[0] % 5)
		}
		if len(data) > 1 {
			depth = int(data[1] % 4)
		}
		runDynamicModel(t, "memory", tasks, depth)
	})
}
