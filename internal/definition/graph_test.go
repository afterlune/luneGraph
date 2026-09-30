package definition

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
)

func TestGraphBuilderValidation(t *testing.T) {
	var nilGraph *Graph[int]
	if err := nilGraph.AddNode(model.NodeSpec[int]{}); err == nil {
		t.Fatal("nil graph accepted node")
	}
	if err := nilGraph.AddJoin(model.JoinSpec[int]{}); err == nil {
		t.Fatal("nil graph accepted join")
	}
	if err := nilGraph.AddSubgraph("child", New[int]("entry")); err == nil {
		t.Fatal("nil graph accepted subgraph")
	}
	if err := nilGraph.AddEdge("a", "b"); err == nil {
		t.Fatal("nil graph accepted edge")
	}
	if err := RegisterContinuation[int, int](nil, "key", func([]byte) (int, error) { return 0, nil }, func(context.Context, model.CallInfo, int, int) (int, error) { return 0, nil }); err == nil {
		t.Fatal("nil graph accepted continuation")
	}

	g := New[int]("entry")
	validNode := model.Node[int](func(context.Context, model.CallInfo, int) (model.Transition[int], error) {
		return model.EndExecution(0), nil
	})
	for _, spec := range []model.NodeSpec[int]{
		{Name: "", Run: validNode},
		{Name: " padded", Run: validNode},
		{Name: "missing-run"},
		{Name: "bad-scope", Run: validNode, OnError: model.FailureScope(99)},
	} {
		if err := g.AddNode(spec); err == nil {
			t.Fatalf("AddNode accepted %+v", spec)
		}
	}
	addDefinitionNode(t, g, "entry", validNode)
	if err := g.AddNode(model.NodeSpec[int]{Name: "entry", Run: validNode}); err == nil {
		t.Fatal("duplicate node accepted")
	}

	merge := model.Merge[int](func(context.Context, model.CallInfo, []int) (int, error) { return 0, nil })
	for _, spec := range []model.JoinSpec[int]{
		{Name: "", From: "entry", Merge: merge},
		{Name: "join", From: "", Merge: merge},
		{Name: "join", From: "entry"},
		{Name: "join", From: "entry", Merge: merge, OnError: model.FailureScope(99)},
	} {
		if err := g.AddJoin(spec); err == nil {
			t.Fatalf("AddJoin accepted %+v", spec)
		}
	}
	addDefinitionNode(t, g, "left", validNode)
	addDefinitionNode(t, g, "right", validNode)
	addDefinitionJoin(t, g, model.JoinSpec[int]{Name: "joined", From: "entry", Merge: merge})
	if err := g.AddJoin(model.JoinSpec[int]{Name: "joined", From: "entry", Merge: merge}); err == nil {
		t.Fatal("duplicate join accepted")
	}
	if err := g.AddJoin(model.JoinSpec[int]{Name: "left", From: "entry", Merge: merge}); err == nil {
		t.Fatal("join colliding with node accepted")
	}

	child := New[int]("child-entry")
	addDefinitionNode(t, child, "child-entry", validNode)
	if err := g.AddSubgraph("", child); err == nil {
		t.Fatal("empty subgraph name accepted")
	}
	if err := g.AddSubgraph("nil-child", nil); err == nil {
		t.Fatal("nil child accepted")
	}
	if err := g.AddSubgraph("entry", child); err == nil {
		t.Fatal("subgraph colliding with node accepted")
	}
	if err := g.AddSubgraph("joined", child); err == nil {
		t.Fatal("subgraph colliding with join accepted")
	}
	if err := g.AddSubgraph("worker", child); err != nil {
		t.Fatal(err)
	}
	if err := g.AddSubgraph("worker", child); err == nil {
		t.Fatal("duplicate subgraph accepted")
	}

	if err := g.AddEdge("missing", "entry"); err == nil {
		t.Fatal("unknown edge source accepted")
	}
	if err := g.AddEdge("entry", "missing"); err == nil {
		t.Fatal("unknown edge target accepted")
	}
	addDefinitionNode(t, g, "after", validNode)
	addDefinitionEdge(t, g, "entry", "worker")
	addDefinitionEdge(t, g, "worker", "after")
	if err := g.AddEdge("worker", "after"); err == nil {
		t.Fatal("duplicate edge accepted")
	}

	decode := func([]byte) (int, error) { return 1, nil }
	apply := func(_ context.Context, _ model.CallInfo, state, value int) (int, error) { return state + value, nil }
	for _, register := range []func() error{
		func() error { return RegisterContinuation[int, int](g, "", decode, apply) },
		func() error { return RegisterContinuation[int, int](g, "nil-decode", nil, apply) },
		func() error { return RegisterContinuation[int, int](g, "nil-apply", decode, nil) },
	} {
		if err := register(); err == nil {
			t.Fatal("invalid continuation accepted")
		}
	}
	if err := RegisterContinuation[int, int](g, "typed", decode, apply); err != nil {
		t.Fatal(err)
	}
	if err := RegisterContinuation[int, int](g, "typed", decode, apply); err == nil {
		t.Fatal("duplicate continuation accepted")
	}
	continuation := g.continuations["typed"]
	value, err := continuation.Decode(nil)
	if err != nil {
		t.Fatal(err)
	}
	state, err := continuation.Apply(context.Background(), model.CallInfo{}, 4, value)
	if err != nil || state != 5 {
		t.Fatalf("typed continuation = %d, %v", state, err)
	}
}

func TestExpandRewritesMountedGraphAndContinuationNames(t *testing.T) {
	child := New[int]("begin")
	var originalTargets = []string{"finish"}
	addDefinitionNode(t, child, "begin", func(_ context.Context, _ model.CallInfo, state int) (model.Transition[int], error) {
		return model.Transition[int]{State: state, Action: model.ActionWait, Targets: originalTargets, Continuation: "resume"}, nil
	})
	addDefinitionNode(t, child, "finish", func(_ context.Context, _ model.CallInfo, state int) (model.Transition[int], error) {
		return model.Return(state), nil
	})
	addDefinitionEdge(t, child, "begin", "finish")
	if err := RegisterContinuation[int, int](child, "resume", func([]byte) (int, error) { return 1, nil }, func(_ context.Context, _ model.CallInfo, state, value int) (int, error) { return state + value, nil }); err != nil {
		t.Fatal(err)
	}

	parent := New[int]("start")
	addDefinitionNode(t, parent, "start", func(_ context.Context, _ model.CallInfo, state int) (model.Transition[int], error) {
		return model.To(state, "worker"), nil
	})
	addDefinitionNode(t, parent, "after", func(_ context.Context, _ model.CallInfo, state int) (model.Transition[int], error) {
		return model.EndExecution(state), nil
	})
	if err := parent.AddSubgraph("worker", child); err != nil {
		t.Fatal(err)
	}
	addDefinitionEdge(t, parent, "start", "worker")
	addDefinitionEdge(t, parent, "worker", "after")

	machine, err := expand(parent)
	if err != nil {
		t.Fatal(err)
	}
	if machine.Entry != "start" || len(machine.Nodes) != 4 {
		t.Fatalf("flattened machine entry=%q nodes=%v", machine.Entry, machine.Nodes)
	}
	if _, ok := machine.Edges["start"]["worker/begin"]; !ok {
		t.Fatalf("parent edge was not expanded: %v", machine.Edges)
	}
	if _, exists := machine.Edges["worker"]; exists {
		t.Fatalf("mount alias became a runtime source: %v", machine.Edges["worker"])
	}
	if _, ok := machine.Edges["worker/begin"]["worker/finish"]; !ok || machine.ReturnTargets["worker/begin"] != "after" || machine.ReturnTargets["worker/finish"] != "after" {
		t.Fatalf("flattened child edges=%v return targets=%v", machine.Edges, machine.ReturnTargets)
	}
	if _, ok := machine.Continuations["worker/resume"]; !ok {
		t.Fatalf("continuation names = %v", machine.Continuations)
	}
	start, err := machine.Nodes["start"].Run(context.Background(), model.CallInfo{}, 3)
	if err != nil || !reflect.DeepEqual(start.Targets, []string{"worker/begin"}) {
		t.Fatalf("parent transition = %+v, %v", start, err)
	}
	childTransition, err := machine.Nodes["worker/begin"].Run(context.Background(), model.CallInfo{}, 3)
	if err != nil || childTransition.Continuation != "worker/resume" || !reflect.DeepEqual(childTransition.Targets, []string{"worker/finish"}) || originalTargets[0] != "finish" {
		t.Fatalf("child transition = %+v, original targets=%v, err=%v", childTransition, originalTargets, err)
	}
}

func TestExpandHandlesNestedAndRepeatedMounts(t *testing.T) {
	shared := New[int]("run")
	addDefinitionNode(t, shared, "run", func(_ context.Context, _ model.CallInfo, value int) (model.Transition[int], error) {
		return model.Return(value), nil
	})
	root := New[int]("first")
	if err := root.AddSubgraph("first", shared); err != nil {
		t.Fatal(err)
	}
	if err := root.AddSubgraph("second", shared); err != nil {
		t.Fatal(err)
	}
	addDefinitionNode(t, root, "begin", func(_ context.Context, _ model.CallInfo, value int) (model.Transition[int], error) {
		return model.To(value, "first"), nil
	})
	root.entry = "begin"
	addDefinitionEdge(t, root, "begin", "first")
	addDefinitionEdge(t, root, "first", "second")
	machine, err := expand(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := machine.Nodes["first/run"]; !ok {
		t.Fatalf("first mount missing: %v", machine.Nodes)
	}
	if _, ok := machine.Nodes["second/run"]; !ok || machine.ReturnTargets["first/run"] != "second/run" {
		t.Fatalf("repeated mounts nodes=%v returns=%v", machine.Nodes, machine.ReturnTargets)
	}

	inner := New[int]("inner-run")
	addDefinitionNode(t, inner, "inner-run", func(_ context.Context, _ model.CallInfo, value int) (model.Transition[int], error) {
		return model.Return(value), nil
	})
	outer := New[int]("outer-run")
	if err := outer.AddSubgraph("nested", inner); err != nil {
		t.Fatal(err)
	}
	outer.entry = "nested"
	addDefinitionNode(t, outer, "outer-run", func(_ context.Context, _ model.CallInfo, value int) (model.Transition[int], error) {
		return model.Return(value), nil
	})
	addDefinitionEdge(t, outer, "nested", "outer-run")
	root = New[int]("outer")
	if err := root.AddSubgraph("outer", outer); err != nil {
		t.Fatal(err)
	}
	addDefinitionNode(t, root, "done", func(_ context.Context, _ model.CallInfo, value int) (model.Transition[int], error) {
		return model.EndExecution(value), nil
	})
	addDefinitionEdge(t, root, "outer", "done")
	machine, err = expand(root)
	if err != nil {
		t.Fatal(err)
	}
	if machine.Entry != "outer/nested/inner-run" || machine.ReturnTargets["outer/nested/inner-run"] != "outer/outer-run" || machine.ReturnTargets["outer/outer-run"] != "done" {
		t.Fatalf("nested entry=%q returns=%v", machine.Entry, machine.ReturnTargets)
	}
}

func TestCompileAndExpandRejectInvalidDefinitions(t *testing.T) {
	identity := func(value int) (int, error) { return value, nil }
	validNode := model.Node[int](func(context.Context, model.CallInfo, int) (model.Transition[int], error) {
		return model.EndExecution(0), nil
	})
	merge := model.Merge[int](func(context.Context, model.CallInfo, []int) (int, error) { return 0, nil })
	compile := func(g *Graph[int]) error {
		_, err := g.Compile(model.Config[int]{MachineID: "machine", Clone: identity})
		return err
	}
	tests := []struct {
		name  string
		build func(*testing.T) *Graph[int]
		match string
	}{
		{name: "unknown entry", build: func(*testing.T) *Graph[int] { return New[int]("missing") }, match: "entry vertex"},
		{name: "join entry", build: func(t *testing.T) *Graph[int] {
			g := New[int]("joined")
			addDefinitionNode(t, g, "source", validNode)
			addDefinitionJoin(t, g, model.JoinSpec[int]{Name: "joined", From: "source", Merge: merge})
			return g
		}, match: "is a join"},
		{name: "unknown join source", build: func(t *testing.T) *Graph[int] {
			g := New[int]("source")
			addDefinitionNode(t, g, "source", validNode)
			addDefinitionJoin(t, g, model.JoinSpec[int]{Name: "join", From: "missing", Merge: merge})
			return g
		}, match: "unknown fan-out source"},
		{name: "two joins for source", build: func(t *testing.T) *Graph[int] {
			g := New[int]("source")
			addDefinitionNode(t, g, "source", validNode)
			addDefinitionJoin(t, g, model.JoinSpec[int]{Name: "one", From: "source", Merge: merge})
			addDefinitionJoin(t, g, model.JoinSpec[int]{Name: "two", From: "source", Merge: merge})
			return g
		}, match: "more than one join"},
		{name: "join multiple outputs", build: func(t *testing.T) *Graph[int] {
			g := New[int]("source")
			addDefinitionNode(t, g, "source", validNode)
			addDefinitionJoin(t, g, model.JoinSpec[int]{Name: "join", From: "source", Merge: merge})
			addDefinitionNode(t, g, "a", validNode)
			addDefinitionNode(t, g, "b", validNode)
			addDefinitionEdge(t, g, "join", "a")
			addDefinitionEdge(t, g, "join", "b")
			return g
		}, match: "at most one outgoing edge"},
		{name: "subgraph multiple outputs", build: func(t *testing.T) *Graph[int] {
			g := New[int]("worker")
			child := New[int]("work")
			addDefinitionNode(t, child, "work", validNode)
			if err := g.AddSubgraph("worker", child); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"a", "b"} {
				addDefinitionNode(t, g, name, validNode)
				addDefinitionEdge(t, g, "worker", name)
			}
			return g
		}, match: "at most one outgoing edge"},
		{name: "recursive child", build: func(t *testing.T) *Graph[int] {
			g := New[int]("self")
			if err := g.AddSubgraph("self", g); err != nil {
				t.Fatal(err)
			}
			return g
		}, match: "recursive subgraph"},
		{name: "qualified vertex collision", build: func(t *testing.T) *Graph[int] {
			g := New[int]("worker")
			child := New[int]("work")
			addDefinitionNode(t, child, "work", validNode)
			if err := g.AddSubgraph("worker", child); err != nil {
				t.Fatal(err)
			}
			addDefinitionNode(t, g, "worker/work", validNode)
			return g
		}, match: "collides"},
		{name: "qualified continuation collision", build: func(t *testing.T) *Graph[int] {
			g := New[int]("worker")
			child := New[int]("work")
			addDefinitionNode(t, child, "work", validNode)
			if err := g.AddSubgraph("worker", child); err != nil {
				t.Fatal(err)
			}
			registerDefinitionContinuation(t, g, "worker/input")
			registerDefinitionContinuation(t, child, "input")
			return g
		}, match: "collides"},
		{name: "nil child from malformed builder", build: func(t *testing.T) *Graph[int] {
			g := New[int]("entry")
			addDefinitionNode(t, g, "entry", validNode)
			g.subgraphs["broken"] = nil
			return g
		}, match: "is nil"},
		{name: "unknown edge source from malformed builder", build: func(t *testing.T) *Graph[int] {
			g := New[int]("entry")
			addDefinitionNode(t, g, "entry", validNode)
			g.edges["missing"] = map[string]struct{}{"entry": {}}
			return g
		}, match: "unknown edge source"},
		{name: "unknown edge target from malformed builder", build: func(t *testing.T) *Graph[int] {
			g := New[int]("entry")
			addDefinitionNode(t, g, "entry", validNode)
			g.edges["entry"] = map[string]struct{}{"missing": {}}
			return g
		}, match: "unknown edge target"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := compile(test.build(t))
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error = %v, want %q", err, test.match)
			}
		})
	}
	if _, err := buildComponent[int](nil, "", nil); err == nil {
		t.Fatal("buildComponent accepted nil graph")
	}
	if _, err := (*Graph[int])(nil).Compile(model.Config[int]{MachineID: "m", Clone: identity}); err == nil {
		t.Fatal("Compile accepted nil graph")
	}
	if err := compile(New[int]("missing")); err == nil {
		t.Fatal("Compile accepted an invalid entry")
	}
	for _, config := range []model.Config[int]{{MachineID: "", Clone: identity}, {MachineID: " machine", Clone: identity}, {MachineID: "m"}} {
		g := New[int]("entry")
		addDefinitionNode(t, g, "entry", validNode)
		if _, err := g.Compile(config); err == nil {
			t.Fatalf("Compile accepted config %+v", config)
		}
	}
}

func TestRewriteNodePreservesNonRoutingActionsAndErrors(t *testing.T) {
	boom := errors.New("node error")
	run := model.Node[int](func(_ context.Context, _ model.CallInfo, state int) (model.Transition[int], error) {
		return model.Return(state), nil
	})
	wrapped := rewriteNode(run, map[string]string{"next": "child/next"}, map[string]string{"wait": "child/wait"})
	transition, err := wrapped(context.Background(), model.CallInfo{}, 3)
	if err != nil || transition.Action != model.ActionReturn {
		t.Fatalf("Return changed by wrapper: %+v, %v", transition, err)
	}
	wrapped = rewriteNode(func(context.Context, model.CallInfo, int) (model.Transition[int], error) {
		return model.Transition[int]{}, boom
	}, nil, nil)
	if _, err := wrapped(context.Background(), model.CallInfo{}, 0); !errors.Is(err, boom) {
		t.Fatalf("node error lost by wrapper: %v", err)
	}
}

func addDefinitionNode(t *testing.T, g *Graph[int], name string, run model.Node[int]) {
	t.Helper()
	if err := g.AddNode(model.NodeSpec[int]{Name: name, Run: run}); err != nil {
		t.Fatal(err)
	}
}

func addDefinitionJoin(t *testing.T, g *Graph[int], spec model.JoinSpec[int]) {
	t.Helper()
	if err := g.AddJoin(spec); err != nil {
		t.Fatal(err)
	}
}

func addDefinitionEdge(t *testing.T, g *Graph[int], from, to string) {
	t.Helper()
	if err := g.AddEdge(from, to); err != nil {
		t.Fatal(err)
	}
}

func registerDefinitionContinuation(t *testing.T, g *Graph[int], key string) {
	t.Helper()
	if err := RegisterContinuation[int, int](g, key, func([]byte) (int, error) { return 1, nil }, func(_ context.Context, _ model.CallInfo, state, value int) (int, error) {
		return state + value, nil
	}); err != nil {
		t.Fatal(err)
	}
}
