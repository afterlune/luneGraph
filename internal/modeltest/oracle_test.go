package modeltest

import (
	"bytes"
	"testing"

	graph "github.com/afterlune/luneGraph"
)

func TestModelRejectsIncorrectBoundaries(t *testing.T) {
	for _, width := range []int{0, 2, 4} {
		m := model{width: width}
		s := state{Owner: "model-run", Round: 2, Sum: 2 * m.weight(), Inputs: 1, Branch: -1, Values: map[string]int{"sum": 2 * m.weight()}}
		cp := graph.Checkpoint[state]{RunID: "model-run", MachineID: "model-v1", FormatVersion: graph.CheckpointFormatVersion, Steps: uint64(2 * m.roundSteps()), Invocations: []graph.Invocation[state]{{Node: "work", Status: graph.InvocationWaiting, State: s}}}
		if width > 0 {
			cp.Invocations[0].Node = "boundary"
		}
		cp.Revision = cp.Steps + 2
		if err := m.check(cp); err != nil {
			t.Fatal(err)
		}
		mutations := []func(*graph.Checkpoint[state]){
			func(c *graph.Checkpoint[state]) { c.Steps++ }, func(c *graph.Checkpoint[state]) { c.Revision++ },
			func(c *graph.Checkpoint[state]) { c.Invocations[0].State.Owner = "other" },
			func(c *graph.Checkpoint[state]) { c.Invocations[0].State.Sum++ },
			func(c *graph.Checkpoint[state]) { c.Invocations[0].State.Inputs++ },
			func(c *graph.Checkpoint[state]) { c.Invocations[0].State.Values["speculative"] = 1 },
			func(c *graph.Checkpoint[state]) { c.Completed = true },
			func(c *graph.Checkpoint[state]) { c.Invocations[0].Node = "missing" },
			func(c *graph.Checkpoint[state]) { c.Invocations[0].Status = graph.InvocationFailed },
		}
		for i, mutate := range mutations {
			candidate, err := cp.Clone(clone)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&candidate)
			if m.check(candidate) == nil {
				t.Fatalf("width=%d mutation=%d accepted", width, i)
			}
		}
	}
}

func TestInstructionBounds(t *testing.T) {
	width, concurrency, capacity, ops := decode(bytes.Repeat([]byte{255}, 300))
	if width != 4 || concurrency != 4 || capacity != 4 || len(ops) != 64 {
		t.Fatal("decode limits")
	}
	for _, op := range ops {
		if op.mode != 7 || op.target != 4 || op.budget != 8 {
			t.Fatalf("decode=%+v", op)
		}
	}
	_, _, _, odd := decode([]byte{0, 56})
	if len(odd) != 1 || odd[0] != (instruction{mode: 0, target: 1, budget: 8}) {
		t.Fatalf("odd trailing instruction: %+v", odd)
	}
}
