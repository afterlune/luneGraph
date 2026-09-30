package model

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
)

// Independent oracle: allocate structure directly, without Copy or its helpers.
func referenceCheckpointCopy[S any](src Checkpoint[S]) Checkpoint[S] {
	out := src
	out.Invocations = append([]Invocation[S](nil), src.Invocations...)
	for i := range out.Invocations {
		out.Invocations[i].Next = append([]string(nil), src.Invocations[i].Next...)
	}
	out.Groups = append([]ActivationGroup(nil), src.Groups...)
	for i := range out.Groups {
		out.Groups[i].Children = append([]string(nil), src.Groups[i].Children...)
	}
	if src.Failure != nil {
		failure := *src.Failure
		out.Failure = &failure
	}
	if src.Final != nil {
		value := *src.Final
		out.Final = &value
	}
	return out
}

func TestCheckpointCopyAgainstIndependentOracle(t *testing.T) {
	t.Run("scalar", func(t *testing.T) { checkCopySequence(t, func(i int) int { return i + 1 }) })
	t.Run("reference", func(t *testing.T) {
		checkCopySequence(t, func(i int) copyReferenceState { return copyReferenceState{Values: map[string]int{"value": i + 1}} })
	})
	t.Run("value512", func(t *testing.T) {
		checkCopySequence(t, func(i int) copyValueState { return copyValueState{uint64(i + 1), 42, 93} })
	})
}

func checkCopySequence[S any](t *testing.T, state func(int) S) {
	rng := rand.New(rand.NewPCG(37, 91))
	var dst Checkpoint[S]
	for step, size := range []int{0, 1, 8, 9, 128, 512, 130, 9, 8, 0, 512, 1, 128, 0} {
		final := state(step)
		src := Checkpoint[S]{FormatVersion: CheckpointFormatVersion, RunID: "copy", MachineID: "typed", Revision: uint64(step + 1), Steps: uint64(step), NextID: 1000, ScheduleCursor: 77, Completed: step%2 == 0, Final: &final}
		for i := range size {
			inv := Invocation[S]{ID: fmt.Sprintf("i%d", i+1), CallID: fmt.Sprintf("c%d", i+1), Node: "node", State: state(i), Status: InvocationReady, GroupID: "g1", ChildGroupID: "g2", BranchIndex: i, Continuation: "advance"}
			switch rng.IntN(4) {
			case 0:
				inv.Next = []string{}
			case 1:
				inv.Next = []string{"a"}
			case 2:
				inv.Next = []string{"a", "b", "c"}
			}
			src.Invocations = append(src.Invocations, inv)
		}
		if size > 0 {
			src.Groups = []ActivationGroup{{ID: "g1", CallID: "c100", Source: "fork", ParentID: "i1", JoinNode: "join", Children: []string{"i2", "i3"}}}
			src.Failure = &Failure{InvocationID: "i10", Node: "node", Message: "failure", PanicStack: "stack"}
		}
		oldInv, oldGroups := dst.Invocations, dst.Groups
		oldRoutes := make([][]string, len(oldInv))
		for i := range oldInv {
			oldRoutes[i] = oldInv[i].Next
		}
		CopyInto(&dst, src)
		want := referenceCheckpointCopy(src)
		if !reflect.DeepEqual(dst, want) || !reflect.DeepEqual(Copy(src), want) {
			t.Fatalf("step %d size %d differs from reference", step, size)
		}
		if size > 0 && cap(oldInv) >= size && &oldInv[0] != &dst.Invocations[0] {
			t.Fatal("invocation storage not reused")
		}
		for _, entry := range oldInv[min(size, len(oldInv)):] {
			if !reflect.DeepEqual(entry, Invocation[S]{}) {
				t.Fatal("shrunken invocation tail retained")
			}
		}
		if len(src.Groups) == 0 {
			for _, entry := range oldGroups {
				if !reflect.DeepEqual(entry, ActivationGroup{}) {
					t.Fatal("group tail retained")
				}
			}
		}
		for i, inv := range src.Invocations {
			if inv.Next == nil || len(inv.Next) == 0 {
				if i < len(oldRoutes) {
					for _, value := range oldRoutes[i] {
						if value != "" {
							t.Fatal("obsolete route retained")
						}
					}
				}
			} else {
				dst.Invocations[i].Next[0] = "changed"
				if src.Invocations[i].Next[0] != "a" {
					t.Fatal("route structure shared")
				}
				dst.Invocations[i].Next[0] = "a"
			}
		}
		if len(dst.Groups) > 0 {
			dst.Groups[0].Children[0] = "changed"
			if src.Groups[0].Children[0] != "i2" {
				t.Fatal("group structure shared")
			}
			dst.Groups[0].Children[0] = "i2"
		}
		*dst.Final = state(step + 100)
		if !reflect.DeepEqual(*src.Final, final) {
			t.Fatal("final pointer shared")
		}
	}
}

func TestCheckpointBulkCopyKeepsStateShallow(t *testing.T) {
	src := Checkpoint[[]int]{Invocations: make([]Invocation[[]int], 32)}
	for i := range src.Invocations {
		src.Invocations[i].State = []int{i}
	}
	dst := Copy(src)
	for i := range dst.Invocations {
		if &dst.Invocations[i].State[0] != &src.Invocations[i].State[0] {
			t.Fatal("structural copy cloned application state")
		}
	}
}
