package model

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestCheckpointCopySeparatesStructure(t *testing.T) {
	final := 9
	original := Checkpoint[int]{
		Final:       &final,
		Invocations: []Invocation[int]{{ID: "i1", Next: []string{"a", "b"}}},
		Groups:      []ActivationGroup{{ID: "g1", Children: []string{"i2", "i3"}}},
		Terminals:   []Terminal[int]{{InvocationID: "i4", State: 4}},
		Failures:    []Failure{{InvocationID: "i5", Message: "failure"}},
	}

	copied := Copy(original)
	copied.Invocations[0].Next[0] = "changed"
	copied.Groups[0].Children[0] = "changed"
	copied.Terminals[0].State = 40
	copied.Failures[0].Message = "changed"
	*copied.Final = 90
	if original.Invocations[0].Next[0] != "a" || original.Groups[0].Children[0] != "i2" || original.Terminals[0].State != 4 || original.Failures[0].Message != "failure" || final != 9 {
		t.Fatalf("Copy shared checkpoint structure: original=%+v final=%d", original, final)
	}
	if !reflect.DeepEqual(copied.Invocations[0].Next, []string{"changed", "b"}) {
		t.Fatalf("copied invocation next = %v", copied.Invocations[0].Next)
	}
}

func TestCheckpointCopyIntoReusesStructure(t *testing.T) {
	final := []int{5}
	original := Checkpoint[[]int]{
		RunID: "run",
		Final: &final,
		Invocations: []Invocation[[]int]{
			{ID: "i1", State: []int{1}, Next: []string{"a", "b"}},
			{ID: "i2", State: []int{2}, Next: []string{"c"}},
		},
		Groups:    []ActivationGroup{{ID: "g1", Children: []string{"i1", "i2"}}},
		Terminals: []Terminal[[]int]{{InvocationID: "i3", State: []int{3}}},
		Failures:  []Failure{{InvocationID: "i4", Message: "failed"}},
	}
	destination := Copy(original)
	invocationStorage := &destination.Invocations[0]
	nextStorage := &destination.Invocations[0].Next[0]
	groupStorage := &destination.Groups[0]
	childrenStorage := &destination.Groups[0].Children[0]
	terminalStorage := &destination.Terminals[0]
	failureStorage := &destination.Failures[0]

	next := Copy(original)
	next.RunID = "next-run"
	next.Revision = 2
	next.Invocations[0].State = []int{11}
	next.Invocations[0].Next = []string{"x", "y"}
	next.Groups[0].Children = []string{"i7", "i8"}
	next.Terminals[0].State = []int{33}
	next.Failures[0].Message = "next failure"
	CopyInto(&destination, next)

	if !reflect.DeepEqual(destination, Copy(next)) {
		t.Fatalf("CopyInto result = %+v, want %+v", destination, Copy(next))
	}
	if &destination.Invocations[0] != invocationStorage || &destination.Invocations[0].Next[0] != nextStorage ||
		&destination.Groups[0] != groupStorage || &destination.Groups[0].Children[0] != childrenStorage ||
		&destination.Terminals[0] != terminalStorage || &destination.Failures[0] != failureStorage {
		t.Fatal("CopyInto did not reuse destination storage")
	}
	destination.Invocations[0].Next[0] = "changed"
	destination.Groups[0].Children[0] = "changed"
	destination.Terminals[0].State[0] = 99
	destination.Failures[0].Message = "changed"
	if original.Invocations[0].Next[0] != "a" || original.Groups[0].Children[0] != "i1" || original.Terminals[0].State[0] != 3 || original.Failures[0].Message != "failed" {
		t.Fatalf("CopyInto shared structure with source: %+v", original)
	}
	if &destination.Invocations[0].State[0] != &next.Invocations[0].State[0] {
		t.Fatal("CopyInto changed Copy's shallow state-copy behavior")
	}
}

func TestCheckpointCopyIntoClearsUnusedEntries(t *testing.T) {
	large := Checkpoint[int]{
		Invocations: []Invocation[int]{
			{ID: "i1", Next: []string{"a", "b"}},
			{ID: "i2", Next: []string{"c"}},
		},
		Groups:    []ActivationGroup{{ID: "g1", Children: []string{"i1", "i2"}}},
		Terminals: []Terminal[int]{{InvocationID: "i3", State: 3}, {InvocationID: "i4", State: 4}},
		Failures:  []Failure{{InvocationID: "i5", Message: "stale"}, {InvocationID: "i6", Message: "stale"}},
	}
	destination := Copy(large)
	small := Checkpoint[int]{Invocations: []Invocation[int]{{ID: "i1", Next: []string{"only"}}}}
	CopyInto(&destination, small)
	if !reflect.DeepEqual(destination, Copy(small)) {
		t.Fatalf("CopyInto after shrink = %+v, want %+v", destination, Copy(small))
	}
	if got := destination.Invocations[:cap(destination.Invocations)]; !reflect.DeepEqual(got[1], Invocation[int]{}) {
		t.Fatalf("stale invocation retained in spare capacity: %+v", got[1])
	}
	if got := destination.Invocations[0].Next[:cap(destination.Invocations[0].Next)]; got[1] != "" {
		t.Fatalf("stale edge retained in spare capacity: %v", got)
	}
}

func TestCheckpointCloneCopiesEveryStateAndReportsFailures(t *testing.T) {
	input := Checkpoint[[]int]{
		Invocations: []Invocation[[]int]{{ID: "i1", State: []int{1}}},
		Terminals:   []Terminal[[]int]{{InvocationID: "i2", State: []int{2}}},
		Final:       ptr([]int{3}),
	}
	clone := func(state []int) ([]int, error) { return append([]int(nil), state...), nil }
	cloned, err := input.Clone(clone)
	if err != nil {
		t.Fatal(err)
	}
	cloned.Invocations[0].State[0] = 10
	cloned.Terminals[0].State[0] = 20
	(*cloned.Final)[0] = 30
	if input.Invocations[0].State[0] != 1 || input.Terminals[0].State[0] != 2 || (*input.Final)[0] != 3 {
		t.Fatalf("Clone shared state: %+v", input)
	}

	if _, err := input.Clone(nil); err == nil {
		t.Fatal("Clone accepted a nil copier")
	}
	wantErr := errors.New("copy failed")
	if _, err := input.Clone(func([]int) ([]int, error) { return nil, wantErr }); !errors.Is(err, wantErr) || !strings.Contains(err.Error(), `invocation "i1"`) {
		t.Fatalf("Clone error = %v", err)
	}
	if _, err := input.Clone(func([]int) ([]int, error) { panic(wantErr) }); !errors.Is(err, wantErr) {
		t.Fatalf("Clone panic = %v", err)
	}

	terminalOnly := Checkpoint[int]{Terminals: []Terminal[int]{{InvocationID: "i7", State: 7}}}
	if _, err := terminalOnly.Clone(func(int) (int, error) { return 0, wantErr }); !errors.Is(err, wantErr) || !strings.Contains(err.Error(), `terminal "i7"`) {
		t.Fatalf("terminal clone error = %v", err)
	}
	finalOnly := Checkpoint[int]{Final: ptr(8)}
	if _, err := finalOnly.Clone(func(int) (int, error) { return 0, wantErr }); !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "final state") {
		t.Fatalf("final clone error = %v", err)
	}
}

func TestCheckpointLookupAndIDAllocation(t *testing.T) {
	checkpoint := Checkpoint[int]{
		NextID:      12,
		Invocations: []Invocation[int]{{ID: "i1"}, {ID: "i2"}},
		Groups:      []ActivationGroup{{ID: "g3"}},
	}
	index, invocation := FindInvocation(&checkpoint, "i2")
	if index != 1 || invocation == nil || invocation.ID != "i2" {
		t.Fatalf("FindInvocation = %d, %+v", index, invocation)
	}
	if index, invocation = FindInvocation(&checkpoint, "missing"); index != -1 || invocation != nil {
		t.Fatalf("missing invocation = %d, %+v", index, invocation)
	}
	index, group := FindGroup(&checkpoint, "g3")
	if index != 0 || group == nil || group.ID != "g3" {
		t.Fatalf("FindGroup = %d, %+v", index, group)
	}
	if index, group = FindGroup(&checkpoint, "missing"); index != -1 || group != nil {
		t.Fatalf("missing group = %d, %+v", index, group)
	}
	if got := NewID(&checkpoint, "c"); got != "c12" || checkpoint.NextID != 13 {
		t.Fatalf("NewID = %q, next=%d", got, checkpoint.NextID)
	}
}

func TestCheckpointHeaderAndSharedValidators(t *testing.T) {
	valid := Checkpoint[int]{FormatVersion: CheckpointFormatVersion, RunID: "run", MachineID: "machine", Revision: 1}
	if err := ValidateCheckpointHeader(valid); err != nil {
		t.Fatalf("valid header rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Checkpoint[int]){
		"empty run":      func(value *Checkpoint[int]) { value.RunID = "" },
		"padded run":     func(value *Checkpoint[int]) { value.RunID = " run" },
		"empty machine":  func(value *Checkpoint[int]) { value.MachineID = "" },
		"padded machine": func(value *Checkpoint[int]) { value.MachineID = "machine " },
		"format version": func(value *Checkpoint[int]) { value.FormatVersion++ },
		"zero revision":  func(value *Checkpoint[int]) { value.Revision = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := valid
			mutate(&invalid)
			if err := ValidateCheckpointHeader(invalid); !errors.Is(err, ErrInvalidCheckpoint) {
				t.Fatalf("invalid header error = %v", err)
			}
		})
	}
	if ValidName("") || ValidName(" name") || ValidName("name ") || !ValidName("a b") {
		t.Fatal("ValidName does not match the whitespace contract")
	}
	if !ValidScope(FailInvocation) || !ValidScope(FailGroup) || !ValidScope(FailExecution) || ValidScope(FailureScope(99)) {
		t.Fatal("ValidScope does not match the supported failure scopes")
	}
}

func TestTransitionsCopyTargetsAndPreserveActions(t *testing.T) {
	targets := []string{"next"}
	continued := To(1, targets...)
	continued.Targets[0] = "changed"
	if targets[0] != "next" || continued.Action != ActionContinue {
		t.Fatalf("To transition = %+v, source targets=%v", continued, targets)
	}
	wait := Wait(2, "resume", "done")
	if wait.Action != ActionWait || wait.Continuation != "resume" || !reflect.DeepEqual(wait.Targets, []string{"done"}) {
		t.Fatalf("Wait transition = %+v", wait)
	}
	if EndBranch(3).Action != ActionEndBranch || EndExecution(4).Action != ActionEndExecution || Return(5).Action != ActionReturn {
		t.Fatal("terminal transition helper returned the wrong action")
	}
}

func TestErrorFormattingAndUnwrap(t *testing.T) {
	want := errors.New("cause")
	panicErr := &PanicError{Callback: "node work", InvocationID: "i1", Value: want, Stack: []byte("stack")}
	if !errors.Is(panicErr, want) || !strings.Contains(panicErr.Error(), "node work") || len(panicErr.Stack) == 0 {
		t.Fatalf("PanicError = %v", panicErr)
	}
	transitionErr := &TransitionError{InvocationID: "i2", Node: "next", Cause: want}
	if !errors.Is(transitionErr, want) || !strings.Contains(transitionErr.Error(), "next") {
		t.Fatalf("TransitionError = %v", transitionErr)
	}
}

func ptr[T any](value T) *T { return &value }
