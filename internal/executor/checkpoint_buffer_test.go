package executor

import (
	"reflect"
	"testing"
)

func TestClearCheckpointStateValuesRetainsOnlyStructure(t *testing.T) {
	final := []int{3}
	checkpoint := Checkpoint[[]int]{
		Invocations: []Invocation[[]int]{{ID: "i1", State: []int{1}, Next: []string{"next"}}},
		Groups:      []ActivationGroup{{ID: "g1", Children: []string{"i1"}}},
		Final:       &final,
	}
	clearCheckpointStateValues(&checkpoint)
	if checkpoint.Invocations[0].State != nil || checkpoint.Final != nil {
		t.Fatalf("state references remain in spare checkpoint: %+v", checkpoint)
	}
	if !reflect.DeepEqual(checkpoint.Invocations[0].Next, []string{"next"}) || !reflect.DeepEqual(checkpoint.Groups[0].Children, []string{"i1"}) {
		t.Fatalf("checkpoint structure was cleared: %+v", checkpoint)
	}
}
