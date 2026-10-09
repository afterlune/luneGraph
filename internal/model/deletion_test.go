package model

import (
	"reflect"
	"testing"
)

func TestDeletionIDsOwnedAndValidated(t *testing.T) {
	input := []string{"a", "b", "a"}
	ids, err := DeletionIDs(input)
	if err != nil || !reflect.DeepEqual(ids, []string{"a", "b"}) {
		t.Fatal(ids, err)
	}
	input[0] = "changed"
	if ids[0] != "a" {
		t.Fatal("caller slice retained")
	}
	if _, err := DeletionIDs([]string{"valid", " invalid "}); err == nil {
		t.Fatal("invalid ID accepted")
	}
}
