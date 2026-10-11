package model

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestChangeValidationAndOwnership(t *testing.T) {
	cp := Checkpoint[map[string]int]{FormatVersion: CheckpointFormatVersion, RunID: "r", MachineID: "m", Revision: 2, NextID: 3, Invocations: []Invocation[map[string]int]{{ID: "i1", State: map[string]int{"n": 1}, Next: []string{"next"}}}, Groups: []ActivationGroup{{ID: "g2", Children: []string{"i1"}}}}
	c := cp.Change()
	c.UpsertInvocations[0].Next[0] = "detached"
	c.UpsertGroups[0].Children[0] = "detached"
	if cp.Invocations[0].Next[0] != "next" || cp.Groups[0].Children[0] != "i1" {
		t.Fatal("checkpoint shared change structure")
	}
	c.UpsertInvocations[0].Next[0] = "next"
	c.UpsertGroups[0].Children[0] = "i1"
	c.DeleteInvocations = []string{"i9"}
	c.DeleteGroups = []string{"g9"}
	if err := ValidateChange(1, c); err != nil {
		t.Fatal(err)
	}
	owned, err := c.Clone(func(s map[string]int) (map[string]int, error) {
		out := make(map[string]int)
		for k, v := range s {
			out[k] = v
		}
		return out, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	c.UpsertInvocations[0].State["n"] = 9
	c.UpsertInvocations[0].Next[0] = "changed"
	c.UpsertGroups[0].Children[0] = "changed"
	c.DeleteInvocations[0] = "changed"
	c.DeleteGroups[0] = "changed"
	if owned.UpsertInvocations[0].State["n"] != 1 || owned.UpsertInvocations[0].Next[0] != "next" || owned.UpsertGroups[0].Children[0] != "i1" || owned.DeleteInvocations[0] != "i9" || owned.DeleteGroups[0] != "g9" {
		t.Fatal(owned)
	}
	if _, err := owned.Clone(func(s map[string]int) (map[string]int, error) { return s, errors.New("clone") }); err == nil {
		t.Fatal("clone failure ignored")
	}
	for _, mutate := range []func(*Change[map[string]int]){
		func(c *Change[map[string]int]) { c.Header.RunID = "" }, func(c *Change[map[string]int]) { c.Header.Revision = 3 },
		func(c *Change[map[string]int]) { c.Final = &map[string]int{} }, func(c *Change[map[string]int]) { c.Header.Failure = &Failure{} },
		func(c *Change[map[string]int]) { c.Header.Completed = true },
		func(c *Change[map[string]int]) { c.DeleteInvocations = append(c.DeleteInvocations, "i1") }, func(c *Change[map[string]int]) { c.DeleteGroups = append(c.DeleteGroups, "g2") },
		func(c *Change[map[string]int]) {
			c.UpsertInvocations = append(c.UpsertInvocations, c.UpsertInvocations[0])
		}, func(c *Change[map[string]int]) { c.UpsertGroups = append(c.UpsertGroups, c.UpsertGroups[0]) },
		func(c *Change[map[string]int]) { c.DeleteInvocations = []string{""} }, func(c *Change[map[string]int]) { c.DeleteGroups = []string{""} },
	} {
		bad, _ := owned.Clone(func(s map[string]int) (map[string]int, error) { return s, nil })
		mutate(&bad)
		if err := ValidateChange(1, bad); err == nil {
			t.Fatal("invalid delta accepted", bad)
		}
	}
	if !errors.Is(ValidateChange(math.MaxUint64, owned), ErrExecutionLimit) {
		t.Fatal("counter exhaustion")
	}
	before := Header{RunID: "r", MachineID: "m", Revision: 1, Steps: 2, NextID: 5, HadLocalFailures: true}
	after := before
	after.Revision++
	if err := ValidateAdvance(before, after); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Header){func(h *Header) { h.RunID = "other" }, func(h *Header) { h.Revision++ }, func(h *Header) { h.MachineID = "other" }, func(h *Header) { h.Steps = 1 }, func(h *Header) { h.NextID = 1 }, func(h *Header) { h.HadLocalFailures = false }} {
		bad := after
		mutate(&bad)
		if ValidateAdvance(before, bad) == nil {
			t.Fatal(bad)
		}
	}
	before.Completed = true
	if !errors.Is(ValidateAdvance(before, after), ErrRunCompleted) {
		t.Fatal("terminal advance accepted")
	}
	if !reflect.DeepEqual(owned.Checkpoint().Header(), owned.Header) {
		t.Fatal("header materialization changed")
	}
}
