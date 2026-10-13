package model

import "testing"

func TestReusableTransitionRemapsBranchesWithoutChangingCallerOrOtherMount(t *testing.T) {
	branches := []Branch[int]{{Target: "entry", State: 2}, {Target: "entry", State: 3}}
	tr := FanOut(1, branches...)
	branches[0].Target = "caller changed"
	a := ReadTransition(RemapTransition(tr, map[string]string{"entry": "a/entry"}, nil))
	b := ReadTransition(RemapTransition(tr, map[string]string{"entry": "b/entry"}, nil))
	original := ReadTransition(tr)
	if a.Branches[0].Target != "a/entry" || b.Branches[0].Target != "b/entry" || original.Branches[0].Target != "entry" || original.Branches[1].State != 3 {
		t.Fatalf("shared remapping: original=%+v a=%+v b=%+v", original, a, b)
	}
	unknown := ReadTransition(RemapTransition(Wait(1, "input", "missing"), nil, nil))
	if unknown.Targets[0] != "" || unknown.Continuation != "" {
		t.Fatalf("unknown local names escaped: %+v", unknown)
	}
}
