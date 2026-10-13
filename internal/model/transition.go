package model

import "time"

// Action describes the control flow produced by a node.
type Action uint8

const (
	ActionContinue Action = iota + 1
	ActionWait
	ActionEndBranch
	ActionEndExecution
	ActionReturn
	ActionFanOut
	ActionCompleteBranch
)

// Transition is an explicit control decision constructed with To, ToTargets,
// Wait, FanOut, CompleteBranch, EndBranch, EndExecution, or Return.
// Its zero value is invalid when returned without an error.
type Transition[S any] struct {
	data TransitionData[S]
}

// TransitionData is the internal view consumed by definition and executor.
// It is intentionally not exported by the root graph facade.
type TransitionData[S any] struct {
	State        S
	Action       Action
	Targets      []string
	Branches     []Branch[S]
	Continuation string
}

// Branch describes one runtime-created child invocation.
type Branch[S any] struct {
	Target string
	State  S
	// EntryTimeout overrides the target node timeout for each attempt of the
	// entry callback only. Zero inherits the node timeout; negative is invalid.
	EntryTimeout time.Duration
}

// To continues to the required target or fans out to additional declared targets.
func To[S any](state S, target string, additionalTargets ...string) Transition[S] {
	targets := make([]string, 1, len(additionalTargets)+1)
	targets[0] = target
	targets = append(targets, additionalTargets...)
	return Transition[S]{data: TransitionData[S]{State: state, Action: ActionContinue, Targets: targets}}
}

// ToTargets continues to the supplied runtime-generated targets. At least one
// target is required; an empty slice is rejected when the transition is applied.
func ToTargets[S any](state S, targets []string) Transition[S] {
	return Transition[S]{data: TransitionData[S]{State: state, Action: ActionContinue, Targets: append([]string(nil), targets...)}}
}

// Wait suspends this invocation for a continuation, then follows the required
// declared target or fans out to additional targets after Apply updates state.
func Wait[S any](state S, continuation, target string, additionalTargets ...string) Transition[S] {
	tr := To(state, target, additionalTargets...)
	tr.data.Action = ActionWait
	tr.data.Continuation = continuation
	return tr
}

// FanOut creates an activation group with one independent child per branch.
func FanOut[S any](parent S, branches ...Branch[S]) Transition[S] {
	return Transition[S]{data: TransitionData[S]{State: parent, Action: ActionFanOut, Branches: append([]Branch[S](nil), branches...)}}
}

// CompleteBranch returns this invocation's state to its activation group's Merge.
func CompleteBranch[S any](state S) Transition[S] {
	return Transition[S]{data: TransitionData[S]{State: state, Action: ActionCompleteBranch}}
}

// EndBranch finishes the current invocation without producing a result.
func EndBranch[S any]() Transition[S] {
	return Transition[S]{data: TransitionData[S]{Action: ActionEndBranch}}
}

// EndExecution finishes the whole run with state as its final result.
func EndExecution[S any](state S) Transition[S] {
	return Transition[S]{data: TransitionData[S]{State: state, Action: ActionEndExecution}}
}

// Return leaves the current subgraph and continues at its mount point.
func Return[S any](state S) Transition[S] {
	return Transition[S]{data: TransitionData[S]{State: state, Action: ActionReturn}}
}

// ReadTransition exposes a value view to internal consumers. S and slices are
// borrowed read-only; mutations belong in the model's constructors/remapping.
func ReadTransition[S any](tr Transition[S]) TransitionData[S] { return tr.data }

// RemapTransition qualifies local names without modifying the original slices.
func RemapTransition[S any](tr Transition[S], targets, continuations map[string]string) Transition[S] {
	d := &tr.data
	if d.Action != ActionContinue && d.Action != ActionWait && d.Action != ActionFanOut {
		return tr
	}
	if len(d.Targets) != 0 {
		d.Targets = append([]string(nil), d.Targets...)
		for i, target := range d.Targets {
			d.Targets[i] = targets[target]
		}
	}
	if d.Action == ActionFanOut && len(d.Branches) != 0 {
		d.Branches = append([]Branch[S](nil), d.Branches...)
		for i, branch := range d.Branches {
			d.Branches[i].Target = targets[branch.Target]
		}
	}
	if d.Action == ActionWait {
		d.Continuation = continuations[d.Continuation]
	}
	return tr
}
