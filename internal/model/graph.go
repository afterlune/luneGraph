package model

import (
	"context"
	"strings"
	"time"
)

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

// Transition contains a node's new state and its explicit control decision.
type Transition[S any] struct {
	State        S
	Action       Action
	Targets      []string
	Branches     []Branch[S]
	Continuation string
}

// Branch describes one runtime-created child invocation.
type Branch[S any] struct {
	Target  string
	State   S
	Timeout time.Duration
}

// To continues to one target or fans out to several declared targets.
func To[S any](state S, targets ...string) Transition[S] {
	return Transition[S]{State: state, Action: ActionContinue, Targets: append([]string(nil), targets...)}
}

// Wait suspends this invocation until a continuation receives resume input.
func Wait[S any](state S, continuation string, targets ...string) Transition[S] {
	return Transition[S]{State: state, Action: ActionWait, Targets: append([]string(nil), targets...), Continuation: continuation}
}

// FanOut creates an activation group with one independent child per branch.
func FanOut[S any](parent S, branches ...Branch[S]) Transition[S] {
	return Transition[S]{State: parent, Action: ActionFanOut, Branches: append([]Branch[S](nil), branches...)}
}

// CompleteBranch returns this invocation's state to its activation group's Merge.
func CompleteBranch[S any](state S) Transition[S] {
	return Transition[S]{State: state, Action: ActionCompleteBranch}
}

// EndBranch finishes the current invocation without producing a result.
func EndBranch[S any]() Transition[S] {
	return Transition[S]{Action: ActionEndBranch}
}

// EndExecution finishes the whole run with state as its final result.
func EndExecution[S any](state S) Transition[S] {
	return Transition[S]{State: state, Action: ActionEndExecution}
}

// Return leaves the current subgraph and continues at its mount point.
func Return[S any](state S) Transition[S] {
	return Transition[S]{State: state, Action: ActionReturn}
}

// Node performs one invocation and chooses its next control action. CallInfo
// carries a stable callback ID across recovery replays.
type Node[S any] func(context.Context, CallInfo, S) (Transition[S], error)

// CallInfo identifies one logical user callback within a run. CallID is
// persisted before the callback can start and remains stable if recovery
// replays that callback. Use RunID and CallID as an application deduplication
// key, with an application-specific namespace when needed.
type CallInfo struct {
	// RunID identifies the execution.
	RunID string
	// InvocationID identifies the path, which may visit several nodes.
	InvocationID string
	// CallID is unique within the run and stable across replay of this callback.
	CallID string
	// Node is the name of the vertex executing this callback.
	Node string
	// Step is the cumulative count of completed node starts before this callback.
	Step uint64
	// BranchIndex is the branch index within an activation group (0 for root/linear paths).
	BranchIndex int
}

// Clone makes an independent copy of state before a user callback runs.
type Clone[S any] func(S) (S, error)

// BranchResult describes the final outcome of one child branch in an activation group.
type BranchResult[S any] struct {
	Index  int
	Target string
	Node   string
	Status InvocationStatus
	State  S
	Error  string
}

// OK reports whether the branch completed successfully and produced a valid State.
func (b BranchResult[S]) OK() bool { return b.Status == InvocationJoined }

// Failed reports whether the branch failed. Error contains the failure message.
func (b BranchResult[S]) Failed() bool { return b.Status == InvocationFailed }

// Ended reports whether the branch ended without producing a result.
func (b BranchResult[S]) Ended() bool { return b.Status == InvocationEnded }

// Resolved reports whether the branch reached a terminal status (joined, failed, or ended).
func (b BranchResult[S]) Resolved() bool { return b.OK() || b.Failed() || b.Ended() }

// Pending reports whether the branch was still pending/running when the join was evaluated.
func (b BranchResult[S]) Pending() bool { return !b.Resolved() }

// Merge combines branch outcomes that reached one activation group's join. CallInfo
// carries the activation group's stable join callback ID.
type Merge[S any] func(context.Context, CallInfo, S, []BranchResult[S]) (S, error)

// FailureScope controls how a node or join error affects an execution.
type FailureScope uint8

const (
	FailInvocation FailureScope = iota
	FailGroup
	FailExecution
)

// NodeSpec registers a node and its default failure policy.
type NodeSpec[S any] struct {
	Name    string
	Run     Node[S]
	OnError FailureScope
	Timeout time.Duration
}

// JoinSpec consumes obligations created by fan-out at From. Its only possible
// successor is its sole outgoing edge; without one it ends the parent branch.
type JoinSpec[S any] struct {
	Name     string
	From     string
	Merge    Merge[S]
	OnError  FailureScope
	Timeout  time.Duration
	Quorum   int
	CanMerge func(parent S, branches []BranchResult[S]) bool
}

// Continuation adapts an untyped persisted payload to a typed state handler.
type Continuation[S any] struct {
	Decode func([]byte) (any, error)
	Apply  func(context.Context, CallInfo, S, any) (S, error)
}

// Config identifies a compiled graph and defines how state is copied.
type Config[S any] struct {
	MachineID string
	Clone     Clone[S]
}

// Machine is the immutable definition consumed by the executor.
type Machine[S any] struct {
	ID            string
	Entry         string
	Clone         Clone[S]
	Nodes         map[string]NodeSpec[S]
	Joins         map[string]JoinSpec[S]
	JoinBySource  map[string]string
	Edges         map[string]map[string]struct{}
	Continuations map[string]Continuation[S]
	ReturnTargets map[string]string
}

func ValidName(name string) bool {
	return name != "" && name == strings.TrimSpace(name)
}

func ValidScope(scope FailureScope) bool { return scope <= FailExecution }
