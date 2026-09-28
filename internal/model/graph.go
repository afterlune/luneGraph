package model

import (
	"context"
	"strings"
)

// Action describes the control flow produced by a node.
type Action uint8

const (
	ActionContinue Action = iota + 1
	ActionWait
	ActionEndBranch
	ActionEndExecution
)

// Transition contains a node's new state and its explicit control decision.
type Transition[S any] struct {
	State        S
	Action       Action
	Targets      []string
	Continuation string
}

// To continues to one target or fans out to several declared targets.
func To[S any](state S, targets ...string) Transition[S] {
	return Transition[S]{State: state, Action: ActionContinue, Targets: append([]string(nil), targets...)}
}

// Wait suspends this invocation until a continuation receives resume input.
func Wait[S any](state S, continuation string, targets ...string) Transition[S] {
	return Transition[S]{State: state, Action: ActionWait, Targets: append([]string(nil), targets...), Continuation: continuation}
}

// EndBranch finishes the current invocation and records a terminal state.
func EndBranch[S any](state S) Transition[S] {
	return Transition[S]{State: state, Action: ActionEndBranch}
}

// EndExecution finishes the whole run with state as its final result.
func EndExecution[S any](state S) Transition[S] {
	return Transition[S]{State: state, Action: ActionEndExecution}
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
}

// Clone makes an independent copy of state before a user callback runs.
type Clone[S any] func(S) (S, error)

// Merge combines values that reached one activation group's join. CallInfo
// carries the activation group's stable join callback ID.
type Merge[S any] func(context.Context, CallInfo, []S) (S, error)

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
}

// JoinSpec consumes obligations created by fan-out at From. Its only possible
// successor is its sole outgoing edge; without one it ends the parent branch.
type JoinSpec[S any] struct {
	Name    string
	From    string
	Merge   Merge[S]
	OnError FailureScope
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
}

func ValidName(name string) bool {
	return name != "" && name == strings.TrimSpace(name)
}

func ValidScope(scope FailureScope) bool { return scope <= FailExecution }
