package model

import (
	"context"
	"strings"
	"time"
)

// NodeFunc performs one invocation and chooses its next control action. CallInfo
// carries a stable callback ID across recovery replays.
type NodeFunc[S any] func(context.Context, CallInfo, S) (Transition[S], error)

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
func (b BranchResult[S]) Resolved() bool { return b.Status.Resolved() }

// Pending reports whether the branch was still pending/running when the join was evaluated.
func (b BranchResult[S]) Pending() bool { return !b.Resolved() }

// MergeFunc combines branch outcomes that reached one activation group's join. CallInfo
// carries the activation group's stable join callback ID.
type MergeFunc[S any] func(context.Context, CallInfo, S, []BranchResult[S]) (S, error)

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
	Run     NodeFunc[S]
	OnError FailureScope
	// Timeout bounds each callback attempt after admission. Zero disables it.
	Timeout time.Duration
}

// JoinSpec merges branches from FanOutSource. It follows its sole outgoing
// edge after merging, or ends the parent branch when there is no edge.
type JoinSpec[S any] struct {
	Name         string
	FanOutSource string
	Merge        MergeFunc[S]
	OnError      FailureScope
	// Timeout bounds each Merge attempt after admission. Zero disables it.
	Timeout time.Duration
	Quorum  int
	// CanMerge is a fast, deterministic, side-effect-free predicate. It may be
	// called repeatedly as branch outcomes change. It receives cloned parent
	// and branch states; it must not mutate them or block. A panic is handled as
	// a join failure according to OnError.
	CanMerge func(parent S, branches []BranchResult[S]) bool
}

// Continuation adapts an untyped persisted payload to a typed state handler.
type Continuation[S any] struct {
	Decode func([]byte) (any, error)
	Apply  func(context.Context, CallInfo, S, any) (S, error)
}

// Config identifies a compiled graph and defines how state is copied.
type Config[S any] struct {
	DefinitionID string
	Clone        Clone[S]
}

// CompiledEdge holds a pre-resolved outgoing edge target and its node/join index.
type CompiledEdge struct {
	Target          string
	TargetNodeIndex int // index in CompiledGraph.Nodes, or -1
	TargetJoinIndex int // index in CompiledGraph.Joins, or -1
}

// CompiledNode stores the node specification, its fan-out join, return target,
// and the range of its outgoing edges.
type CompiledNode[S any] struct {
	Spec            NodeSpec[S]
	Index           int // index in CompiledGraph.Nodes
	FanOutJoinIndex int // index in CompiledGraph.Joins, or -1 if no join for this fanout source
	ReturnEdge      CompiledEdge
	Mounted         bool
	EdgeStart       int // start index in CompiledGraph.Edges
	EdgeEnd         int // end index in CompiledGraph.Edges
}

// CompiledJoin combines join specification and its unique outgoing edge target.
type CompiledJoin[S any] struct {
	Spec     JoinSpec[S]
	NextEdge CompiledEdge
}

// CompiledContinuation holds registered typed continuation with sorted key.
type CompiledContinuation[S any] struct {
	Key  string
	Cont Continuation[S]
}

// CompiledGraph is the immutable definition consumed by the executor.
type CompiledGraph[S any] struct {
	ID                string
	Entry             string
	EntryIndex        int
	Clone             Clone[S]
	Nodes             []CompiledNode[S]
	Edges             []CompiledEdge
	Joins             []CompiledJoin[S]
	Continuations     []CompiledContinuation[S]
	NodeIndex         map[string]int
	JoinIndex         map[string]int
	ContinuationIndex map[string]int
}

// ResolveTarget precomputes the indices for a named destination.
func (g CompiledGraph[S]) ResolveTarget(target string) CompiledEdge {
	edge := CompiledEdge{Target: target, TargetNodeIndex: -1, TargetJoinIndex: -1}
	if idx, ok := g.NodeIndex[target]; ok && target != "" {
		edge.TargetNodeIndex = idx
	}
	if idx, ok := g.JoinIndex[target]; ok && target != "" {
		edge.TargetJoinIndex = idx
	}
	return edge
}

func ValidName(name string) bool {
	return name != "" && name == strings.TrimSpace(name)
}

func ValidScope(scope FailureScope) bool { return scope <= FailExecution }
