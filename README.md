# lune-graph

`lune-graph` is a typed, resumable and durable graph execution runtime for Go. A graph defines nodes and allowed edges. Each execution owns an evolving state and can loop, branch, wait for input, and resume from a checkpoint. The runtime defines execution semantics; applications define the state and the meaning of each node. The core uses the Go standard library; the optional SQLite store uses `modernc.org/sqlite`.

The public API stays in the `graph` package, imported from `github.com/afterlune/luneGraph`. Its implementation is organized under `internal/model`, `internal/definition`, `internal/executor`, and `internal/observation`; persistence APIs and stores live in `checkpoint`, `checkpoint/memory`, and `checkpoint/sqlite`. The optional `observe` package provides a standard-library logging adapter.

The [execution contract](docs/execution-semantics.md) specifies commit points, crash recovery, Store errors, and the boundary between graph state and external side effects. LLMs, agents, and data modalities are application concerns built on this runtime.

The project's performance, concurrency, and reliability standards and benchmark commands are documented in [docs/performance.md](docs/performance.md).

This project is licensed under the [Apache License, Version 2.0](LICENSE).

## Install

Requires Go 1.26.5 or newer. Add the module to your Go project:

```sh
go get github.com/afterlune/luneGraph@latest
```

## A small graph

```go
package main

import (
	"context"
	"fmt"

	graph "github.com/afterlune/luneGraph"
)

func main() {
	g := graph.New[int]("count")
	err := g.AddNode(graph.NodeSpec[int]{
		Name: "count",
		Run: func(_ context.Context, call graph.CallInfo, n int) (graph.Transition[int], error) {
			// Use call.RunID and call.CallID for application-side deduplication.
			if n == 3 {
				return graph.EndExecution(n), nil
			}
			return graph.To(n+1, "count"), nil
		},
	})
	if err != nil { panic(err) }
	if err := g.AddEdge("count", "count"); err != nil { panic(err) }
	runner, err := g.Compile(graph.Config[int]{
		MachineID: "counter-v1",
		Clone:     graph.ValueClone[int],
	})
	if err != nil { panic(err) }
	result, err := runner.Start(context.Background(), "run-1", 0, graph.Options[int]{})
	if err != nil { panic(err) }
	fmt.Println(*result.Checkpoint.Final) // 3
}
```

`Graph[S]` is a builder. `Compile` copies its definition into a `Runner[S]`; later builder changes do not affect that runner. Subgraphs are expanded into the same compiled machine and checkpoint. The same runner may serve concurrent executions if node, continuation, merge, and clone functions are safe to call concurrently. Node, join, and continuation callbacks receive a `CallInfo` with the run ID, invocation ID, persisted callback ID, executing node name (`Node`), monotonically increasing step count (`Step`), and activation branch index (`BranchIndex`).

Nodes return one of five explicit decisions:

- `To(state, targets...)` continues to one edge or fans out to several edges.
- `Wait(state, continuation, targets...)` suspends that invocation until it receives input.
- `EndBranch[S]()` ends one path without returning or retaining a business result.
- `EndExecution(state)` ends the entire execution, cancels other active invocations, and sets `Checkpoint.Final`.
- `Return(state)` leaves the current subgraph and follows the edge leaving its mount point.

## Subgraphs

`AddSubgraph(name, child)` mounts another `Graph[S]` under a vertex name. The child uses the same state type, and the compiled runner's `Clone` function applies to its nodes and branches. Edges entering the mount name enter the child's configured entry. Add zero or one outgoing edge from the mount name; that edge is the continuation for `Return(state)`. A mounted graph without an outgoing edge ends the current branch when it returns. `EndBranch` ends its current invocation and `EndExecution` ends the whole run, as they do in the parent graph. A `Return` from a node outside a mounted graph is an invalid transition.

Compilation recursively expands nested mounts. Child node, join, and continuation names are qualified by their mount path, such as `worker/review` or `worker/review/approved`. Node transitions and wait continuations use names from their own graph definition; the compiler resolves them to these qualified names. Checkpoints therefore store qualified node and continuation names. Keep the same graph definition and `MachineID` when recovering a run; change the ID when a subgraph change makes the definition incompatible with saved checkpoints.

```go
child := graph.New[int]("work")
if err := child.AddNode(graph.NodeSpec[int]{Name: "work", Run: func(_ context.Context, _ graph.CallInfo, n int) (graph.Transition[int], error) {
	return graph.Return(n + 1), nil
}}); err != nil {
	panic(err)
}

parent := graph.New[int]("start")
if err := parent.AddNode(graph.NodeSpec[int]{Name: "start", Run: func(_ context.Context, _ graph.CallInfo, n int) (graph.Transition[int], error) {
	return graph.To(n, "worker"), nil
}}); err != nil {
	panic(err)
}
if err := parent.AddNode(graph.NodeSpec[int]{Name: "after", Run: func(_ context.Context, _ graph.CallInfo, n int) (graph.Transition[int], error) {
	return graph.EndExecution(n), nil
}}); err != nil {
	panic(err)
}
if err := parent.AddSubgraph("worker", child); err != nil {
	panic(err)
}
if err := parent.AddEdge("start", "worker"); err != nil {
	panic(err)
}
if err := parent.AddEdge("worker", "after"); err != nil {
	panic(err)
}
```

Targets must be declared with `AddEdge`. A node's ordinary error follows its `NodeSpec.OnError` policy: `FailInvocation` (the default), `FailGroup`, or `FailExecution`. A run can override that policy with `Options.FailureOverride`. `FailGroup` removes its activation group and nested descendants, cancels running callbacks in that subtree, and marks the group's parent invocation failed. Enclosing groups continue to settle under their own join rules. At the root, `FailGroup` escalates to an execution failure. Local failures set `Checkpoint.HadLocalFailures` and do not become a top-level error; their details are available through transient observer events. `FailExecution` commits a failed terminal checkpoint and returns the original error; `Recover` later returns `StatusFailed`, the stored checkpoint, and `ErrRunFailed` with its recorded message. Panics from nodes, joins, continuation handlers, decoders, and state cloning become `*PanicError`; execution-level panics retain their stack in `Checkpoint.Failure.PanicStack`; local panic details are available through observer events. Invalid actions, edges, or joins return `*TransitionError` regardless of failure policy. Clone errors and invalid transitions return the last committed checkpoint. Store panics are outside this callback boundary.

## Parallel branches and joins

Each fan-out creates a new activation group. The supplied `Clone` function gives each branch an independent state. Register a `JoinSpec[S]` with `From` set to the fan-out node and a `Merge` function, then connect branch paths to that join with edges. The join waits until every branch in that activation group has reached it, ended, or failed. `Merge` receives only the states that reached the join, in target order; it may receive an empty slice. A join may have one outgoing edge or end the parent branch. Groups may be nested, and loops can create new generations of the same group.

`Clone` is required even for a graph without fan-out. The runner clones state before calling user code so an in-place mutation followed by an error cannot change an earlier checkpoint. `Clone` must copy mutable maps, slices, pointers, and other referenced data that callbacks may modify. Use `graph.ValueClone[S]` for scalar or immutable states. Use `graph.JSONClone[S]` for arbitrary structs when a manual deep-copy function is not implemented. Ended branch states are discarded when their activation settles. To return business results, route branches through a join and continue to `EndExecution(state)`, or persist results in application storage.

## Pausing, resuming, and storing

Register each continuation with `RegisterContinuation`, or `RegisterJSONContinuation` for JSON payloads. Its decoder turns a `[]byte` resume payload into a concrete Go type, and its typed handler applies that value to the waiting state. `Wait` records the continuation key and allowed next targets. `Resume(ctx, checkpoint, []ResumeInput{{InvocationID: id, Payload: data}}, options)` applies input to the addressed invocation; other waiting invocations may remain paused. All supplied payloads are decoded before any input is applied. A decode error leaves the checkpoint unchanged; accepted inputs are then committed one at a time. If a later input fails under `FailExecution`, the failure terminal is committed after the accepted prefix and clears active invocations. Passing no inputs advances ready invocations after a step budget was exhausted.

`Start`, `Resume`, and `Recover` return a `Result[S]` containing a `Checkpoint[S]`. The checkpoint holds invocation positions, activation groups, a local-failure flag, an optional execution-level failure, a revision, and cumulative completed steps. Completed checkpoints contain no invocations or groups. Natural completion may have neither `Final` nor `Failure`; only `EndExecution(state)` supplies a final state. Treat it as immutable. Checkpoints carry `FormatVersion` (`CheckpointFormatVersion` is currently 3); `Resume` and `Recover` reject incompatible or malformed checkpoints with `ErrInvalidCheckpoint`. Version-1 and version-2 checkpoints are not upgraded automatically and must be completed with the old runtime or migrated by the application. `MaxSteps` defaults to 10,000 node starts per call and can be overridden; exhausting it returns `StatusBudget`. `MaxConcurrency` defaults to `GOMAXPROCS(0)`.

### Shared callback concurrency

`MaxConcurrency` limits node concurrency within one public call. Use a shared
`Limiter` to bound node, join Merge, and continuation Apply callbacks across
executions, compiled runners, state types, and stores in the same process:

```go
limiter, err := graph.NewLimiter(32)
if err != nil {
    return err
}
options := graph.Options[State]{MaxConcurrency: 8, Limiter: limiter, Store: store}
// Reuse limiter in every participating execution's Options.
```

Capacity must be positive; a non-nil zero-value Limiter is invalid. Capacity is
fixed; share the pointer and do not copy the Limiter. A nil Limiter uses only
the existing local concurrency limit. Waiting responds to context cancellation
and does not consume the step budget or change the pending CallID. The scheduler
continues handling running results while waiting for capacity, and creates
workers only when it schedules tasks. Permits are released after callback
observation and before checkpoint submission. Clone, Decode, Store and Observer
do not acquire their own permits; node state cloning happens after admission.

The budget bounds callbacks, rather than execution admission, waiting callers,
or all process goroutines. There is no FIFO, cross-process budget, run lease, or
automatic retry. Host applications manage queues and fairness. A callback that
synchronously drives another execution must avoid waiting on a shared budget
already occupied by itself. Compiled subgraphs keep using the same execution.

Ready invocations receive starts in round-robin ID order, tracked by `ScheduleCursor` across resumes. Concurrent results are committed in the order the scheduler receives them, so competing `EndExecution` results depend on completion timing. Cancellation waits for running callbacks to return. If a revision, step, or ID counter cannot advance, the runner returns `ErrExecutionLimit` with `StatusFailed` and the last committed checkpoint.

### Recoverable interruption

A node, join, or continuation handler can return `graph.Interrupt(cause)` when it needs the caller to defer further execution, for example while confirming a remote effect's outcome:

```go
return graph.Transition[State]{}, graph.Interrupt(errConfirmationPending)
```

The runner returns `StatusInterrupted`, an error matching `ErrInterrupted`, and the last committed checkpoint. `errors.Is` and `errors.As` can also inspect the cause; `Interrupt(nil)` requests interruption without a cause. Failure policies do not apply to this returned control signal. The callback's result and state changes are discarded, with no new checkpoint revision, step, or failure marker. Pending callbacks retain their original `CallID` for replay. Parallel interruption cancels other running callbacks, waits for their return, and discards their uncommitted results; callbacks must respond to context cancellation.

`Wait` commits state and waits for continuation input. Interruption instead exits this public call without committing the current candidate. A join shares a commit with the result that triggered it, so an interrupted join also discards that result; nested joins in the same candidate may replay too. An interrupted continuation remains waiting: resubmit its input and any later uncommitted inputs; earlier committed inputs remain accepted.

The caller decides when to invoke `Recover` or `Resume` again. There is no persisted interruption flag or reason, automatic retry, or unlock operation. External effects still require application idempotency or reconciliation using the run and call identity; interruption does not guarantee exactly-once effects. Clone, Decode, and Store errors retain their existing semantics, and panicking with an interruption error remains a panic. If the public call's context is cancelled when interruption is handled, cancellation takes priority.

This API adds the `"interrupted"` result status. Callers that exhaustively handle statuses must add a branch; checkpoint format 3 and Store interfaces are unchanged. See the interruption recovery tests in `examples/effects` for node, continuation, and join receipt replay.

An optional `graph.Store[S]` provides `Create`, `Load`, revision-based `CompareAndSwap`, and `Delete` (which cleans up all checkpoints of a run, returning `checkpoint.ErrNotFound` if missing). `checkpoint/memory` provides a concurrent in-memory implementation. `checkpoint/sqlite` persists the latest checkpoint for each run in a local SQLite file and supports CAS across processes on the same machine. `checkpoint.Codec[S]` appends an encoding of the complete checkpoint to a caller-provided byte slice and decodes it; `checkpoint.JSON[S]` uses `encoding/json`, so the state type must survive a JSON round trip. `Append` must not retain the destination or returned slice. Applications with other state types can implement the codec interface. This is a source-breaking change for custom codecs: replace `Marshal(value)` with `Append(dst, value)` and append the encoded bytes to `dst`. SQLite requires the database file's parent directory to exist.

To resume a budgeted run after reopening the database, reconstruct the same graph and compile it with the same `MachineID`:

```go
ctx := context.Background()
store, err := sqlite.Open(ctx, "runs.db", checkpoint.JSON[int]{})
if err != nil { panic(err) }
_, err = runner.Start(ctx, "run-2", 0, graph.Options[int]{Store: store, MaxSteps: 1})
if err != nil { panic(err) }
if err := store.Close(); err != nil { panic(err) }

// The following calls can run in a new process with the same graph definition.
store, err = sqlite.Open(ctx, "runs.db", checkpoint.JSON[int]{})
if err != nil { panic(err) }
defer store.Close()
result, err := runner.Recover(ctx, "run-2", nil, graph.Options[int]{Store: store})
if err != nil { panic(err) }
fmt.Println(*result.Checkpoint.Final) // 3
```

When configured, the runner saves the initial checkpoint and each accepted transition, including an execution-level failure. `Recover` loads the latest checkpoint by run ID and can return an already completed result without running callbacks or changing its revision. A stored execution-level failure returns `ErrRunFailed`; the original Go error type is available only to the call that produced it. Supplying inputs to any completed run returns its checkpoint with `StatusFailed` and `ErrRunCompleted`; the inputs are not consumed. Concurrent recoveries may race: a losing commit returns `ErrConflict` without an automatic retry. `Resume` instead uses its supplied checkpoint as a run ID, machine ID, format, and revision reference; it rejects a stale reference and an already completed run. Stores must reject stale revisions with `graph.ErrConflict`, which guarantees no write. Another storage error can leave the commit outcome uncertain: use `Recover` or load the latest checkpoint before retrying. Keep `MachineID` stable for one graph definition and change it when that definition becomes incompatible with existing checkpoints.

`runner.Fork(ctx, newRunID, checkpoint, options)` creates an independent execution starting from an active (non-terminal) checkpoint without mutating the source run. When a store is configured, it persists the branched state as revision 1 via `Create` and continues execution under the new run ID, enabling speculative exploration, backtracking, or trial branches.

Running nodes remain pending in persisted checkpoints until their result is committed. A crash, cancellation, or save failure can therefore cause a callback to run again after recovery. Each node execution, continuation application, and join merge receives a `CallInfo.CallID` that is committed before the callback can start and reused if that logical callback is replayed. New loop visits, continuation applications, and fan-out joins receive new IDs. Use an application-specific namespace together with `RunID` and `CallID` as a deduplication key. This supports at-least-once handling; it does not make external side effects exactly once. Clone functions and continuation decoders should not perform side effects. Cancellation asks running nodes to stop through `context.Context` and waits for them to return.

## Run the durable example

From a checkout of this repository, run these commands in separate processes:

```sh
go run ./examples/durable start -db runs.db -run demo
# status=waiting run=demo
go run ./examples/durable resume -db runs.db -run demo -value 3
# status=completed run=demo value=3
```

The [example](examples/durable) uses a caller-defined struct state and a typed integer continuation. `start` persists a waiting invocation and exits. `resume` opens the same SQLite database, finds the waiting invocation, and calls `Recover` with its input. Use a new run ID to start another execution; starting the same run twice returns a conflict. The database's parent directory must already exist.

## Application-owned effect receipts

The [effect example](examples/effects) uses a typed counter state and two
independent SQLite files: graph checkpoints and application effects.

```sh
go run ./examples/effects start -checkpoints runs.db -effects effects.db -run first -delta 3
# status=completed run=first value=3
go run ./examples/effects start -checkpoints runs.db -effects effects.db -run second -delta 4
# status=completed run=second value=7
go run ./examples/effects recover -checkpoints runs.db -effects effects.db -run first
# status=completed run=first value=3
```

Use fresh files for these outputs. The application's transaction updates its
counter and saves a receipt keyed by namespace, RunID and CallID. A replay
returns the original result, even after other requests advance the counter.
`recover` loads the latest checkpoint; a completed run returns its saved final
state without another callback. The example's process-crash test kills a child
after the effect commits and before its checkpoint commits, then verifies
recovery replays the callback without another increment.

This is application-side deduplication of a local transactional effect, not a
transaction spanning the graph Store or an exactly-once guarantee for remote
effects. See [recovery and external effects](docs/execution-semantics.md#recovery-and-external-effects).

## Execution observation

Set `Options.Observer` to receive transient events for public runner calls, node and join callbacks, continuation decoding and application, and actual Store calls. The observer receives metadata without application state or resume bytes. With a nil observer, the runtime creates no observation session, operation ID, or timestamps.

```go
import (
    "log/slog"
    "os"

    graph "github.com/afterlune/luneGraph"
    "github.com/afterlune/luneGraph/observe"
)

logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))
options := graph.Options[int]{Observer: observe.NewSlog(logger)}
result, err := runner.Start(ctx, "observed-run", 0, options)
```

`graph.ObserverFunc` also adapts a function taking `(context.Context, graph.Event)`. Delivery is synchronous and may be concurrent, so observers must be concurrency safe and return promptly. A slow observer adds latency and may affect the completion order of parallel callbacks. Each observer panic is isolated; graph failure policies and checkpoint commit points remain unchanged. The logging adapter reports public calls at Info, callbacks and Store calls at Debug, and errors at Error. A nil logger uses `slog.Default()`.

An `OperationID` correlates one `Start`, `Resume`, or `Recover` call within the current process. `RunID` and persisted `CallID` identify logical node, join, and continuation-apply callbacks across replay; decode events have no CallID. Nodes and continuations in subgraphs use qualified names. A finished callback event reports what that callback returned before routing validation or persistence. For write events, `Revision` is the attempted revision and any Store error may mean the commit outcome is uncertain, except `ErrConflict`, which guarantees no write. Use the Store to confirm recovery state. Events are best effort, are not stored, and can be incomplete after a crash. Error messages remain application-provided and may contain user data.

Node, join and continuation Apply callbacks also emit `PhaseResolved` with
`Event.Outcome`: `OutcomeCommitted`, `OutcomeDiscarded`, or `OutcomeUnknown`.
This resolves the callback's result, including handled failures; it does not
classify external side effects. All callbacks in one candidate, including its
trigger and nested joins, share the submission outcome. With no Store,
committed means accepted into the in-memory checkpoint. Interruption, invalid
transitions, superseded results, cancellation before submission, and CAS
`ErrConflict` discard results. Other errors returned by CAS, including context
errors, leave the submission outcome unknown. Reload the Store to confirm it.

A resolved event retains callback identity and Action, has zero Duration, and
reports the target Revision when commit was entered or the callback's base
Revision otherwise. Its Err describes rejection or uncertain submission; the
original callback error remains on the finished event. Every started execution
callback gets one resolved event before a normally returning public call
finishes. Crashes and Store panics can leave this sequence incomplete. Consumers
must handle `PhaseStarted`, `PhaseFinished` and `PhaseResolved` explicitly;
a non-started event is no longer necessarily a callback finish. Resolution
events remain transient and do not provide a durable event log.


Add `-observe` to either durable example command to write JSON events to stderr. Its result line remains on stdout:

```sh
go run ./examples/durable start -db runs.db -run observed -observe
go run ./examples/durable resume -db runs.db -run observed -value 3 -observe
```

The [execution contract](docs/execution-semantics.md#observation) defines event ordering, revision fields, and timing boundaries.

## Graph visualization

Both `Graph[S]` and compiled `Runner[S]` provide `ExportMermaid()` to export Mermaid flowchart representations of graph topology:

```go
builderDiagram := g.ExportMermaid()       // Hierarchical builder topology with mounted subgraphs
runnerDiagram  := runner.ExportMermaid()  // Flattened execution machine with expanded nodes and return targets
```

## API migration

The module path is now `github.com/afterlune/luneGraph`. Replace imports of `lune-graph` and its subpackages with the GitHub path. The root package remains named `graph`; this import migration does not change checkpoint format or require a new `MachineID`.

`Snapshot[S]`, `Result.Snapshot`, `ErrInvalidSnapshot`, and `SnapshotFormatVersion` are now `Checkpoint[S]`, `Result.Checkpoint`, `ErrInvalidCheckpoint`, and `CheckpointFormatVersion`. Callback signatures now include `graph.CallInfo`; update node, join, and continuation handlers. Checkpoint format 3 rejects version-1 snapshots and version-2 checkpoints, which need an application-managed migration or the old runtime. Import the in-memory store from `github.com/afterlune/luneGraph/checkpoint/memory`; missing runs return `checkpoint.ErrNotFound`.

### Checkpoint format 3 migration

Replace `EndBranch(state)` with `EndBranch[S]()`. `Terminal[S]` and `Checkpoint.Terminals` have been removed. Route required branch results through joins and `EndExecution`, or application storage. Replace `Checkpoint.Failures` with `HadLocalFailures` for local-failure status and `Failure *Failure` for execution failure. `Failure.Scope` has been removed because persisted failure details always describe an execution-level failure. Update custom codecs to preserve both new fields. Capture detailed local errors through an Observer or application receipts; checkpoints no longer serve as an event archive. Completed checkpoints have empty invocation and group collections, including successful natural completion without a final result. SQLite uses the existing table schema; checkpoint payload format compatibility still applies.
