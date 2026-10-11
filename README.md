# lune-graph

A strongly typed, resumable, durable graph execution runtime for Go. Graphs define
nodes and allowed edges; each execution owns its evolving state. The core
handles scheduling, branches, joins, waiting and recovery. Models, tools,
permissions and agent policy belong to applications.

**Current changes are unreleased.** Checkpoint format 4 and SQLite
schema 2 break compatibility with previous versions. See [CHANGELOG](CHANGELOG.md),
[execution contract](docs/execution-semantics.md), [Store guide](docs/store-implementation.md)
Requires Go 1.26.5 or newer.

The standard-library core is imported as `graph` from
`github.com/afterlune/luneGraph`. Optional SQLite persistence uses modernc.org/sqlite.
Implementation is separated into internal/definition, internal/executor,
internal/model and internal/observation. Persistence and logging are separate
packages. License: Apache-2.0.

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
    if err := g.AddNode(graph.NodeSpec[int]{
        Name: "count", OnError: graph.FailExecution,
        Run: func(_ context.Context, call graph.CallInfo, n int) (graph.Transition[int], error) {
            if n == 3 { return graph.EndExecution(n), nil }
            return graph.To(n+1, "count"), nil
        },
    }); err != nil { panic(err) }
    if err := g.AddEdge("count", "count"); err != nil { panic(err) }
    runner, err := g.Compile(graph.Config[int]{
        MachineID: "counter-v1", Clone: graph.ValueClone[int],
    })
    if err != nil { panic(err) }
    execution, result, err := runner.Start(context.Background(), "run-1", 0, nil, graph.Options{})
    if err != nil { panic(err) }
    if result.Status != graph.StatusCompleted { panic(result.Status) }
    fmt.Println(*result.Final) // 3
    checkpoint, err := execution.Checkpoint()
    if err != nil { panic(err) }
    fmt.Println(checkpoint.Steps) // 4
}
```

Compile copies the builder; subsequent builder mutations do not change its
Runner. Concurrent executions may share the Runner when callbacks and Clone are
concurrency safe. Treat returned checkpoints as immutable.

## Control flow and state

| Decision | Behavior |
| --- | --- |
| To(state, targets...) | Follow declared edges; multiple targets create an activation |
| Wait(state, continuation, targets...) | Commit state and await invocation input |
| EndBranch[S]() | End this path and discard its business result |
| EndExecution(state) | Finish the run, supply Final, cancel/drain other tasks |
| Return(state) | Leave a mounted subgraph through its sole exit |

Register JoinSpec with From naming the fan-out node. Each activation joins only
its own children. Merge receives joined states in target order; ended/failed
branches are omitted, so its inputs can be empty. Loops and nested groups work.
Merge runs independently: branch arrivals commit before Merge starts, and other
branches can advance while it runs. A rejected Merge replays independently with
its original CallID. Revision includes Merge commits; Steps counts node outcomes.

AddSubgraph mounts a Graph with the same S. Compile expands it into the same
machine/checkpoint, using qualified names such as `worker/review`. A mount has
zero/one exit. Return follows that exit or ends its invocation. Change MachineID
when a definition is incompatible with stored execution positions.

Clone must copy every mutable reference a callback can change, including values
inside containers. Copying a `map[string][]byte` alone still shares its byte
slices. The [runnable Clone example](clone_example_test.go) copies both levels
and preserves nil and non-nil empty values for its state type. Choose a Clone
that preserves your own state's required semantics. The same contract applies
to fan-out copies and Merge inputs; callbacks must not keep modifying returned
state after transferring its ownership to the runtime.

ValueClone is for scalars/immutable state. JSONClone is convenient only when S
survives a semantics-preserving JSON round trip: unexported and ignored fields
are lost, and interface values can change type. Prefer manual Clone for hot
paths. Keep large content externally and store references in S.

## Waiting and durable execution

RegisterContinuation adapts a typed payload decoder and handler to persisted
bytes. RegisterJSONContinuation uses JSON. Wait names that continuation.
ResumeInput identifies a waiting invocation; all inputs decode before any Apply,
then accepted inputs commit individually. A later error preserves the accepted
prefix. Keep input outside the runtime until the application has reconciled its
original waiting identity against authoritative state.

The [runnable partial-input example](resume_example_test.go) records RunID,
InvocationID, original waiting CallID and an owned payload. After an uncertain
commit, it loads the checkpoint and selects only inputs whose original waiting
identity is still pending. Unmatched records remain available for application
reconciliation; a changed CallID alone does not prove that a payload succeeded.
Terminal runs and an empty matching set do not receive inputs. `Resume` opens a
checkpoint as a new in-memory execution and checks its revision against the
Store. Return conflicts and uncertain errors to the caller for another
reconciliation decision; the example adds no automatic retry or acknowledgement.

This reconciliation path performs an explicit Load plus Resume's own full Load.
It can therefore materialize a full checkpoint twice. Callback side effects
still need application idempotency across concurrent replays.

Start, Resume, Recover and Fork return an `Execution` handle and the result of
that call. `Execution.Advance` continues the same live scheduler state; when a
Store is attached, it checks the persisted header revision before advancing.
`Execution.Checkpoint` creates an independent full checkpoint only when needed,
and `Execution.Pending` lists waiting continuation identities without copying
their state. Keep one handle on one goroutine; applications can still make
progress from another handle, but the Store's revision check will reject a stale
advance. Recover loads by RunID and is the cold-open path. Fork creates an
independent run from an active checkpoint, with a new RunID and revision 1.
Completed runs cannot be resumed or forked; Recover can inspect terminal results
without invoking callbacks.

```go
store, err := sqlite.Open(ctx, "runs.db", checkpoint.JSON[State]{})
if err != nil { return err }
defer store.Close()
execution, result, err := runner.Start(ctx, "run-1", initial, store, graph.Options{})
// Reopen with the same definition/MachineID in another process:
execution, result, err = runner.Recover(ctx, "run-1", inputs, store, graph.Options{})
// Within this process, continue the same live execution without reloading all states:
result, err = execution.Advance(ctx, moreInputs, graph.Options{})
```

Import `checkpoint` and `checkpoint/sqlite` from the module's corresponding
subpackages. SQLite requires an existing parent directory. Its StateCodec[S]
encodes S only; execution metadata is stored separately. Memory Store is in
checkpoint/memory and requires an independent-copy Clone.

Store.Commit(expectedRevision, Change[S]) atomically applies changed records.
Custom Stores implement Create, Load, LoadHeader, Commit and single-run Delete.
LoadHeader returns only revision and scheduling metadata for warm-handle conflict
checks; it must not materialize invocation states. ErrConflict
guarantees no write; other Store errors can leave the outcome uncertain. Reload
before replay. Load returns Invocations and Groups sorted by ascending
lexicographic ID; ActivationGroup.Children preserves fan-out target order. CAS
does not grant exclusive execution ownership. Multi-run cleanup is caller-owned;
repeated Delete calls do not form an atomic batch.

## Outcomes and effects

Nodes, Merge and Apply receive persisted CallInfo.CallID. Use namespace + RunID +
CallID for idempotency or effect reconciliation. Crash/cancellation can replay a
pending callback. This is at-least-once; external effects are application owned.
Clone and Decode should have no effects. [The effect example](examples/effects)
shows transactional receipts.

FailInvocation is the default: local failure can produce nil error and
StatusCompletedWithFailures. FailGroup removes its activation subtree; at the
root it becomes FailExecution. FailExecution records a terminal Failure; Recover
returns ErrRunFailed and its stored message. Invalid transitions, Clone errors
and cancellation do not create a failed terminal. Callback panics become
PanicError; Store/codec panics are outside this boundary.

**StatusFailed describes a failed call.** Inspect Result.Header.Completed/Failure
and reload after uncertain writes to determine execution lifetime. See the
[complete result handling table](docs/execution-semantics.md#failures-and-public-call-outcomes).

Interrupt(cause) exits with StatusInterrupted/ErrInterrupted and discards that
callback result, retaining its pending CallID. Accepted branch/input/inner-join
commits remain accepted. Reconcile the cause and Recover when appropriate.
Cancellation stops admission, cancels workers and waits for callbacks to return.

## Concurrency and observation

MaxSteps defaults to 10,000 node starts. MaxConcurrency defaults to GOMAXPROCS and
bounds nodes and joins together. Admitted tasks and ready joins drain after a
node budget is exhausted. Reuse a Limiter across calls/runners/state types to
bound all node, Merge and Apply callbacks in one process:

```go
limiter, err := graph.NewLimiter(64)
if err != nil { return err }
options := graph.Options{MaxConcurrency: 8, Limiter: limiter}
```

Host applications still bound resident executions, waiting callers and memory.
Do not synchronously drive work requiring the permit held by the caller.

For example, with one shared permit, a Node, Merge or Apply that synchronously
calls another Runner with the same Limiter holds the permit while waiting for
work that cannot acquire it. Passing context lets cancellation end this wait;
it does not make the dependency progress without cancellation. Two permits can
serve this single nested call, but do not guarantee progress for arbitrary
dependency chains or concurrent nested calls. The
[runnable Limiter example](limiter_example_test.go) expresses a dependency as
graph edges so callbacks return and release their permits before successors run.

Observer/ObserverFunc deliver synchronous concurrent metadata. Observers must
return promptly; delivery panics are isolated. Finished does not mean committed:
Resolved reports committed/discarded/unknown per callback. OperationCommit
observes delta submission. Nil observation allocates no session or ledger.
The observe package provides a slog adapter; events contain no application state
or input bytes, but errors can contain application text.

Builder.ExportMermaid returns `(string, error)` and rejects recursive mounts;
Runner.ExportMermaid returns its flattened topology as a string. Generated IDs
are deterministic and independent of node names, and labels are escaped.

## Examples and verification

- [Durable CLI](examples/durable): waiting and typed continuation across processes.
- [Effect receipts](examples/effects): effects, uncertain outcomes and replay.

```sh
gofmt -w .
go test ./...
go test -race ./...
go vet ./...
go test -coverpkg=./... -coverprofile=coverage.out ./...
go run ./internal/coveragecheck -profile coverage.out
```

Race needs CGO and a C compiler. Prefer a temporary coverage path. CI covers
Windows/Linux and enforces at least 85% statement coverage per production
package. [Performance and graph-level benchmark guide](docs/performance.md) records
reproducible measurements and known trade-offs; it does not claim application-level
production capacity.

## Format-4 migration

Replace custom Store.CompareAndSwap with Commit and preserve delta atomicity and
ownership. Remove Store.DeleteMany implementations; repeated Delete calls are
not atomic as a batch. Replace Codec[S] with StateCodec[S]. JSON[S].Append/Unmarshal now
operate on S, not Checkpoint[S]; for complete checkpoint interchange instantiate
JSON[graph.Checkpoint[S]]. Add independent Merge commits to revision accounting
and replace OperationCompareAndSwap with OperationCommit. Handle the Builder
Mermaid error and Resume's ErrRunCompleted.

Formats 1/2/3 and SQLite schema 1 are rejected. Complete existing runs using the
old runtime and use a new database for format 4; there is no automatic migration.
See [CHANGELOG](CHANGELOG.md) for the complete source and semantic changes.

The execution-handle API is a separate source migration: use non-generic
`graph.Options`, pass Store as the explicit argument to Start/Resume/Recover/Fork,
and capture their `(execution, result, error)` returns. Read `result.Header` and
`result.Final`; call `execution.Checkpoint()` for a full snapshot or reuse the
handle with `execution.Advance(...)` for another in-process call. Custom Store
implementations must add `LoadHeader` while leaving checkpoint encoding intact.
