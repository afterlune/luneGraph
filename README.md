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
    result, err := runner.Start(context.Background(), "run-1", 0, graph.Options[int]{})
    if err != nil { panic(err) }
    if result.Status != graph.StatusCompleted { panic(result.Status) }
    fmt.Println(*result.Checkpoint.Final) // 3
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

Clone must copy mutable maps, slices, pointers and other references. For example:

```go
type State struct { Values map[string]int; Data []byte }
func clone(s State) (State, error) {
    values := make(map[string]int, len(s.Values))
    for key, value := range s.Values { values[key] = value }
    s.Values = values
    s.Data = append([]byte(nil), s.Data...)
    return s, nil
}
```

ValueClone is for scalars/immutable state. JSONClone is convenient only when S
survives a semantics-preserving JSON round trip: unexported and ignored fields
are lost, and interface values can change type. Prefer manual Clone for hot
paths. Keep large content externally and store references in S.

## Waiting and durable execution

RegisterContinuation adapts a typed payload decoder and handler to persisted
bytes. RegisterJSONContinuation uses JSON. Wait names that continuation.
ResumeInput identifies a waiting invocation; all inputs decode before any Apply,
then accepted inputs commit individually. A later error preserves the accepted
prefix. Persist input outside the runtime until authoritative reload confirms
its pending CallID has advanced.

Start creates a run. Resume drives its supplied checkpoint, loading authoritative
state and rejecting stale references when a Store is supplied. Recover loads by
RunID. Fork creates an independent run from an active checkpoint, with a new
RunID and revision 1. Completed runs cannot be resumed or forked; Recover can
inspect terminal results without invoking callbacks.

```go
store, err := sqlite.Open(ctx, "runs.db", checkpoint.JSON[State]{})
if err != nil { return err }
defer store.Close()
result, err := runner.Start(ctx, "run-1", initial, graph.Options[State]{Store: store})
// Reopen with the same definition/MachineID in another process:
result, err = runner.Recover(ctx, "run-1", inputs, graph.Options[State]{Store: store})
```

Import `checkpoint` and `checkpoint/sqlite` from the module's corresponding
subpackages. SQLite requires an existing parent directory. Its StateCodec[S]
encodes S only; execution metadata is stored separately. Memory Store is in
checkpoint/memory and requires an independent-copy Clone.

Store.Commit(expectedRevision, Change[S]) atomically applies changed records.
Custom Stores implement Create, Load, Commit, Delete and DeleteMany. ErrConflict
guarantees no write; other Store errors can leave the outcome uncertain. Reload
before replay. CAS does not grant exclusive execution ownership.

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

**StatusFailed describes a failed call.** Inspect Checkpoint.Completed/Failure
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
options := graph.Options[State]{Store: store, MaxConcurrency: 8, Limiter: limiter}
```

Host applications still bound resident executions, waiting callers and memory.
Do not synchronously drive work requiring the permit held by the caller.

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
ownership. Replace Codec[S] with StateCodec[S]. JSON[S].Append/Unmarshal now
operate on S, not Checkpoint[S]; for complete checkpoint interchange instantiate
JSON[graph.Checkpoint[S]]. Add independent Merge commits to revision accounting
and replace OperationCompareAndSwap with OperationCommit. Handle the Builder
Mermaid error and Resume's ErrRunCompleted.

Formats 1/2/3 and SQLite schema 1 are rejected. Complete existing runs using the
old runtime and use a new database for format 4; there is no automatic migration.
See [CHANGELOG](CHANGELOG.md) for the complete source and semantic changes.
