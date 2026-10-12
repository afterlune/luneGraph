# lune-graph

A strongly typed, resumable, durable graph execution runtime for Go. Graphs define
nodes and allowed edges; each execution owns its evolving state. The core
handles scheduling, branches, joins, waiting and recovery. Models, tools,
permissions and agent policy belong to applications.

**Current changes are unreleased.** Checkpoint format 7 is incompatible with
earlier checkpoint formats; SQLite schema 2 remains in use. See [CHANGELOG](CHANGELOG.md),
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
| FanOut(parent, branches...) | Create a dynamic zero-or-more task activation using declared targets |
| CompleteBranch(state) | Return this task's result to its current group's Merge |
| Wait(state, continuation, targets...) | Commit state and await invocation input |
| EndBranch[S]() | End this path and discard its business result |
| EndExecution(state) | Finish the run, supply Final, cancel/drain other tasks |
| Return(state) | Leave a mounted subgraph through its sole exit |

Register JoinSpec with From naming a FanOut source. Each activation joins only
its own children. FanOut branches can repeat targets and carry independent
states; zero and one branch still pass through a normal activation and Merge.
Merge receives a clone of the state saved by FanOut, then ordered branch
outcomes as `[]BranchResult[S]`. Each `BranchResult` reports `Index`, `Target`,
`Node`, `Status`, `State`, and `Error` (with helpers `b.OK()`, `b.Failed()`,
`b.Ended()`). Healthy values can be extracted directly using `BranchValues(branches)`.
This gives parent and Lead nodes full visibility into partial branch failures and
ended tasks, supporting tolerant aggregation. Workers can dispatch nested tasks
from the same declared graph; CompleteBranch returns to their immediate activation
group without storing the caller's join name in state.
Merge runs independently: branch arrivals commit before Merge starts, and other
branches can advance while it runs. A rejected Merge replays independently with
its original CallID. Revision includes Merge commits; Steps counts node outcomes.

Nodes, joins and dynamic branches support declarative timeouts via
`NodeSpec.Timeout`, `JoinSpec.Timeout` and `Branch.Timeout` (`time.Duration`).
When configured, the runtime derives a context with `context.WithTimeout`
for the callback execution. If a deadline expires, `context.DeadlineExceeded`
is recorded. Under `OnError: FailInvocation`, the failure is isolated to that
branch and stored on its activation record, allowing sibling branches and
tolerant joins to proceed. Dynamic branch timeouts override the
target node's default timeout for the initial step, then clear upon single-target
routing so subsequent nodes use their own configured timeouts.

Speculative joins and branch pruning are supported via `JoinSpec.Quorum` (integer
threshold of successful branches) or `JoinSpec.CanMerge` (predicate over parent and
available branch results). When met, `Merge` runs early; `BranchResult.Resolved()`
reports whether a branch finished, while `BranchResult.Pending()` identifies
uncompleted tasks. Upon join commit, remaining running and pending branches in the
group are cancelled and pruned. If quorum cannot be met due to branch failures, the
join safely falls back to standard execution when all branches settle.

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
Nil interface payloads and typed nil values retain their decoded Go semantics.
ResumeInput identifies a specific wait using InvocationID and its original
waiting CallID; both fields are required. Empty or mismatched CallIDs, duplicate
invocations and non-waiting addresses return ErrInvalidResumeInput (check with
errors.Is) with StatusFailed, without an execution-level Failure or an input
commit. All inputs decode before any Apply; accepted inputs then commit
individually. A later error preserves the accepted
prefix. Keep input outside the runtime until the application has reconciled its
original waiting identity against authoritative state.

Capture the identity when the input is created, and reuse that identity on retry:

```go
waiting := execution.Pending()[0] // Select the intended waiting invocation.
input := graph.ResumeInput{
    InvocationID: waiting.InvocationID,
    CallID: waiting.CallID,
    Payload: payload,
}
result, err := execution.Advance(ctx, []graph.ResumeInput{input}, graph.Options{})
```

Never replace an old input's CallID with the latest waiting CallID. Identity
checks prevent rebinding to another wait; they do not grant exclusive execution
or acknowledge external input delivery.

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
lexicographic ID; ActivationGroup.Branches preserves fan-out order and its
metadata. CAS
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
- [Offline Team](examples/team): a deterministic Lead assigns dynamic tasks to
  typed Workers, one Worker recursively dispatches tasks, and approval resumes
  after SQLite recovery without an external LLM service.

```sh
go run ./examples/team start -db runs.db -run demo -tasks 2
go run ./examples/team resume -db runs.db -run demo -invocation iN -call cN -approved=true
```

Use the invocation and CallID printed by `start`; do not replace them with the
current waiting identity after an uncertain resume result.

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

## Format-7 migration

Checkpoint branch metadata now lives in the ordered `ActivationGroup.Branches`
records. Each `ActivationBranch` stores its invocation ID, declared target,
failure message and timeout. `Invocation.BranchIndex` still identifies the
invocation's position in that group; `Invocation.Target`, `Invocation.Error`
and `Invocation.Timeout` have been removed. Update code that constructs or
inspects checkpoints and custom Stores.

Format 7 rejects format-6 and older checkpoints. Finish active runs with their
matching runtime and use a new database for new executions. SQLite schema 2
remains in use; activation-group payloads use the new record shape.

## Format-6 migration

Merge callbacks now receive `(ctx, call, parentState, branches []BranchResult[S])`.
Update custom Merge signatures. Inspect individual branch outcomes via `b.OK()`,
`b.Failed()`, `b.Ended()`, `b.Target` and `b.Error`, or use `BranchValues(branches)`
to extract healthy states directly.

Format 6 rejects format-5 and older checkpoints; finish active runs with the
matching runtime and use a new database for new executions. SQLite schema 2 and
its stored record layout are unchanged.

## Format-5 migration

Merge callbacks now receive `(ctx, call, parentState, branchStates)`. Update
custom Merge signatures. Register a JoinSpec for each FanOut source. Convert
dynamic task lists to `FanOut(parent, graph.Branch[S]{Target, State}...)`, and
use `CompleteBranch(state)` to deliver results to the current activation group.
Existing `To(state, targets...)` retains its declared, distinct-target behavior.

Format 5 permits joined activations with zero or one child and allows more join
children than static outgoing edges. Format 4 checkpoints are rejected; finish
active runs with the matching runtime and use a new database for new runs.
SQLite schema 2 and its stored record layout are unchanged.

The earlier source and schema migrations are described below for projects
upgrading from previous releases.

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

Resume inputs now require `CallID`, obtained from the original waiting
invocation in `Pending()` or a checkpoint. Update every input producer, including
persisted application input records. Inputs without a matching CallID are
rejected with `ErrInvalidResumeInput`; there is no optional legacy mode.
Format 5 rejected format-4 checkpoints. The current checkpoint format and
migration boundary are described above.
