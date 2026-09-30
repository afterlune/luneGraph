# Execution semantics

`lune-graph` executes a compiled graph over a caller-defined `S`. Graph nodes,
edges, branches, joins, runs, failures, and checkpoints define the runtime.
Applications define the meaning of their state and callbacks.

## Subgraph composition

`Graph[S].AddSubgraph` mounts another builder with the same `S`. `Compile`
recursively expands each mount into the parent machine, so a run still has one
state, one scheduler, and one checkpoint. A mount name acts as a graph vertex:
incoming edges enter the child graph's configured entry, and its optional
single outgoing edge is the destination for `Return(state)`. A return with no
destination ends that invocation branch. `EndBranch` and `EndExecution` inside
a child keep their run-wide behavior. `Return` from an unmounted node is an
invalid transition.

Nested and repeated mounts receive qualified node, join, and continuation
names based on their mount paths. For example, a child node `review` mounted
as `worker` is stored as `worker/review`; a continuation `approval` in that
child is stored as `worker/approval`. Node targets and wait continuation keys
are resolved within the graph where the callback was registered. These names
are part of the checkpoint's execution position, so recovery requires the same
expanded graph and a compatible `MachineID`. Subgraphs do not change the
checkpoint format.

## State ownership

Every run owns a `Checkpoint[S]`. The runner calls `Config.Clone` before passing
state to a node or continuation and when giving branch states to a join. `Clone`
must copy every mutable map, slice, pointer target, or other reference that a
callback may change. Stores own independent copies of values passed to them and
return independent values from `Load`. Treat returned checkpoints as immutable.

Keep large artifacts in an application-owned store and put references in `S`:

```go
type ArtifactRef struct {
    URI      string
    MIMEType string
    Size     int64
}

type PipelineState struct {
    Input  ArtifactRef
    Output *ArtifactRef
}
```

The graph runtime does not interpret artifact content or URI schemes. A
persisted state must round-trip through its chosen `checkpoint.Codec[S]` without
losing data required by callbacks. `checkpoint.JSON[S]` uses `encoding/json`.

## Commit points

When `Options.Store` is set, `Start` creates revision 1 with the entry
invocation ready **before** calling its node. Every accepted node outcome then
advances `Steps` and commits one new revision with `CompareAndSwap`. This
includes a transition to waiting, a completed branch, a locally handled
failure, or an execution-level failure. The latest committed checkpoint is the
recovery point.

`Resume` checks the supplied run, machine, format, and revision against the
Store, then executes the checkpoint loaded from it. It decodes all supplied
inputs before applying any. Each accepted input is applied and committed
separately. A later error under a local failure policy leaves the committed
prefix available; `FailExecution` commits a failure terminal after that prefix.
Calling `Resume` without inputs advances ready invocations after a step budget
was exhausted. `Status` describes why the current call returned; the checkpoint
holds the persisted execution position.

`Recover(ctx, runID, inputs, opts)` requires a Store and loads its latest
checkpoint once. An active run follows the same input and scheduling rules as
`Resume`. A successfully completed run with no inputs returns the stored result
without executing callbacks or writing a new revision. A persisted
execution-level failure returns `StatusFailed`, the stored checkpoint, and
`graph.ErrRunFailed` with the recorded message; its original Go error type
cannot be reconstructed from storage. Inputs to any terminal run return
`graph.ErrRunCompleted`, `StatusFailed`, and the stored checkpoint without being
consumed. `Resume` continues to reject terminal checkpoints. If another executor
commits between `Recover`'s load and CAS, the losing write returns
`graph.ErrConflict`; `Recover` does not retry automatically.

`FailExecution`, including a root `FailGroup` escalation, commits a terminal
checkpoint with `Completed=true`, no active invocations or groups, and one
terminal `Failure` with effective scope `FailExecution`. Earlier local failures
remain recorded. Node failures advance `Steps`; continuation failures only
advance the revision. The originating call returns
its original error after a successful commit. Cancellation, invalid transitions,
and state-copy failures do not create a failed terminal. A Store error is not
itself a terminal failure, but it may leave a terminal write's outcome unknown;
reload before deciding whether to retry. A crash before the terminal commits
can still cause the callback to run again.

`FailGroup` from an invocation in an activation group removes that group's child
invocations and all nested groups beneath them, then marks the group's parent
invocation failed. Running callbacks in the removed subtree are canceled and
drained. Enclosing groups remain active and settle under their own join rules;
only children that reach an enclosing join contribute states. `FailGroup` at the
root escalates to `FailExecution`.

Concurrent node results commit in arrival order. `EndExecution` cancels other
running callbacks and commits the winner's final state. A callback can finish
without its result being committed if another result ends the run or a commit
fails.

## Recovery and external effects

An executing node remains **ready** in the last committed checkpoint until its
outcome commits. If the process stops in that interval, loading the checkpoint
and resuming runs the node again. Cancellation and an uncertain Store write can
also leave a callback's external effect ahead of the stored checkpoint.
Therefore a callback may execute more than once across recovery. The runtime
does not make callback side effects exactly once.

Node, join merge, and continuation apply callbacks receive a `CallInfo` with
`RunID`, `InvocationID`, and `CallID`. The runtime persists a callback's ID
before that callback can start. Recovery reuses the same ID when replaying that
logical callback. A new node visit in a loop, a new continuation application,
or a new fan-out join gets a new ID. Use an application-specific namespace with
`RunID` and `CallID` as the deduplication key; invocation IDs identify paths
and are reused across sequential node visits. This key lets an application
recognize replays but cannot make an external effect atomic with checkpoint
commit. The external system must provide idempotency or atomic deduplication.
State clone functions and continuation decoders are expected to be pure and do
not receive a `CallInfo`.

Checkpoint format 2 persists callback IDs. Version-1 checkpoints are rejected
by this runner with `ErrInvalidCheckpoint`; applications must finish those runs
with the older runtime or migrate them before recovery.

`graph.ErrConflict` from a Store write guarantees that the attempted write did
not happen. Another Store error, including cancellation during a write, may
leave the outcome unknown. Load the latest checkpoint before deciding whether
to retry. Rebuild the same graph with a compatible `MachineID` when resuming in
a new process. An incompatible graph definition needs a new machine ID or an
application-managed migration.

### Transactional application receipts

The [effect example](../examples/effects) demonstrates this boundary using a
local counter. Its application ledger owns a separate SQLite file and stores
the increment and original result under `(namespace, RunID, CallID)`.
An immediate write transaction serializes the receipt lookup, counter update,
and receipt insert across connections. Both writes commit together or roll
back together. A matching receipt returns the stored result rather than the
counter's current value; reusing its key with a different increment returns a
checkable application error. The namespace versions the operation semantics.

The recovery sequence is:

1. The callback commits the counter update and receipt in the application DB.
2. The graph commits the callback outcome in its independent checkpoint DB.
3. If step 2 fails or the process crashes before it, recovery loads the latest
   checkpoint. A pending callback replays with the same CallID and reads the
   receipt, restoring the original result without another counter update.
4. If the checkpoint write succeeded but its acknowledgement was lost, recovery
   advances the committed checkpoint instead of repeating an accepted callback.

Applications must retain receipts while a callback can still be replayed.
Completed-run recovery returns persisted state without invoking callbacks.
The example uses `FailExecution` for business errors; it adds no automatic
retry policy. A ledger commit error can itself have an uncertain outcome:
inspect the receipt before deciding how to resolve a business failure. A
terminal graph failure is not made resumable by finding a receipt.

This pattern protects effects performed inside the application transaction.
A receipt inserted separately from a remote write does not provide the same
guarantee. Remote effects still need the destination's idempotency or an
application-managed protocol. There is no atomic transaction between this
ledger and graph checkpoints, and the runtime's contract remains at-least-once.

## Observation

`Options.Observer` receives a `graph.Event` at `PhaseStarted` and
`PhaseFinished` for each observed operation. It is optional and applies only to
the current public call; the compiled Runner does not retain it. With a nil
observer, the runtime does not create an observation session, allocate an
operation ID, or read clocks.

| Operation | Observed boundary |
| --- | --- |
| `OperationStart`, `OperationResume`, `OperationRecover` | One public call, including validation, cancellation, and draining workers |
| `OperationNode`, `OperationJoin` | The callback, including panic conversion, before accepting its outcome |
| `OperationDecode`, `OperationApply` | One continuation decoder or typed apply callback |
| `OperationCreate`, `OperationLoad`, `OperationCompareAndSwap` | One actual call to the configured Store |

Public calls begin observation after checking that their Runner and context are
non-nil. Other validation errors still produce a public finished event with the
returned status and error. Internal recovery does not produce a nested public
`Resume` event. Recovering a terminal run produces a public pair and a Load
pair, without callback or write events.

Every event from one public call has the same process-local `OperationID`,
RunID, and compiled MachineID. OperationID is not persisted and must not be
used as a cross-process identifier. Node, join, and continuation apply events
use the callback's persisted CallID; replay uses that ID again in a new public
operation. InvocationID identifies the path. Decode events have InvocationID
and Continuation but no CallID. Subgraph node and continuation names are
qualified as in the checkpoint.

Revision means the following:

- A public start event has revision zero for Start and Recover, or the supplied
  reference revision for Resume. Its finished event has the returned checkpoint
  revision, even when the call returns an error.
- Callback events have the revision of the checkpoint used to schedule or
  apply the callback, before committing its outcome.
- Create and CAS events have the attempted write revision. Load starts at zero
  and finishes with the loaded revision on success, or zero on error.

Status is populated only on public finished events. Action is populated only
on node finished events and describes the callback's returned decision before
transition validation. Err is the actual callback or Store error, or the public
call's returned error. A locally handled callback error can therefore coexist
with a successful public return. A successful callback event does not mean that
its outcome passed validation or committed. A Store error does not establish
whether its write committed, except for the documented no-write guarantee of
ErrConflict. Confirm execution position through the Store. Errors are shared
with execution handling; observers must treat them and referenced objects as
read-only.

Delivery is synchronous on the operation's goroutine. One operation starts
before it finishes, and a public call finishes after all its running callbacks
have returned and their events have been delivered. Different workers and
different public calls can deliver events concurrently and have no total
ordering. Canceled or superseded callbacks still produce finished events if
they return, even when their results are discarded. A slow observer may change
the arrival order of parallel results.

Observers must be concurrency safe, return promptly, and avoid recursively
driving the same execution. Each observation panic is recovered independently
and does not become a Failure or alter the callback's returned value. There is
no observer timeout or asynchronous queue. Events retain the operation's
context, including cancellation; observers must decide how to report canceled
operations themselves.

Time is the event creation time. Duration uses the monotonic clock and excludes
delivery of that operation's started and finished events. Public-call duration
includes nested event delivery and worker draining. Events contain no state,
checkpoint objects, or payload bytes; their error messages are still supplied
by application callbacks or stores. Events are transient and can be incomplete
after a crash or an unrecovered panic. Store panics remain outside the callback
panic boundary and do not produce misleading finished events.

The optional `observe.NewSlog` adapter uses Info for public calls, Debug for
callbacks and Store calls, and Error for any event carrying an error. It checks
`Logger.Enabled` before constructing attributes. A nil logger selects
`slog.Default()` at construction. Its handler must support concurrent calls.
The adapter adds no persistence and makes no delivery guarantee beyond the
Observer contract.

## Store contract

`Store[S]` implementations follow these rules:

| Operation | Successful result | Checkable failure |
| --- | --- | --- |
| `Create` | Save revision 1 for a new run | Existing run: `graph.ErrConflict`; malformed header: `graph.ErrInvalidCheckpoint` |
| `Load` | Return an independent copy of the latest revision | Missing run: `checkpoint.ErrNotFound` |
| `CompareAndSwap` | Save `expected + 1` for the same run and machine | Stale revision, wrong machine, or skipped revision: `graph.ErrConflict`; malformed header: `graph.ErrInvalidCheckpoint` |

A valid write header has the current `CheckpointFormatVersion`, nonempty run and
machine IDs without surrounding whitespace, and a positive revision. CAS at
the maximum revision returns `graph.ErrExecutionLimit`. A conflict never
changes the stored checkpoint. Other storage errors may have committed and
require a fresh `Load` to establish the latest revision.
