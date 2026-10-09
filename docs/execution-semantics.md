# Execution semantics

`lune-graph` executes a compiled graph over a caller-defined `S`. Graph nodes,
edges, branches, joins, runs, failures, and checkpoints define the runtime.
Applications define the meaning of their state and callbacks.

## Explicit recoverable interruption

Nodes, join merges, and continuation Apply handlers may return
`graph.Interrupt(cause)` (including a wrapped error) to end the current public
call with `StatusInterrupted` and an error matching `graph.ErrInterrupted`.
The cause remains available through `errors.Is` and `errors.As`;
`Interrupt(nil)` returns the sentinel itself. This control signal bypasses
`OnError` and `FailureOverride` and does not commit a failure or callback result.

The returned checkpoint is the last committed checkpoint. Interruption does
not advance its revision, steps, or schedule cursor, change its failure flags,
or replace pending CallIDs. Start and Fork may already have created their
initial checkpoint, and earlier results within this public call may already
have committed. With no Store, the same boundary applies to the checkpoint
returned to the caller for Resume.

Parallel interruption stops scheduling, cancels running callbacks, and drains
their results before returning. Those uncommitted results are discarded even
if successful. The runner cannot force callbacks to exit; they must respond
to context cancellation. Effects performed by discarded callbacks can replay.

A join and its triggering branch result or continuation input belong to the
same commit candidate. If a join interrupts, that entire candidate is
discarded, including earlier nested merges in it. Recovery can replay both
the trigger and joins with their original CallIDs. A continuation interruption
leaves the invocation waiting; its input is not persisted. Supply that input
again, along with later uncommitted inputs. Earlier committed inputs must not
be resubmitted.

Interruption is transient: neither its status nor cause is written into the
checkpoint. The caller decides when to Recover or Resume; these entrypoints
do not require an unlock operation and may replay pending callbacks normally.
Applications remain responsible for idempotency and reconciling uncertain
external effects. Store commit errors still require reloading to determine
what was committed.

Interruption does not claim exclusive ownership of a run. Two concurrent
Recover calls can both replay the same persisted CallID and return
`StatusInterrupted` without either writing a checkpoint. Another call can
also commit a completion or failure while an interrupted call is still
running. The interrupted call returns its own last committed checkpoint,
which may then be stale. Load or Recover the latest checkpoint to inspect
the authoritative outcome; Resume with the older revision returns
`ErrConflict`. An interruption cannot undo or unlock a committed terminal.

Only explicitly returned errors from the three effect-capable callback kinds
are interruption signals. Clone, Decode, and Store errors retain their
existing behavior even if they wrap `ErrInterrupted`. A panic wrapping the
marker remains a `PanicError` subject to the applicable failure policy. When
the public context is cancelled while an interruption is being handled,
the call returns `StatusCancelled` and the context error instead.

The new `"interrupted"` result status requires an additional branch in
exhaustive status handling. The checkpoint format and Store contract remain
unchanged.

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

`Fork(ctx, newRunID, checkpoint, opts)` creates an independent execution starting
from an existing checkpoint's execution position and state. The target checkpoint
must not be terminal (`ErrRunCompleted`). If a Store is configured, `Fork` initializes
the new run at revision 1 using `Store.Create` (returning `ErrConflict` if `newRunID`
already exists). The original execution remains unchanged. `Fork` allows exploration,
speculative execution, or trial branches without mutating the parent run's history.

`FailExecution`, including a root `FailGroup` escalation, commits a terminal
checkpoint with `Completed=true`, no active invocations or groups, and one
`Failure` pointer. Earlier local failures remain represented by
`HadLocalFailures`; their messages and states are not retained. Node failures
advance `Steps`; continuation failures only
advance the revision. The originating call returns
its original error after a successful commit. Cancellation, invalid transitions,
and state-copy failures do not create a failed terminal. A Store error is not
itself a terminal failure, but it may leave a terminal write's outcome unknown;
reload before deciding whether to retry. A crash before the terminal commits
can still cause the callback to run again.

`FailGroup` from an invocation in an activation group removes that group's child
invocations and all nested groups beneath them, then marks the group's parent
invocation failed. Running callbacks in the removed subtree are canceled before
settling enclosing groups, so their callback permits can be released before an
enclosing join waits for admission. All callbacks are drained before the public
call returns. This cancellation belongs to the current call and does not confirm
that its failure candidate was committed. If the candidate is rejected or its
submission is uncertain, reload the Store; recovery can replay callbacks canceled
from the uncommitted candidate using their persisted CallIDs.
Enclosing groups remain active and settle under their own join rules;
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
`RunID`, `InvocationID`, `CallID`, `Node`, `Step`, and `BranchIndex`.
`Node` identifies the current executing node or join name. `Step` provides the
execution step count at invocation start. `BranchIndex` gives the branch index
within the activation group for fan-out branches (0 for sequential paths).
The runtime persists a callback's ID
before that callback can start. Recovery reuses the same ID when replaying that
logical callback. A new node visit in a loop, a new continuation application,
or a new fan-out join gets a new ID. Use an application-specific namespace with
`RunID` and `CallID` as the deduplication key; invocation IDs identify paths
and are reused across sequential node visits. This key lets an application
recognize replays but cannot make an external effect atomic with checkpoint
commit. The external system must provide idempotency or atomic deduplication.
State clone functions and continuation decoders are expected to be pure and do
not receive a `CallInfo`.

Checkpoint format 3 persists callback IDs without accumulating branch outcomes
or local-failure records. Version-1 and version-2 checkpoints are rejected
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
`PhaseFinished` for each observed operation. Execution callbacks additionally
receive `PhaseResolved`, described below. It is optional and applies only to
the current public call; the compiled Runner does not retain it. With a nil
observer, the runtime does not create an observation session, allocate an
operation ID, or read clocks.

| Operation | Observed boundary |
| --- | --- |
| `OperationStart`, `OperationResume`, `OperationRecover` | One public call, including validation, cancellation, and draining workers |
| `OperationNode`, `OperationJoin` | The callback, including panic conversion, before accepting its outcome |
| `OperationDecode`, `OperationApply` | One continuation decoder or typed apply callback |
| `OperationCreate`, `OperationLoad`, `OperationCompareAndSwap`, `OperationDelete` | One actual call to the configured Store |

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
- Started and finished callback events have the revision of the checkpoint used to schedule or
  apply the callback, before committing its outcome.
- Create and CAS events have the attempted write revision. Load starts at zero
  and finishes with the loaded revision on success, or zero on error.

Status is populated only on public finished events. Action is populated only
on node finished events and describes the callback's returned decision before
transition validation. Err is the actual callback or Store error, or the public
call's returned error. Resolved events instead report their resolution cause. A locally handled callback error can therefore coexist
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


## Shared callback budget

`Options.Limiter` optionally participates in a fixed, process-local budget
created with `graph.NewLimiter(capacity)`. Capacity must be positive; a non-nil
zero value is rejected during option validation. Share the pointer across
executions, runners and state types; there is no mutable capacity or Close.

Node, join Merge and continuation Apply callbacks each acquire one permit.
Node admission precedes state cloning; clone failure releases the reservation.
Merge and Apply acquire before callback observation starts. Clone, continuation
Decode, Store and Observer do not acquire additional permits. Callback started
and finished delivery occur within the permit lifetime, so a slow observer
can delay availability. No permit is held during checkpoint submission.

Node scheduling uses the smaller of the local node concurrency and shared
capacity, starts workers on demand, and reuses them within the public call.
When admission is blocked, the scheduler selects among results, capacity and
context cancellation. Waiting does not start a callback, emit callback events,
consume node starts, or replace pending CallIDs. Cancellation while waiting
returns the last committed checkpoint and StatusCancelled without applying a
failure policy. Already running callbacks still drain under the existing
cancellation contract. Budgets release on callback return, panic conversion,
interruption and pre-callback clone failure.

This is a callback budget, not execution admission or ownership. Independent
Recover calls still contend through CAS and can replay the same CallID. Hosts
own queueing and fairness; neither FIFO nor cross-process limiting is promised.
Avoid synchronously driving another execution from a callback if the same
budget is already occupied by that callback and cannot admit the nested work.

## Callback result resolution

Execution callbacks (node, join and Apply) have a third observation phase,
`PhaseResolved`, with one of these `CallbackOutcome` values:

| Outcome | Meaning |
| --- | --- |
| `OutcomeCommitted` | Candidate accepted, including committed failure handling; with no Store, accepted in memory only. |
| `OutcomeDiscarded` | Result rejected before submission, or CAS returned ErrConflict with its guaranteed no-write semantics. |
| `OutcomeUnknown` | CAS returned another error, including cancellation/deadline errors; reload to determine actual persistence. |

Existing callback finished events report callback return, before validation or
commit. Resolved events correlate by `(OperationID, Operation, CallID)`; use
RunID and CallID to correlate logical replay across public calls. Resolved
Revision is the target revision when commit was entered, otherwise that
callback's original base revision. Resolved Err is the rejection or uncertain
submission reason; it is nil for committed outcomes even if the original
callback error was handled and committed. Action retains the node decision,
Time records resolution, Duration is zero, and Status remains unset.

A candidate consists of its triggering node or Apply and all joins invoked
while settling it, including nested merges and handled merge failures. Their
results resolve together. A later join interruption discards earlier merges
in that candidate. Input application resolves one committed prefix at a time;
a later interrupted input does not discard an earlier committed application.
Other parallel callback attempts remain unresolved until processed or drained;
superseded results resolve as discarded. Every started execution callback gets
exactly one resolution before a normally returning public call finishes.
Decode, Store and public operations keep only started/finished phases.

Only enabled observation allocates the unresolved metadata ledger. Entries are
removed on resolution, retain no state/payload/error history, and all remaining
entries are discarded after workers drain. Events can interleave across
workers and executions, and are neither persisted nor atomically coupled to
Store writes. A crash, Store panic, or observer delivery panic can leave the
consumer's record incomplete. Later recovery does not rewrite earlier unknown
events or reconstruct missing events. Consumers should switch on phases
explicitly instead of treating every non-started event as finished.

## Store contract

`Store[S]` implementations follow these rules:

| Operation | Successful result | Checkable failure |
| --- | --- | --- |
| `Create` | Save revision 1 for a new run | Existing run: `graph.ErrConflict`; malformed header: `graph.ErrInvalidCheckpoint` |
| `Load` | Return an independent copy of the latest revision | Missing run: `checkpoint.ErrNotFound` |
| `CompareAndSwap` | Save `expected + 1` for the same run and machine | Stale revision, wrong machine, or skipped revision: `graph.ErrConflict`; malformed header: `graph.ErrInvalidCheckpoint` |
| `Delete` | Remove all saved checkpoints for a run | Missing run: `checkpoint.ErrNotFound` |
| `DeleteMany` | Atomically remove the named runs; missing/duplicate IDs are harmless | Invalid ID rejects the whole batch; cancellation or storage failure returns an error |

A valid write header has the current `CheckpointFormatVersion`, nonempty run and
machine IDs without surrounding whitespace, and a positive revision. CAS at
the maximum revision returns `graph.ErrExecutionLimit`. A conflict never
changes the stored checkpoint. Other storage errors may have committed and
require a fresh `Load` to establish the latest revision.

`DeleteMany(ctx, runIDs)` validates the entire list before deletion. With a
valid Store and context, an empty list succeeds. IDs are not retained by the
implementation. Memory deletes under one lock; SQLite executes SQL chunks in
one transaction. A batch has one atomic outcome, including when a commit error
leaves that outcome uncertain. Reload target IDs with a fresh context to
confirm, then retry the idempotent batch as necessary. Invalid IDs never delete
anything. Separate Load calls can straddle a concurrent deletion; they are not
a multi-run snapshot.

The caller selects IDs and ensures their executions will no longer be driven.
Deletion is not cancellation, a completed-only condition, a retention policy,
or an execution lease. Use unique run IDs for new executions so stale owners
cannot address a new execution under an old ID. A CAS against a deleted run
returns conflict; Recover returns not found. Checkpoint deletion does not
delete application effect receipts or external data. Wrapped batch deletion
uses one started/finished `OperationDelete` pair, with the wrapper session's
metadata and the original error; it has no callback resolution event.

## Bounded execution position

`EndBranch[S]()` ends a path without a result. Ended and locally failed
invocations exist only while their activation is unsettled; settlement removes
them and releases their states. A local failure sets `HadLocalFailures` for
the remainder of the execution, including across recovery. It does not append
an error record. Detailed errors remain available in transient observer events
or application-owned receipts.

Completed checkpoints contain no invocations or activation groups.
`EndExecution(state)` sets `Final`; natural completion leaves `Final` nil.
`FailExecution` instead sets `Failure`, with its invocation ID, node, message,
and optional panic stack. A completed checkpoint with neither `Final` nor
`Failure` is valid. These changes do not add or remove commit boundaries,
alter callback IDs, or change at-least-once recovery and Store guarantees.
