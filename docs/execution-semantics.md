# Execution semantics — checkpoint format 7

The graph describes allowed control flow. Each execution owns its evolving `S`.
Compiled Runners are immutable and may be shared. Callbacks, Clone, observers,
codecs and Stores must satisfy their documented concurrency contracts.

## Live execution handles

`Start`, `Resume`, and `Recover` return an `Execution` handle, the result of the
initial call, and an error. `Result` contains the call status, a metadata
`Header`, and an optional copied `Final` state; it does not contain the full
invocation topology or states. Call `Execution.Checkpoint()` when an independent
full checkpoint is needed. That operation copies all live invocation states
with Clone. `Execution.Pending()` returns sorted waiting-invocation identities
and continuation names without exposing or copying their state. `Fork` returns
an independent checkpoint and does not return a handle or execution result.

`Execution.Advance(ctx, inputs, opts)` continues the same in-memory scheduler
state. A handle is single-caller: do not call its methods concurrently. With a
Store, each Advance reads `LoadHeader` and checks the current revision before
running callbacks; this avoids loading or cloning unrelated invocation states.
It is a conflict check, not a lease. Another Runner or handle can commit after
the check, so callbacks may still replay and effects remain at-least-once.

Recover is the cold-open path: it loads the full checkpoint once and returns a
new live handle. Resume opens its supplied checkpoint as a new handle and, when
a Store is supplied, loads authoritative state and rejects a stale revision.
Use the returned handle for repeated in-process advances. After an uncertain
Store error, the handle retains its last locally accepted position; the next
Advance detects a committed remote revision through `LoadHeader` and returns
`ErrConflict`. Reload with Recover and reconcile inputs before continuing.

## State and composition

- Clone is required before nodes, continuation Apply and Merge inputs. Copy every
  mutable reference a callback can change. ValueClone is for scalar/immutable
  state. JSONClone requires a semantics-preserving JSON round trip; unexported or
  ignored fields and concrete interface types can be lost. Clone and Decode
  must not perform side effects.
- A container copy alone does not isolate mutable values inside it: a copied
  map can still share slices or pointers with the source. The
  [executable Clone example](../clone_example_test.go) shows a state-specific
  copy for nested byte slices, including nil/empty semantics. This rule applies
  at fan-out and Merge input boundaries as well as before individual callbacks.
- Static `To(state, target, moreTargets...)` requires at least one declared
  target. Use `ToTargets(state, targets)` for runtime-generated slices; an empty
  slice is rejected when the transition is applied.
  `FanOut(parent, branches...)` creates one activation for zero or more declared
  node/subgraph-entry tasks; repeated targets are valid and each branch receives
  an independent Clone. The source requires a JoinSpec. `CompleteBranch(state)`
  returns a child result to its immediate activation's join.
- Merge receives a Clone of the activation parent state followed by full branch
  results as `[]BranchResult[S]` in activation order. Each result reports
  `Index`, `Target`, `Node`, `Status`, `State`, and `Error` (with helpers `b.OK()`,
  `b.Failed()`, `b.Ended()`). Healthy states can be extracted via `BranchValues(branches)`.
  Ended branches (`EndBranch`) and failed branches (`FailInvocation`) do not trigger
  premature failure of the group; parents can inspect errors and handle partial failures
  tolerantly. Nested task groups and loops are allowed.
- Checkpoints keep ordered branch identity and metadata on the activation:
  `ActivationGroup.Branches` contains each child invocation ID, its declared
  target, local error message and timeout. The child's `Invocation.BranchIndex`
  addresses that record and remains the index reported in `CallInfo` and
  `BranchResult`; branch metadata is not duplicated on `Invocation`.
- Compile expands subgraphs into the same machine, state type, scheduler and
  checkpoint. Names are qualified by mount path. A mount has at most one exit;
  Return follows it or ends its invocation without an exit. Return outside a
  mount is invalid. EndExecution ends the entire run.
- To/Wait targets must be declared edges; Wait names a registered continuation.
  EndBranch discards its business result. EndExecution supplies Final. Natural
  completion can have neither Final nor Failure.
- Returned checkpoints are immutable caller views. Callbacks transfer ownership
  of returned state; they must not mutate retained references after returning.
  Stores own copies of writes and return independent copies from Load. Large
  content belongs in external storage; graph state should carry references.

## Per-task timeouts

Declarative timeouts are supported for static nodes, join callbacks, and dynamic branches:
- `NodeSpec.Timeout`: per-invocation deadline for the static node.
- `JoinSpec.Timeout`: per-execution deadline for the `Merge` callback.
- `Branch.Timeout`: per-branch deadline created by `FanOut`. Overrides the target node's default timeout for the initial step, then clears upon subsequent single-target transitions so the target node's own `NodeSpec.Timeout` applies.
- Negative durations are rejected at graph compilation (`AddNode`, `AddJoin`) and at runtime (`FanOut`).
- When a task has a positive timeout, the scheduler derives a context with `context.WithTimeout(runCtx, task.timeout)` and cancels it immediately upon callback completion.
- If a callback runs past its deadline, `context.DeadlineExceeded` is propagated even if the callback returned nil.
- Under `OnError: FailInvocation`, a timed-out branch records its error on the
  activation branch record and sets its invocation to `InvocationFailed`, while
  other branches and the eventual join proceed normally. Under `FailGroup` or
  `FailExecution`, group-level or terminal execution failure semantics apply.
- Branch timeouts and error strings are stored with activation group payloads
  and replayed identically during recovery.

## Speculative joins and branch pruning

A `JoinSpec` can declare early join thresholds:
- `Quorum int`: positive integer specifying the minimum number of successfully joined branches (`b.OK()`) needed to trigger `Merge` early.
- `CanMerge func(parent S, branches []BranchResult[S]) bool`: an alternative to
  Quorum. `Compile` rejects a JoinSpec that sets both. The predicate may be
  called repeatedly as outcomes change, so it must be fast, deterministic,
  side-effect-free and non-blocking. It receives cloned parent and branch
  states; do not mutate them. Clone errors preserve the last committed position
  without a terminal failure. Predicate panics become `PanicError` and follow
  the join's `OnError` policy.
- In `Merge`, `BranchResult.Resolved()` reports true for branches that joined, ended, or failed, while `BranchResult.Pending()` reports true for tasks that were still running or queued when early join triggered.
- When `Merge` commits, the entire activation group is retired: all child branches (including running and pending ones) are deleted from the live state and checkpoint.
- Active goroutines of pruned branches are promptly cancelled via context cancellation, and their delayed outputs are discarded upon exit.
- If early join conditions cannot be met due to branch errors or ended paths, the join falls back to standard execution as soon as all branches resolve (`remaining == 0`), preventing deadlocks.

## Independent callback commits

Start creates revision 1 before invoking callbacks. Accepted node outcomes,
continuation applications and joins each commit independently. Internal
settlement of a group without Merge can also advance the revision.

A branch reaching a join commits its state first. Once all children are joined,
ended or failed, the group is a persisted pending join. Merge runs as a worker
independently of other branches. Its result updates the parent and removes that
activation's children in one commit. Slow Merge callbacks do not block the driver
from dispatching or accepting independent node results. Express dependencies in
topology: waiting for work that requires the waiting callback's own permit can
still exhaust a shared Limiter.

This changes format 3: a rejected/interrupted Merge leaves triggering branches
committed. Only the pending Merge replays. Committed inner joins stay committed
when an outer join interrupts.

Revision counts accepted commits. Steps counts accepted node outcomes, including
ordinary node failures; Merge and Apply do not increase Steps. MaxSteps bounds
node starts per public call, default 10,000. After exhaustion, admitted tasks
finish and eligible joins settle; no new nodes start. Ready nodes then produce
StatusBudget. MaxConcurrency, default GOMAXPROCS, bounds nodes and joins together.
For `StatusBudget`, `Result.BudgetReason` is `BudgetMaxSteps` or
`BudgetMaxInvocations`; otherwise it is empty. A transition rejected for
exceeding the invocation count reports `BudgetMaxInvocations`; if no such
transition is rejected, exhausted node admission reports `BudgetMaxSteps` after
admitted work drains.
`Options.MaxInvocations` optionally bounds invocation records in the current
execution checkpoint; zero leaves it unlimited. Negative values are rejected.
It applies to transitions that create children through static multi-target `To`
or dynamic `FanOut`, and is checked before branch states are cloned or committed.
The count includes all invocation records retained in the checkpoint, including
waiting paths, group parents and resolved branches not yet removed by their join.
If a transition would exceed the limit, the entire transition is discarded and
the call returns nonterminal StatusBudget at the last committed position. The
pending callback retains its CallID and can be retried by raising the limit on a
later Resume, Recover, or Advance call. This can replay the callback;
continuation inputs must retain their original wait identity and be resubmitted
if their Apply transition exceeded the budget. A lower limit does not prevent
existing work from draining when it creates no new invocation records. The option is per public
call and is not persisted; it does not limit cumulative task creation, branch
data already constructed by callbacks, resident runs, or total process memory.
Continuation inputs apply sequentially before the driver starts. Counter
exhaustion returns ErrExecutionLimit and the last committed position without a
terminal Failure.

Each public call orders initially ready nodes numerically after the persisted
ScheduleCursor, wrapping around it. New ready work joins a local FIFO queue.
Results commit in driver receive order; competing EndExecution results depend
on completion timing. No global FIFO admission or deterministic parallel
completion is promised.

## Waiting, interruption and cancellation

ResumeInput addresses a specific wait with InvocationID and its original
waiting CallID. Both are required. An empty/mismatched CallID, duplicate
invocation, or unknown/non-waiting address returns ErrInvalidResumeInput
(inspect with errors.Is) and StatusFailed without a terminal Failure.
Inputs are validated and decoded before any Apply; invalid identities reject
the batch without applying or committing any input. Decode failure
leaves it unchanged. Inputs then commit one at a time. A later failure or
interruption preserves the accepted prefix. Resubmit only inputs whose original
waiting CallID remains pending in the authoritative checkpoint.

Application input records should retain RunID, InvocationID, the original
waiting CallID and their own payload bytes. After an uncertain write, Load the
authoritative position and select only records still matching a waiting
invocation and that original CallID. Keep unmatched records for application
reconciliation: a path may have advanced, failed or been removed, so a mismatch
alone is not an acknowledgement that this particular payload succeeded. Do not
rebind an old input to a new waiting CallID at the same InvocationID.
Pass that original waiting CallID in ResumeInput.CallID. Identity checking does
not acknowledge successful delivery, deduplicate external effects, or grant a
lease; concurrent recoveries can still execute the same Apply before one loses
its revision check.

The [executable partial-input example](../resume_example_test.go) submits the
matching records with Resume and the loaded checkpoint as its revision reference.
Resume loads again and rejects a reference already advanced by another caller.
This path costs two full Loads; it does not add commits or make admission
exclusive. A conflict after a callback starts still permits replay and requires
application effect idempotency. Terminal positions and empty matching sets are
handled without input submission. The example performs no automatic retry,
input deletion or success acknowledgement.

Wait commits state and waits for input. Interrupt(cause), including wrapped
signals, discards the current callback result and exits with StatusInterrupted
and ErrInterrupted. No interruption flag, cause, revision or step is persisted;
failure policy is bypassed. Panicking with an interruption signal remains a
panic. Clone, Decode and Store errors are not interruption control signals.
The caller decides when to retry.

Cancellation and interruption stop scheduling, cancel workers and wait for their
return. Other uncommitted results are discarded. Callbacks must cooperate with
context cancellation; Go functions cannot be forcibly stopped. Cancellation
takes priority when observed while handling interruption.

## Failures and public-call outcomes

Ordinary node, Merge and Apply errors use declared policy or FailureOverride.
Apply uses the suspending node's policy.

- FailInvocation is the default. The path fails; enclosing groups continue.
  HadLocalFailures is persisted. Details are transient events, not growing
  checkpoint history.
- FailGroup removes the activation and descendants, marks its parent failed, and
  cancels removed workers after accepting the failure commit. Their late results
  are discarded. At the root it becomes FailExecution. A rejected commit can
  replay original pending callbacks.
- FailExecution commits Completed with Failure and no active topology. The
  original call returns its Go error; Recover returns ErrRunFailed with the
  recorded message, without reconstructing the original error type.
- Callback/Clone panics become PanicError; execution failures retain panic stacks.
  Invalid transitions, cancellation and copy errors preserve the last committed
  checkpoint without a terminal Failure. Store/codec panics are outside the
  callback panic boundary.

Status describes this call, not necessarily the execution's lifetime:

| Result | Meaning | Next action |
| --- | --- | --- |
| Completed, nil error | Terminal; Final may be nil | Consume/clean up |
| CompletedWithFailures, nil error | Terminal with local failures | Apply application policy |
| Waiting | Active paths need input | Persist input externally; Advance, Resume or Recover |
| Budget | Ready work remains | Advance the handle or cold-open with Resume/Recover |
| Interrupted, ErrInterrupted | Callback result discarded | Reconcile; retry pending identity |
| Cancelled, context error | Call stopped; run may remain active | Reload durable state before retry |
| Failed with Result.Header.Failure | Acknowledged failed terminal | Inspect; do not continue |
| Failed without terminal Failure | Call failed; run may remain active | Inspect error; fix/reload |
| ErrConflict | The handle is stale or the commit lost its revision check | Load authoritative position |
| Other Store error | Commit may have happened | Load before replay |

Checking only error misses local failures. Checking only StatusFailed does not
identify a durable terminal. After uncertain writes `Result.Header` can be older
than the authoritative position. Reload before continuing.

## Recovery, forks and effects

Recover loads the latest full checkpoint and advances it, returning a live
handle. A terminal run returns without callbacks or another revision; failed
terminals return ErrRunFailed. Inputs to terminal runs return ErrRunCompleted.
Resume rejects any valid terminal position
with ErrRunCompleted; malformed positions use ErrInvalidCheckpoint. With a Store,
Resume treats its supplied checkpoint as an identity/version/revision reference,
loads authoritative state and rejects stale references with ErrConflict.

Fork validates and clones an active source, changes RunID, resets Revision to 1,
and returns the independent checkpoint. When given a Store, it creates the new
run there but does not drive it, invoke node, join, or Apply callbacks, or emit
execution events. Use Recover to drive a persisted fork; without a Store, pass
the returned checkpoint to Resume. Observation starts with that explicit
execution call. Fork does not load or change the source run. Completed sources
and a new RunID equal to the source RunID are rejected. Pending CallIDs may be
retained because RunID provides a new namespace. Clone remains subject to its
pure, side-effect-free contract. If Store.Create returns an uncertain error,
reload the target RunID before retrying. Applications account for effects after
they explicitly resume a fork.

Every node, Merge and Apply has a CallID persisted before it starts. It survives
replay; new loop visits/activations get new IDs. An application namespace plus
RunID/CallID identifies logical effects. Other CallInfo fields, including Step,
describe dispatch and need not be identical across concurrent replays. CAS is a
commit check, not a run lease: concurrent Recover calls can invoke the same
logical callback before one loses a commit.

Effects are at-least-once. Transactional effect/receipt recording, remote
idempotency keys and reconciliation belong to applications. Callback result
resolution says nothing definitive about its external effect. Deleting
checkpoints does not delete external data or receipts.

## Store and codec contracts

Store has Create, Load, LoadHeader, Commit and single-run Delete. `LoadHeader`
returns copied metadata without loading invocation or group records;
implementations should make it a cheap revision check. Commit takes expected
revision and a Change: authoritative Header/Final and listed invocation/group
upserts/deletions. Omitted records stay unchanged. IDs cannot repeat or be both
upserted/deleted. Missing deletion IDs are harmless. Completed changes contain
no upserts and remove all active records atomically.

Create accepts revision 1 and rejects existing RunID. Commit accepts expected+1
for the same identity, rejects terminal runs, and prevents Steps/NextID regression
or clearing HadLocalFailures. Stores check basic headers/change shape; Runner
also validates topology against the definition. Writes own independent state and
structural copies; Load returns an independent full checkpoint.

Memory clones changed states only. SQLite schema 2 separates header, invocations
and groups; changes commit in one transaction and Load uses a consistent read
transaction. SQLite uses WAL, synchronous=FULL and one connection. StateCodec[S]
encodes S only; SQLite owns scheduling metadata. JSON[S] requires JSON-preserving
state; custom codecs support concurrent use.

ErrConflict guarantees no write. Other Store errors, including context errors,
can be uncertain. Reload rather than retrying an obsolete candidate. There is
no automatic Store retry or distributed lease.

Delete removes one run and reports ErrNotFound when missing. Callers own
quiescence and retention; do not reuse IDs while old owners can address them.
Repeated Delete calls do not form an atomic multi-run operation. Deletion makes
SQLite pages reusable, without guaranteeing immediate file shrink.

Format 7 rejects format-6 checkpoints and older versions. Existing runs require
their matching runtime; use a new database for new executions. SQLite schema 2
remains unchanged, but activation-group payloads have the new branch-record
shape. No automatic migration or compatibility execution machine is supplied.
MachineID remains an application controlled definition identity; change it for
incompatible graphs.

FanOut atomically commits its parent, activation and child invocation IDs before
children can start. Each child gets an independent Clone, invocation ID and
CallID; repeated targets remain distinguishable by branch identity. Zero-child
groups become ready for Merge immediately. CompleteBranch records a successful
child result without selecting a fixed join node in application state.

Merge receives cloned parent state plus full branch outcomes (`[]BranchResult[S]`).
Successful branch states are cloned independently. Failed branches provide their
persisted error message in `b.Error`, while ended branches report `b.Ended()`.
Clone errors do not trigger terminal Failure. An accepted child result stays committed
if Merge is interrupted or its commit is rejected; only Merge replays with its
original CallID. MaxSteps bounds node starts per public call; eligible joins
continue to drain after the node budget is exhausted. Application code owns
recursion-depth and aggregate task-budget policy.

## Shared admission and observation

Limiter bounds node, Merge and Apply callbacks across runners/executions/state
types in one process. Nil uses local limits. Capacity is fixed and positive;
share its pointer, never copy it. Waiting responds to cancellation and does not
consume a node start or change CallID. Node Clone runs after admission; Merge
input cloning runs before admission. Clone, Decode, Store and Observer do not
acquire their own permits. Callback permits release after observation, before
submission. Limiter does not bound resident runs, waiting callers or total
memory. Hosts own admission queues and fairness.

A callback must not synchronously wait for another callback that needs a permit
it is holding. With capacity one, driving another Runner with the same Limiter
from Node, Merge or Apply creates such a wait. Propagate context so the nested
call can exit on cancellation. Increasing capacity to two admits one nested
call, but deeper or concurrent dependencies can still exhaust it. Express
composable dependencies in topology, as in the
[executable Limiter example](../limiter_example_test.go), or have the host drive
independent work after the callback returns. Permits are not reentrant.

Observers synchronously receive transient metadata, never state/input bytes.
They can be concurrent and must return promptly. Delivery panics are isolated;
blocking observers still delay work. Nil observation has no session/ledger/clock
reads. Started/Finished pair callback work; Resolved marks its individual
committed/discarded/unknown result. `OperationAdvance` identifies live-handle
calls separately from checkpoint `OperationResume`. OperationCommit replaces CompareAndSwap.
OperationID correlates one process-local public call; RunID/CallID correlate
replay. Durations exclude their own delivery. Err can contain application text
and must be treated read-only.
