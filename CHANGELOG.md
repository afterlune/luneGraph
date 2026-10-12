# Changelog

## Unreleased

- Add the optional `Options.MaxInvocations` limit for invocation records retained
  by one execution. It applies atomically to static multi-target routing and
  dynamic FanOut; a transition that exceeds the limit is left uncommitted and
  returns resumable `StatusBudget`. The default remains unlimited, the value is
  supplied per public call, and checkpoint format and SQLite schema are unchanged.
- Add `continue` to the Team CLI to recover without submitting approval input.
  `start`, `resume` and `continue` accept `-max-invocations` (default 0, unlimited),
  so a budget-paused run can continue with a higher limit while approval resume
  still requires its original invocation and CallID.

- Update the offline Team example to inject synchronous provider-neutral model
  adapters. The Lead now returns structured assignments, Workers select
  adapters by persisted model ID, and Planner adapters can recursively delegate
  within a depth limit. The example remains deterministic and offline by default;
  its MachineID changes to `team-model-adapters-v2`, so old example runs require
  the prior source version or a new RunID.

- Move durable branch metadata from `Invocation` into ordered
  `ActivationGroup.Branches []ActivationBranch` records. Branch records now own
  the invocation ID, declared target, local error message and timeout; the
  invocation's `BranchIndex` remains the stable position used by `CallInfo`.
  Remove `ActivationGroup.Children` and `Invocation.Target`, `Invocation.Error`
  and `Invocation.Timeout`. This is a public API and checkpoint-format break:
  format 7 rejects format-6 and older checkpoints, with no automatic migration.
  SQLite schema 2 remains unchanged because activation groups already use a
  serialized payload; finish old runs with their matching runtime and use new
  runs for format 7.

- Add speculative join and branch pruning (Quorum & custom predicates).
  `JoinSpec[S].Quorum` and `JoinSpec[S].CanMerge` enable early triggering of `Merge`
  before all branches resolve (e.g. LLM hedging, racing, quorum voting).
  When early join conditions are met, `Merge` runs with available results;
  `BranchResult[S].Resolved()` reports true for settled branches, while
  `BranchResult[S].Pending()` identifies still-running branches.
  Upon join commit, all remaining uncompleted branches in the activation group
  are cancelled and pruned from live topology and checkpoint state.
  If quorum cannot be met due to branch failures, the join automatically falls
  back to normal execution once all branches resolve without deadlocking.

- Add declarative node, join, and dynamic branch timeout controls.
  `NodeSpec[S].Timeout`, `JoinSpec[S].Timeout`, and `Branch[S].Timeout` specify
  per-task deadlines (`time.Duration`). Negative durations are validated and rejected.
  When a node or branch exceeds its deadline, its context is cancelled with
  `context.DeadlineExceeded` and any escaped nil-error result is caught. Under
  `OnError: FailInvocation`, the failure is isolated (`inv.Error = "context deadline exceeded"`),
  enabling tolerant sibling branches and parent join aggregation. Timeouts and error
  messages are persisted and replayed transparently via SQLite Schema 2 metadata.

- Provide full branch outcome inspection in `Merge` via `[]BranchResult[S]`.
  Parents and Leads can observe not only successful branch states (`b.OK()`, `b.State`),
  but also ended branches (`b.Ended()`) and failed branches (`b.Failed()`, `b.Error`)
  with their declared `Target`, settled `Node`, and activation `Index`.
  Add `BranchValues[S](branches)` convenience helper to extract healthy branch states.
  Support tolerant dynamic joins where parent nodes handle partial branch failures
  without failing the entire execution, enabling robust agentic workflows and
  partial-result aggregations. Upgrade checkpoint format version to 6; SQLite
  schema 2 and record layout remain unchanged. Format <= 5 checkpoints are rejected;
  finish active old runs with the matching runtime and use a new database for new runs.

- Add runtime `FanOut(parent, branches...)` for zero, one, or many branch states
  and `CompleteBranch(state)` to return a child result to its current activation
  group. FanOut targets must be declared nodes or mounted subgraph entries and
  the source must have a JoinSpec. Merge now receives a cloned parent state
  before its ordered successful branch results. This source and checkpoint
  contract change emits format 5; SQLite schema 2 and its record layout remain
  unchanged. Format 4 checkpoints are rejected; finish active old runs with the
  matching runtime and use a new database for new runs.
- Add an offline Team example with deterministic Lead/Worker model substitutes,
  nested task dispatch, approval wait and SQLite recovery.
- Reuse the Merge task's existing input-state slot for the cloned parent state,
  so node callback queue entries do not carry an extra copy.
- Record format 5 as the explicit rejection boundary for format-4 checkpoints.

- Separate the executor's accepted live position, FIFO ready queue and reusable
  candidate workspace. Nodes, joins and group failures now share local topology
  collection; wide joins reuse the workspace's ID map for collection and diffing.
  Public APIs, checkpoint formats, scheduling and commit boundaries are unchanged.

- Require ResumeInput.CallID to match the original waiting callback identity.
  Empty/mismatched CallIDs, duplicate invocation inputs and unknown/non-waiting
  addresses return ErrInvalidResumeInput (errors.Is) before any Apply or input
  commit. This is a breaking input-contract change: migrate producers and
  application input records using Pending() or the original checkpoint. Do not
  rebind old payloads to a newer waiting CallID. Checkpoint format 4 and SQLite
  schema 2 remain unchanged; at-least-once and partial-input commits still apply.
- Preserve nil interface and typed nil continuation payloads through the typed
  registration adapter, including JSON null. Clarify that CallID, rather than
  all CallInfo fields, is stable across replay.

- Add executable contract examples and regressions for partial resume input
  reconciliation, nested mutable state cloning and shared Limiter dependencies.
- Add live `Execution` handles for repeated in-process advances. Start, Resume,
  Recover and Fork now return `(execution, result, error)`; Result carries only
  Status, Header and optional Final, while `Execution.Checkpoint()` materializes
  the full state on demand and `Pending()` lists continuation identities.
  `Options` is no longer generic and Store is an explicit method argument.
  Custom Stores implement `LoadHeader` for warm-handle revision checks. This is
  a source API change; the checkpoint encoding and SQLite schema are unchanged.
- Add concurrent recovery effect-receipt coverage, an outer-join conflict case
  keyed by activation identity, and single-run checkpoint scaling benchmarks.
- Dispatch Merge callbacks independently so other branches can progress while
  a join runs. Branch arrival, continuation application and Merge now commit
  separately. A rejected outer join does not replay committed inner work.
- Checkpoint format 4 and SQLite schema 2. Old versions are rejected; complete
  existing runs on the old runtime and use a new database for new runs.
- Replace Store.CompareAndSwap with Commit(expectedRevision, Change[S]); expose
  Header and Change. Commit atomically updates listed records. Terminal changes
  clear active topology. Custom Stores must implement the new delta contract.
- Store.Load returns Invocations and Groups in ascending lexicographic ID order;
  ActivationGroup.Children keeps fan-out target order. Custom Stores must match.
- Remove Store.DeleteMany. Stores expose single-run Delete only; repeated deletes
  are caller-managed and do not provide atomic multi-run cleanup.
- Replace checkpoint.Codec with StateCodec[S]; JSON[S] now encodes S. SQLite
  owns metadata and stores only changed states. For whole-checkpoint interchange,
  JSON[graph.Checkpoint[S]] is still a normal generic JSON codec.
- MaxConcurrency bounds nodes and joins together. Merge commits increase Revision,
  not Steps. MaxSteps still bounds node starts; admitted tasks and ready joins drain.
- Rename observation OperationCompareAndSwap to OperationCommit. Each Merge has
  its own result resolution and revision.
- Resume of a valid terminal run returns ErrRunCompleted. Invalid positions still
  return ErrInvalidCheckpoint. StatusFailed alone does not mean terminal failure.
- Builder.ExportMermaid returns (string, error), uses unique deterministic IDs,
  escapes labels and rejects recursive mounts. Runner.ExportMermaid remains string.

See [the performance guide](docs/performance.md) for benchmark methodology and
measured trade-offs. There is no automatic data migration or compatibility runner.
