# Changelog

## Unreleased

### Added

- Add dynamic `FanOut` and `CompleteBranch` for zero-or-more tasks, nested task
  groups, ordered branch outcomes, and tolerant joins. Joins can run early with
  a quorum or predicate and prune unfinished branches; node and Merge
  callbacks plus dynamic branches support deadlines.
- Add live `Execution` handles for repeated advances, pending-wait inspection,
  recovery, and forks. Add resumable step and invocation budgets, interruption,
  shared callback limiting, and callback observation.
- Add an offline Team example with nested delegation and SQLite approval
  recovery. Its optional registry combines DeepSeek Chat Completions and OpenAI
  Responses adapters using stable model IDs; its routing fingerprint rejects
  recovery after incompatible registry changes. Offline remains the default.
  Credentials stay in provider-specific environment variables, and automated
  provider tests use local HTTP servers.

### API and execution changes

- Separate scheduler decisions from callback preparation, worker lifecycle, and
  checkpoint submission inside the executor. Public APIs, checkpoint format 8,
  callback ordering and recovery contracts remain unchanged.
- Seal Transition fields and require construction through control-flow helpers.
  Wait now requires its first target; continuation Apply remains state-only.
- Rename Branch.Timeout and ActivationBranch.Timeout to EntryTimeout. It covers
  each attempt of the entry callback only and clears atomically on every accepted
  successful entry transition, including waiting and nested fan-out.
- Advance rejects all terminal handles consistently, with or without a Store.
  Recover validates loaded positions before returning a handle, including when
  cancellation occurs after Load. Pending zero-child activations are valid recovery
  and clone-only fork positions.
- Preserve nested FailGroup causes on the parent's outer activation branch,
  including recovery and rejected-commit replay. Invocation budgets now count the
  entire retained live execution, rather than only the touched candidate records.
- Document concurrent Store calls and persisted error/stack metadata. SQLite's
  state codec does not encode or encrypt metadata.
- `Merge` receives the cloned parent state and ordered `[]BranchResult[S]`,
  including successful, ended, failed, and pending branch outcomes.
- `Start`, `Resume`, and `Recover` return a live execution handle, per-call
  result, and error. `Options` is non-generic, Store is passed explicitly, and
  full checkpoints are materialized through the handle.
- Change `Runner.Fork` to return a cloned active checkpoint without executing
  callbacks or emitting execution events. It optionally persists the copy; use
  Recover or Resume to drive it. Fork no longer accepts execution Options or
  returns an execution Result.
- `To` now requires its first target; runtime-generated target slices use
  `ToTargets`. `Result.BudgetReason` distinguishes step and invocation limits,
  and `OperationAdvance` distinguishes live-handle advances from checkpoint
  resumes. `CanMerge` and `Quorum` are exclusive; `CanMerge` receives cloned
  state copies, and predicate panics follow the join failure policy.
- Stores use revision-checked delta commits and expose `LoadHeader`; checkpoint
  state codecs encode application state only. `Store.DeleteMany` was removed.
- Resume inputs require the original waiting `CallID`. A mismatch is rejected
  before any continuation is applied; accepted inputs still commit individually.

### Compatibility

- Checkpoint format 8 rejects format 7 and earlier. There is no automatic
  checkpoint migration or compatibility runner. Finish active runs with their
  matching runtime and use a new database for new executions.
- SQLite remains at schema 2. The minimum supported Go version is 1.26.5.
- This source tree contains unreleased API and execution changes. See the
  [README migration notes](README.md#format-8-migration) before upgrading from
  an earlier checkout. See the [performance guide](docs/performance.md) for
  benchmark methodology and measured trade-offs.
