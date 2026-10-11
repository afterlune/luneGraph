# Changelog

## Unreleased

- Add executable contract examples and regressions for partial resume input
  reconciliation, nested mutable state cloning and shared Limiter dependencies.
  Public APIs, checkpoint format 4 and SQLite schema 2 are unchanged.
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
