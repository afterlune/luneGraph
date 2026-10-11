# Changelog

## Unreleased

- Dispatch Merge callbacks independently so other branches can progress while
  a join runs. Branch arrival, continuation application and Merge now commit
  separately. A rejected outer join does not replay committed inner work.
- Checkpoint format 4 and SQLite schema 2. Old versions are rejected; complete
  existing runs on the old runtime and use a new database for new runs.
- Replace Store.CompareAndSwap with Commit(expectedRevision, Change[S]); expose
  Header and Change. Commit atomically updates listed records. Terminal changes
  clear active topology. Custom Stores must implement the new delta contract.
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

See [core change validation](docs/core-changes.md) for verification and paired
benchmark results. There is no automatic data migration or compatibility runner.
