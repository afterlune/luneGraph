# Execution semantics

`lune-graph` executes a compiled graph over a caller-defined `S`. Graph nodes,
edges, branches, joins, runs, failures, and checkpoints define the runtime.
Applications define the meaning of their state and callbacks.

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
includes a transition to waiting, a completed branch, or a locally handled
failure. The latest committed checkpoint is the recovery point.

`Resume` checks the supplied run, machine, format, and revision against the
Store, then executes the checkpoint loaded from it. It decodes all supplied
inputs before applying any. Each accepted input is applied and committed
separately; a later input error leaves the already committed prefix available.
Calling `Resume` without inputs advances ready invocations after a step budget
was exhausted. `Status` describes why the current call returned; the checkpoint
holds the persisted execution position.

`Recover(ctx, runID, inputs, opts)` requires a Store and loads its latest
checkpoint once. An active run follows the same input and scheduling rules as
`Resume`. A completed run with no inputs returns the stored result without
executing callbacks or writing a new revision. Inputs to a completed run are
rejected with `graph.ErrRunCompleted`, `StatusFailed`, and the stored checkpoint;
the inputs are not consumed. `Resume` continues to reject completed checkpoints.
If another executor commits between `Recover`'s load and CAS, the losing write
returns `graph.ErrConflict`; `Recover` does not retry automatically.

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
does not make callback side effects exactly once; applications should use
idempotent operations or deduplicate them with a stable run and invocation key.

`graph.ErrConflict` from a Store write guarantees that the attempted write did
not happen. Another Store error, including cancellation during a write, may
leave the outcome unknown. Load the latest checkpoint before deciding whether
to retry. Rebuild the same graph with a compatible `MachineID` when resuming in
a new process. An incompatible graph definition needs a new machine ID or an
application-managed migration.

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
