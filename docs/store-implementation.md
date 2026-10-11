# Implementing a Store

Implement graph.Store[S] outside the graph core. Create accepts a full checkpoint;
Load returns a full independent checkpoint with `Invocations` and `Groups`
sorted by ascending lexicographic ID; `ActivationGroup.Children` retains its
fan-out target order. `LoadHeader` returns only a copied Header, including an
independent Failure value, and should avoid loading or decoding invocation and
group states. Warm `Execution.Advance` uses it to detect stale handles. Commit
accepts only changed records. State copies belong to the Store even after the
caller reuses its submission. `Change.Clone(copyState)` can establish ownership
before the atomic write.

## Atomic commit

1. Validate context, header identity/version, expected+1 revision and change shape.
2. Establish independent copies of changed states, Failure, routes, child lists
   and deletion IDs. Never call user Clone while holding a Store-wide lock.
3. In a transaction/critical section, compare the current revision and machine.
   A stale/missing run returns ErrConflict without modifying any record.
4. Reject terminal runs with ErrRunCompleted; reject decreasing Steps/NextID or
   clearing HadLocalFailures. Apply the authoritative Header and Final, listed
   upserts and deletions. Terminal changes remove all invocation/group records.
5. Commit atomically. Load must see either the complete previous or next position.

Clone/encoding failures before writing leave data unchanged. Publicly, errors
other than ErrConflict can be uncertain: a backend may commit and lose its ack.
Clients reload. Do not conceal this by retrying writes internally.

Checkpoint.Change() is a convenience for upserting all records in a checkpoint;
it does not delete omitted records. Change.Checkpoint() materializes touched
records only and must not be mistaken for an authoritative full checkpoint.
Production executors submit precise deltas; these helpers do not imply full
snapshot replacement semantics.

## Deletion and codecs

Delete removes every record of one run and reports checkpoint.ErrNotFound when
missing. Callers ensure quiescence and own retention. Store deletion does not
implement TTL, effects cleanup, or atomic multi-run deletion.

SQLite uses StateCodec[S]. Append must append to and not retain caller buffers;
Unmarshal must return an independent state. Preserve all application-required
fields. Scheduling metadata and CallIDs are handled by SQLite. Codec panics are
Store panics and are not recovered as callback PanicError.

## Verification

Use the built-in Stores and internal/storetest suite as contract references.
Test LoadHeader consistency after commits, missing-run and cancellation errors,
and Failure copy ownership. Also test canonical Load ordering and preservation
of child target order, ownership after caller mutation, stale/missing/wrong-machine
writes, mixed delta atomicity, terminal rejection, cancellation, uncertain acks,
concurrent loads/writes and single-run deletion. Add process-kill tests for
durable backends. Any serialization optimization must pass independent-reader
and reopen checks.
