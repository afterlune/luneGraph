# Core runtime changes and validation

This change set upgrades the runtime to checkpoint format 4 and SQLite schema 2.
It focuses on independent join scheduling, delta commits and the API migration
they require. There is no compatibility runner or automatic database migration.

## Scheduling and commit behavior

- A single invocation with no activation group runs inline, avoiding a worker
  round trip for ordinary sequential paths. Once independent callbacks exist,
  nodes and Merge callbacks use lazy workers; a slow Merge cannot prevent the
  driver from dispatching/accepting independent branches.
- A fan-out's CallID and child topology commit before its branches run. Each
  branch result commits independently. When all children settle, Merge becomes
  runnable; its success/failure/interruption commits separately. A pending Merge
  replays under the same CallID; already-committed inner work does not replay.
- Merge result revisions increase without increasing Steps. MaxSteps still
  bounds node starts. Ready callbacks already admitted and eligible joins settle
  before returning StatusBudget. A group with no Merge settles without consuming
  a callback permit.
- MaxConcurrency bounds simultaneous node and Merge callbacks. Shared Limiter
  bounds node, Merge and Apply callbacks across runs. Merge input Clone runs
  before admission; node Clone runs after admission.
- Observer resolutions now identify one callback's individual commit. The public
  event OperationCommit replaces OperationCompareAndSwap.

## Delta Store API and ownership

```go
type Store[S any] interface {
    Create(context.Context, Checkpoint[S]) error
    Load(context.Context, string) (Checkpoint[S], error)
    Commit(context.Context, uint64, Change[S]) error
    Delete(context.Context, string) error
    DeleteMany(context.Context, []string) error
}
```

`Change[S]` contains authoritative Header/Final, invocation/group upserts and
deletion IDs. Omitted records remain unchanged. The checkpoint convenience
`Checkpoint.Change()` upserts its records and does not mean “replace the full
checkpoint”; `Change.Checkpoint()` materializes only records present in that
change. Stores own changed state values and structural slices. Their Create and
Load paths preserve complete independent checkpoints.

Memory stores Clone only changed states. SQLite stores header, invocation and
group rows separately, and commits a delta in one transaction. It prepares state
encodings before opening the write transaction. SQLite keeps WAL,
`synchronous=FULL`, and one connection; fewer persisted bytes do not remove the
single-writer cost. StateCodec[S] encodes S only; JSON[S] is the standard
implementation. Whole-checkpoint interchange can use JSON[graph.Checkpoint[S]].

Format 4 rejects format 1/2/3 checkpoints; schema 2 rejects schema 1 databases.
Finish old runs with their old runtime and use a new database. Custom Store
implementations and wrappers must adopt Commit and its atomic delta contract.
ErrConflict guarantees no write. Other Store errors can be uncertain; reload
before retry. Resume of a valid terminal checkpoint returns ErrRunCompleted.
Malformed/incompatible checkpoints return ErrInvalidCheckpoint.

Builder.ExportMermaid now returns `(string, error)`, rejects recursive mounts,
escapes labels and assigns unique deterministic IDs. Runner.ExportMermaid remains
a string-returning method.

## Validation

Repository workflow commands on Windows/amd64, Go 1.27.0, AMD Ryzen 7 8845H:

```sh
gofmt -l .
go test ./...
go test -race ./...
go vet ./...
go test -coverpkg=./... -coverprofile=coverage.out ./...
go run ./internal/coveragecheck -profile coverage.out
```

The final full suite, full race suite and vet pass. Production package coverage
passes the existing 85% gate; the lowest packages are executor 85.4% and SQLite
87.5%. Race was run with CGO_ENABLED=1 and the installed MinGW-w64 GCC. Formatting
and `git diff --check` are clean.

The baseline commit is `a3ad1618f5b5ad0ad1a5613593b67d53cc4bb43b`. Benchmarks ran
serially on this Windows/amd64 Go 1.27.0, Ryzen 7 8845H machine. Baselines use
three 300 ms samples. Candidate sequential/no-Store and fan-out/no-Store samples
use five 300 ms samples; durable cases use the final three-sample batch, except
SQLite width-32 fan-out, which was isolated for five samples. CPU counts are
shown per workload. Values are median ns/op, B/op and allocs/op:

| Workload | Baseline | Candidate |
| --- | --- | --- |
| 16-step sequential, CPU 1 | 16,667 / 3,184 / 76 | 16,690 / 3,912 / 79 |
| Width-32 fan-out/join, CPU 8 | 99,738 / 32,137 / 211 | 143,894 / 56,511 / 299 |
| Durable 16-step Memory recovery, CPU 1 | 50,320 / 38,678 / 232 | 55,783 / 49,790 / 299 |
| Durable width-32 Memory fan-out, CPU 8 | 1,606,407 / 1,418,080 / 5,326 | 488,394 / 358,704 / 1,597 |
| Durable width-32 SQLite fan-out, CPU 8 | 26,388,500 / 488,246 / 22,116 | 83,439,275 / 509,380 / 7,560 |
| Durable 16-step SQLite recovery, CPU 8 | 9,208,626 / 50,117 / 941 | 10,310,683 / 104,593 / 1,977 |

The single-invocation fast path brings ordinary sequential latency back to the
baseline range, though bytes and allocation count remain higher. Width-32
in-memory durable fan-out is about 3.3× faster with 70% fewer allocations. The
ordinary width-32 fan-out is about 44% slower, with 76% more bytes and 42% more
allocations. Durable sequential Memory and SQLite recovery also allocate about
29% and 110% more, respectively. The final SQLite width-32 fan-out samples range
from 77 to 130 ms, while an earlier same-host candidate batch measured about
26 ms; SQLite flush time is unstable on this host, so that latency comparison is
inconclusive. Its final allocation count is about 66% lower. These are review
items, not portable thresholds or production SLOs. Raw local samples are in
`artifacts/core/` and are ignored by Git.

Core reliability tests additionally cover model-sequence recovery, nested
FailGroup, interrupted inner/outer Merge, CAS/lost acknowledgements, process
crash, SQLite transaction rollback, foreign-key behavior after connection
replacement, mutable state ownership, and settlement of a no-Merge group while
an unrelated execution holds the remaining shared callback permit.
