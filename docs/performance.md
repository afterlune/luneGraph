# Performance, concurrency, and reliability baseline

The project uses three engineering standards:

- **High performance:** keep repeatable benchmarks for graph scheduling and checkpoint persistence. Compare results on the same Go version and machine before and after runtime changes.
- **High concurrency:** measure parallel branches inside one run and independent runs that share a compiled runner and a store. The runner and built-in stores are expected to support the concurrency documented by their APIs.
- **High reliability:** verify execution and recovery semantics with semantic and fault tests, including cancellation, replay, compare-and-swap conflicts, process crashes, and uncertain commit acknowledgements. Reliability is not represented by a numeric benchmark or an exactly-once claim.

These standards do not set machine-independent latency targets or a CI performance gate. Benchmark results depend on hardware, operating system, filesystem, Go version, and SQLite settings.

For larger definitions, wide fan-out, paused populations, shared executions,
and manual sustained execution, see [the capacity baseline](capacity.md).

## Run the benchmarks

```sh
go test -run '^$' -bench . -benchmem -cpu '1,4,8' ./...
```

The benchmarks report Go's `ns/op`, `B/op`, and `allocs/op` values. Graph compilation and store setup are outside steady-state measurements. Every measured run or write checks its result so a benchmark cannot silently measure an invalid execution.

| Benchmark | Coverage |
| --- | --- |
| `BenchmarkSequentialExecution` | Complete sequential runs of 1, 16, and 128 node steps, without persistence. |
| `BenchmarkFanoutJoin` | Fan-out and join widths 2, 8, and 32, with one worker and the runner's default concurrency. State is a scalar so the result emphasizes scheduler work. |
| `BenchmarkConcurrentRuns` | Independent runs sharing one compiled runner, with no store, the memory store, and SQLite. The built-in stores retain each run checkpoint until that case ends; the SQLite file is temporary. |
| `BenchmarkCheckpointCAS` | Repeated CAS updates to one checkpoint in memory and SQLite. The state contains a 16-entry map and uses a deep-copy function. Reusing one row bounds database growth. |
| `BenchmarkSQLiteReopenRecover` | SQLite open, recovery of a persisted waiting run, continuation application, completion, and close. Fixture creation and database reset happen outside the timed interval. |
| `BenchmarkJSONCheckpointEncoding` | Compares `json.Marshal` with checkpoint JSON append into a reused destination buffer for a width-32 checkpoint containing 16-entry maps. |
| `BenchmarkDurableSequentialExecution` | Repeatedly recovers one persisted loop run with step budgets 1 and 16, using memory and SQLite. The state contains a 16-entry map. |
| `BenchmarkDurableFanoutJoin` | Repeatedly recovers one persisted fan-out/join loop at widths 2, 8, and 32, with serial and default concurrency, using memory and SQLite. The state contains a 16-entry map. |
| `BenchmarkObservation` | Disabled, no-op, and Debug-level JSON slog observers on 16-step sequential execution, width-32 fan-out/join, and durable 16-step recovery with memory and SQLite. State is a scalar. |
| `BenchmarkCheckpointCopyInto` | Reused structural-copy buffers with 1/128/512 invocations, no routes/all waiting/alternating routes, reference state, and scalar/512-byte value controls. No application Clone or Store work is included. |

The durable execution benchmarks create their initial run before timing and repeatedly call `Recover` with the same run ID. Every executed node outcome is persisted through the normal checkpoint path. The loop leaves a bounded checkpoint row with ready work after each measured call, so iterations measure recovery and execution without growing the number of stored runs. Each case loads the final checkpoint after timing and checks it against the returned result.

## Shared callback budget and result resolution

The [shared-budget capacity record](capacity.md#shared-callback-budget-and-result-resolution)
covers 64 executions, eight callers, eight branches and global callback capacity
one/eight with Memory and SQLite. It includes clone/state isolation, three
callback kinds, typed resume input, pending CallID retention, permit release,
CAS ambiguity, nested join candidate resolution and escaping Store-panic drain.
`BenchmarkCapacitySharedLimiter` records per-call latency, batch throughput and
sampled resources. Its observer and validation costs are included; SQLite
samples complete one timed batch and are finite validation, not sustained limits.

The same-host baseline is `d2baa6e`. Longer paired sequential/fan-out samples
have overlapping timing ranges; the large slowdown in the initial short scan
was not reproduced. Lazy workers reduce default sequential and paused-inspect
allocation counts. Enabled result observation and Debug JSON logging add real
costs because each execution callback now resolves its outcome. Nil observation
still allocates no session or ledger and reads no observation clocks. The
capacity record preserves sample ranges, observer costs, workload details,
commands and limitations; there is no machine-independent performance gate.

### Nested failure under exhausted shared admission

`TestLimiterNestedFailGroupProgress` covers node and nested-join `FailGroup`,
two running descendants beneath a removed group, and an enclosing join under
an exhausted four-permit limiter. Memory and SQLite cover completion and
public-call cancellation; Memory also covers CAS conflict and ordinary Store
errors with and without a write. Every case keeps an independent execution
active in the same Store and limiter, checks callback result resolution and
replay identities, and verifies that all permits remain reusable. SQLite is
closed and reopened before checking recovery. Channels establish the order;
timeouts only fail a stalled test, and no semaphore fairness is assumed.

Both failure sources stall at baseline `9ceaeda`: cancellation of removed
callbacks waits for the commit, while the enclosing join waits for permits
those callbacks retain. The fix cancels pruned callbacks before settling
enclosing groups, including pruning caused by a nested join. It does not add a
commit point or a persisted cancellation marker. A rejected candidate still
replays from its last committed checkpoint.

```sh
go test -run '^TestLimiterNestedFailGroupProgress$' -count=20 ./internal/observationtest
go test -race -run '^TestLimiterNestedFailGroupProgress$' -count=20 ./internal/observationtest
```

See the [failure cancellation comparison](capacity.md#nested-failure-cancellation-under-shared-admission)
for the same-host sequential and fan-out allocation/timing comparison.

## Current format-3 diagnosis

The [format-3 scale and cost diagnosis](capacity.md#format-3-scale-and-cost-diagnosis)
records 336 uninstrumented samples at commit `d6ed67e`, using 500ms intervals,
three repetitions, and one/eight Ps. It covers wide fan-out, nested groups,
shared executions, paused populations, and sequential/durable controls. Five
separate CPU/memory profiles and a sampled shared-Store lock/block profile
distinguish executor work, application Clone, serialization, and persistence.
No runtime code or persistence settings changed in that measurement stage.

The bounded follow-up reuses unchanged positions in the existing invocation
index during compaction. Two paired benchmark batches on the same Windows/AMD
host showed a 3–4% lower median for nested 128-group execution; width-512
fan-out results varied in direction and had overlapping ranges. Allocation
counts were effectively unchanged. The [capacity record](capacity.md#invocation-compaction-follow-up)
contains the sample medians and limits of this comparison. This is local
evidence, not a portable performance target or CI gate.

Memory durable allocation volume is dominated by application map Clone
(81.97% in the width-32 profile). SQLite's CAS path accounts for 93.73% of
sampled CPU and JSON Append for 76.40% of allocated bytes; these are different
cost dimensions. Shared Memory has observed Store-wide lock contention, but
aggregate delay across goroutines is not wall-clock latency. Independent
profile percentages overlap; none justify skipping Clone or batching commits.
The capacity record includes sample ranges, the aborted high-frequency
instrumentation attempt, successful sampled replacements, and artifact paths.
The older optimization profiles below remain historical evidence.

## Pre-Append durable-execution sample

This diagnostic sample was collected on 2026-09-29 with Go 1.26.5, Windows/amd64, and an AMD Ryzen 7 6800H, before checkpoint JSON encoding switched to `Append`. It is a machine-specific reference, not a performance target. Each row summarizes five runs as median and observed range. `GOMAXPROCS` is selected by Go's `-cpu` flag.

```sh
go test -run '^$' -bench '^BenchmarkDurableSequentialExecution/storage=(memory|sqlite)/steps=16$' -benchmem -benchtime=1s -count=5 -cpu=8 .
go test -run '^$' -bench '^BenchmarkDurableFanoutJoin/storage=(memory|sqlite)/width=32/concurrency=default$' -benchmem -benchtime=500ms -count=5 -cpu '1,4,8' .
```

| Workload | Store | GOMAXPROCS | ns/op, median [range] | B/op, median [range] | allocs/op, median [range] |
| --- | --- | ---: | ---: | ---: | ---: |
| Sequential, 16 steps | Memory | 8 | 629,734 [146,622–9,128,545] | 42,208 [42,193–42,209] | 292 [291–292] |
| Sequential, 16 steps | SQLite | 8 | 37,274,804 [36,446,133–45,947,545] | 72,750 [72,747–72,831] | 1,226 [1,225–1,227] |
| Fan-out/join, width 32 | Memory | 1 | 1,625,758 [1,516,675–1,703,178] | 1,417,995 [1,417,993–1,417,999] | 5,519 [5,519–5,519] |
| Fan-out/join, width 32 | Memory | 4 | 1,712,412 [1,676,122–1,812,778] | 1,418,539 [1,418,535–1,418,604] | 5,520 [5,520–5,520] |
| Fan-out/join, width 32 | Memory | 8 | 2,075,061 [1,901,910–2,477,101] | 1,419,293 [1,419,281–1,419,383] | 5,520 [5,520–5,520] |
| Fan-out/join, width 32 | SQLite | 1 | 85,492,114 [84,575,143–102,055,617] | 1,892,829 [1,892,696–1,892,836] | 38,672 [38,658–38,672] |
| Fan-out/join, width 32 | SQLite | 4 | 85,862,243 [81,549,757–114,416,967] | 1,921,963 [1,917,292–1,926,854] | 38,687 [38,676–38,691] |
| Fan-out/join, width 32 | SQLite | 8 | 83,319,857 [82,622,286–128,695,333] | 1,932,808 [1,927,990–1,941,956] | 38,691 [38,681–38,696] |

The CPU and memory profiles in this section were collected separately for the memory and SQLite versions of both workloads, with `-benchtime=5s -cpu=8`, before the Append change. Profile files and generated test binaries were kept outside the repository in the local temporary directory. Profiles are process-wide, so they include a small amount of benchmark setup; the dominant stacks below are from the measured recovery loop.

The SQLite CPU profiles attribute more than 92% cumulative time to `Store.CompareAndSwap` and its `database/sql`/SQLite call path. Sampled stacks enter Windows `FlushFileBuffers` during SQLite WAL commit, consistent with the configured `synchronous=FULL` durability. In the SQLite fan-out allocation profile, the `encoding/json.Marshal` subtree accounts for about 80% of allocated bytes. In the memory profiles, the benchmark's map clone accounts for about 77% of sequential allocations and 83% of fan-out allocations; `internal/model.resizeSlice` accounts for about 13% in fan-out. The `inuse_space` profiles show only runtime and platform initialization allocations at the end of these short runs, with no application checkpoint type standing out as retained heap.

The memory sequential timing had one 9.1 ms/op outlier; the SQLite fan-out timing also had occasional high samples. These ranges are reported rather than hidden. Repeat on an otherwise idle host before using them to compare a code change. The allocation profile led to the append-based checkpoint JSON encoder below. Reducing synchronous SQLite commits would alter the documented persistence boundary, and application state cloning remains application-defined.

## Post-Append allocation sample

These follow-up samples were collected on 2026-09-29 with Go 1.26.5, Windows/amd64, and an AMD Ryzen 7 6800H, using the same durable fan-out workload and five-run settings as the pre-Append sample. The focused codec benchmark runs with one P to reduce scheduler variation; ranges are observed ranges, not targets.

```sh
go test -run '^$' -bench '^BenchmarkJSONCheckpointEncoding$' -benchmem -benchtime=500ms -count=5 -cpu 1 ./checkpoint
go test -run '^$' -bench '^BenchmarkDurableFanoutJoin/storage=sqlite/width=32/concurrency=default$' -benchmem -benchtime=500ms -count=5 -cpu '1,4,8' .
```

| JSON checkpoint encoding | ns/op, median [range] | B/op, median [range] | allocs/op, median [range] |
| --- | ---: | ---: | ---: |
| `json.Marshal` | 183,653 [175,757–187,047] | 48,578 [48,578–48,578] | 1,058 [1,058–1,058] |
| `AppendReuse` | 174,492 [167,296–186,288] | 35,033 [35,033–35,033] | 1,058 [1,058–1,058] |

The codec benchmark reduced allocated bytes by 27.9% with the same allocation count. Its timing ranges overlap.

| SQLite fan-out/join, width 32 | GOMAXPROCS | ns/op, median [range] | B/op, median [range] | allocs/op, median [range] |
| --- | ---: | ---: | ---: | ---: |
| After Append | 1 | 84,168,529 [82,108,486–104,993,800] | 1,461,482 [1,461,326–1,461,489] | 38,707 [38,692–38,707] |
| After Append | 4 | 83,458,914 [81,988,786–88,701,057] | 1,488,264 [1,478,421–1,493,310] | 38,721 [38,715–38,729] |
| After Append | 8 | 83,978,257 [81,625,386–123,235,786] | 1,495,012 [1,489,958–1,504,046] | 38,724 [38,712–38,729] |

Against the pre-Append sample above, SQLite fan-out allocated 22.6–22.8% fewer bytes per operation. Allocation counts increased by 33–35 per operation, about 0.1%; timing differences were small relative to the observed ranges and had no consistent direction.

A post-Append `alloc_space` profile of the same SQLite width-32 workload, collected with `-benchtime=5s -cpu=8`, still attributes 74.5% cumulative allocation volume to `checkpoint.JSON.Append`; `encoding/json.mapEncoder.encode` accounts for 42.1% flat and 73.1% cumulative, while `reflect.unsafe_New` accounts for 31.5% flat. This profile includes benchmark setup; the profile and generated test binary were kept outside the repository. Reflection while encoding maps remains the main allocation source after output-buffer reuse.

“Reopen” means the SQLite store and database handles are closed and opened again for each measured recovery. The operating system's filesystem cache is not flushed, so this does not claim a cold-disk measurement.

For comparisons, run the command more than once on an otherwise idle machine and keep the Go version and `-cpu` values fixed. The tests validate every result inside the measured path; this adds a small amount of assertion work to each operation.

For focused profiles, pass `-cpuprofile` and `-memprofile` to a single benchmark selection. Inspect CPU samples with `go tool pprof -top <cpu-profile>` and allocation volume with `go tool pprof -top -sample_index=alloc_space <memory-profile>`. Keep profile files and generated test binaries in a temporary directory.

The executor reuses an alternate checkpoint-structure buffer while a run advances. This can retain slice capacity up to the run's peak invocation and group counts until the call returns. After each successful commit, the old buffer's state fields are cleared so it does not keep prior application state alive. The copy remains shallow for user state, matching `model.Copy`; it does not deep-copy application state.

Structural invocation copying keeps the direct loop for lists of at most eight.
Larger lists batch consecutive entries with nil `Next` through Go's built-in
`copy`; non-nil routes are copied independently with destination-buffer reuse.
Non-nil empty routes still normalize to nil, and obsolete destination routes
are cleared before their headers are replaced. No route storage is shared with
the source, and application state remains shallow. The implementation adds no
auxiliary index or state cache. See [the structural copy measurements](capacity.md#checkpoint-structural-copy-optimization-sample-format-2)
for the same-machine comparison and ownership checks.

Invocation lookup uses a temporary ID index when a checkpoint has more than eight invocations. Shorter lists use a direct scan to avoid map setup cost. The index follows fan-out insertion and group compaction, is rebuilt when a saved execution resumes, and is never written to a checkpoint.

The large-set index keys positions directly by string ID and keeps a separate
numeric ordering of ID/number/position metadata. Ready selection starts after
`ScheduleCursor` using binary search and stops at the first ready invocation
that is not running, wrapping when necessary. It reads status from the current
checkpoint and never retains invocation pointers or user state. Numeric ID
parsing happens on index creation/insertion rather than each large-set lookup
or dispatch. Compaction updates positions and clears removed metadata; sets
shrinking to eight invocations release the index. Setup and additional metadata
cost remain workload-dependent; sparse sets may still scan every entry. See
[the scheduling measurements](capacity.md#scheduling-optimization-sample-format-2) for
the measured tradeoffs and retained round-robin/recovery checks.

Group readiness keeps a temporary confirmed-terminal child prefix per activation.
Joined, ended, and failed children cannot become runnable again in that activation.
Checks therefore resume at the first unconfirmed child, reading its current
checkpoint status. Zero or one group needs no heap-allocated progress cache;
multiple groups use ordered ID/cursor records and an ID map for topology changes.
Removed entries are cleared, and contraction to one group releases the arrays
and map. Metadata can retain capacity up to the call's peak group count.
Each drive or continuation-input batch starts its own cache; none is persisted
or shared by concurrent executions. Group selection and merge input order still
follow the checkpoint slices. Many groups can still require a linear traversal.
See [the group readiness measurements](capacity.md#group-readiness-optimization-sample-format-2)
for workload geometry, allocation tradeoffs, and semantic checks.

## Observation cost

`BenchmarkObservation` lives in `internal/observationtest`, keeping observer
integration checks and their benchmark fixtures together. Graph compilation,
logger construction, Store setup, and the durable seed run are outside timing.
The durable cases reuse one run and check both state progress and the final
stored checkpoint. The fan-out case ends its parent branch at the join; the
existing root fan-out benchmark also runs a final node, so their scores are not
directly comparable. The slog case enables Debug events and writes JSON to
`io.Discard`; it measures formatting and delivery, not filesystem or network I/O.

```sh
go test -run '^$' -bench '^BenchmarkObservation$' -benchmem -benchtime=1s -count=5 -cpu '1,8' ./internal/observationtest
```

The following short diagnostic sample was collected on 2026-09-30 with Go
1.26.5, Windows/amd64, and an AMD Ryzen 7 6800H. It uses `-benchtime=100ms
-count=3 -cpu=1`; numbers are medians. Short SQLite samples have only two or
three iterations and are not suitable for drawing latency conclusions.

| Workload | Observer | ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Sequential, 16 steps | Disabled | 33,163 | 5,856 | 118 |
| Sequential, 16 steps | No-op | 33,519 | 6,176 | 119 |
| Sequential, 16 steps | JSON slog | 156,264 | 33,364 | 186 |
| Fan-out/join, width 32 | Disabled | 134,216 | 35,380 | 334 |
| Fan-out/join, width 32 | No-op | 146,157 | 35,972 | 335 |
| Fan-out/join, width 32 | JSON slog | 474,890 | 92,785 | 474 |
| Durable recovery, memory | Disabled | 52,835 | 9,038 | 159 |
| Durable recovery, memory | No-op | 61,289 | 9,382 | 161 |
| Durable recovery, memory | JSON slog | 327,076 | 49,895 | 279 |
| Durable recovery, SQLite | Disabled | 37,241,167 | 27,442 | 573 |
| Durable recovery, SQLite | No-op | 38,142,600 | 27,786 | 575 |
| Durable recovery, SQLite | JSON slog | 37,463,267 | 68,400 | 694 |

Observation allocates one session per public call and a Store wrapper when
persistence is configured. No-op observation therefore adds one allocation
without a Store and two with a Store in these cases. The enabled node worker
also captures observation metadata, increasing its closure size. Debug JSON
logging has its own allocation and formatting cost. Observers are synchronous;
their latency can affect the completion order of parallel callbacks.

Disabled-observer checks compared the existing benchmarks against commit
`ed8ac2f` on the same machine and toolchain, using three 100ms runs with one P:

| Existing workload | Before B/op | After B/op | Before / after allocs/op |
| --- | ---: | ---: | ---: |
| Sequential, 16 steps | 5,856 | 5,856 | 118 / 118 |
| Fan-out/join, width 32, serial | 35,617 | 35,617 | 338 / 338 |
| Durable sequential, memory, 16 steps | 41,511 | 41,511 | 291 / 291 |
| Durable sequential, SQLite, 16 steps | 63,432 | 63,432 | 1,207 / 1,207 |

The root sequential and fan-out checks also ran with eight Ps; allocation
counts remained unchanged. These short samples establish allocation behavior,
not a latency guarantee. Keep the repeatable commands above for longer timing
comparisons on an otherwise idle machine.

## Recoverable interruption comparison

The interruption implementation was compared with commit `8656f04` on
2026-10-09, using Go 1.26.5, Windows/amd64, and the same AMD Ryzen 7 6800H.
The baseline source was extracted into a temporary directory. Baseline and
candidate benchmarks ran sequentially with the same command:

```sh
go test -run '^$' -bench 'BenchmarkSequentialExecution/steps=16$|BenchmarkFanoutJoin/width=32/concurrency=serial$|BenchmarkDurableSequentialExecution/storage=memory/steps=16$' -benchmem -benchtime=1s -count=3 -cpu '1,4' .
```

Values below are medians of three samples. These workloads exercise ordinary
successful execution, including persisted recovery, where the new callback
interruption check should not allocate.

| Workload | Ps | Before / after ns/op | Before / after B/op | Before / after allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Sequential, 16 steps | 1 | 30,898 / 25,707 | 3,168 / 3,168 | 76 / 76 |
| Sequential, 16 steps | 4 | 54,987 / 50,284 | 4,102 / 4,101 | 79 / 79 |
| Fan-out/join, width 32, serial | 1 | 120,149 / 88,824 | 29,728 / 29,728 | 204 / 204 |
| Fan-out/join, width 32, serial | 4 | 224,907 / 155,076 | 29,729 / 29,729 | 204 / 204 |
| Durable sequential, memory, 16 steps | 1 | 94,005 / 78,746 | 38,664 / 38,664 | 232 / 232 |
| Durable sequential, memory, 16 steps | 4 | 181,107 / 133,394 | 39,608 / 39,612 | 235 / 235 |

Allocation counts were unchanged in these cases. Timing varied substantially
between initial 100ms samples and these longer samples; the lower candidate
medians do not establish a speedup. These measurements also do not quantify
interruption latency, which includes waiting for cancelled callbacks to exit.

## Production package coverage

The Linux and Windows CI jobs generate one combined statement profile with
`go test -coverpkg=./...` and run the private standard-library-only checker:

```sh
go test -coverpkg=./... -coverprofile="${TMPDIR:-/tmp}/lunegraph-coverage.out" ./...
go run ./internal/coveragecheck -profile "${TMPDIR:-/tmp}/lunegraph-coverage.out"
```

PowerShell:

```powershell
go test -coverpkg=./... "-coverprofile=$env:TEMP/lunegraph-coverage.out" ./...
if ($LASTEXITCODE -ne 0) { throw 'tests failed' }
go run ./internal/coveragecheck -profile "$env:TEMP/lunegraph-coverage.out"
```

The checker discovers source-bearing packages in the main module using
`go list -json ./...`. All production packages, including examples and their
ledger, must individually reach 85%. Pure test packages, the exact
`internal/storetest` helper, and the checker itself are excluded. The checker
has its own unit tests. Newly added production packages are included without
updating a package allowlist.

Different test packages can report the same source block. The checker merges
blocks by filename and source coordinates, counts each statement once, and
marks a block covered if any test covered it. It rejects malformed profiles,
inconsistent duplicate statement counts, and eligible packages with missing
or zero effective statements. The threshold uses integer ratios before
display rounding; aggregate coverage cannot hide an under-covered package.

On 2026-10-09, the Windows/amd64 working tree based on `02a415d` passed all
13 eligible packages. Their minimum was SQLite at 86.404%; the checker tests
reached 96.9%. These are combined-profile package values, rather than the
per-test-package percentages printed by `go test -coverpkg=./...`.

## Reliability checks

Run the correctness and static checks alongside benchmark work:

```sh
go test ./...
go test -race ./...
go vet ./...
```

The reliability baseline is the semantic test suite, including the SQLite process-crash and reopen tests in `checkpoint/sqlite`, commit-acknowledgement ambiguity tests, cross-process CAS tests, and execution tests for replay identities, cancellation, and failure handling. A change to execution or persistence semantics must preserve or update those checks. No benchmark score substitutes for them.

Application integration checks in `examples/effects` exercise node,
continuation, and join effects with real SQLite receipts and checkpoints.
They inject CAS rejection, errors before a checkpoint write, and lost
acknowledgements after a write; recovery verifies both the persisted graph
result and application effect count. A subprocess is killed after committing
an effect but before returning its node transition. Additional checks cover
cancellation in that interval, shared Runner/Store executions, duplicate
requests across ledger connections, original-result replay, request mismatch,
and transaction rollback when the receipt insert fails. These tests run in the
normal test suite on Windows and Linux; they do not establish a throughput or
capacity limit for the example application.

Explicit interruption checks additionally cover failure-policy bypass, stable
callback IDs, SQLite reopen, parallel cancellation and draining, continuation
input prefixes, and rollback of join candidates including nested merges.
The effects example interrupts after committing a node, continuation, or join
effect, then verifies receipt replay returns the original result without
applying the effect again. Cancellation, panic, Clone, Decode, and Store tests
ensure the interruption marker only controls explicitly returned errors from
effect-capable callbacks.

The capacity suite also checks bounded checkpoints after repeated branch
endings and local failures across budgeted loops and typed pauses, and newly
completed runs sharing a Runner
and Store. [History and completed-run measurements](capacity.md#retained-history-and-completed-executions)
separate deliberately persisted data from transient executor resources.
