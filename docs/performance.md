# Performance and graph-level benchmarks

These benchmarks measure graph definition, scheduling, checkpoint storage, and
observation through the public API. Application-specific arrival patterns,
admission policies, and production capacity are outside their scope. Results
depend on hardware, operating system, filesystem, Go version, and SQLite settings;
there is no cross-machine latency or throughput threshold.

For execution and persistence contracts, see
[execution semantics](execution-semantics.md) and the
[Store implementation guide](store-implementation.md).

## Benchmark commands

Run focused comparisons serially on the same machine and Go version:

```sh
go test -run '^$' -bench '^(BenchmarkSequentialExecution|BenchmarkFanoutJoin|BenchmarkConcurrentRuns|BenchmarkDurableSequentialExecution|BenchmarkDurableFanoutJoin)$' -benchmem -benchtime=300ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkCapacity' -benchmem -benchtime=300ms -count=3 -cpu '1,8' ./internal/capacitytest
go test -run '^$' -bench '^BenchmarkObservation$' -benchmem -benchtime=300ms -count=3 -cpu '1,8' ./internal/observationtest
go test -run '^$' -bench '^BenchmarkCheckpointCopyInto$' -benchmem -benchtime=300ms -count=3 -cpu '1,8' ./internal/model
go test -run '^$' -bench '^(BenchmarkJSONCheckpointEncoding|BenchmarkCheckpointCAS|BenchmarkSQLiteReopenRecover)$' -benchmem -benchtime=300ms -count=3 -cpu '1,8' ./checkpoint/...
```

The benchmark families cover:

- Sequential and fan-out execution; independent runs sharing a compiled Runner.
- Repeated durable recovery with Memory and SQLite Stores.
- Graph compilation, fan-out, nested groups, shared runs, paused populations,
  history, interruptions, and callback limiting.
- Checkpoint encoding/copying and disabled, no-op, and slog observation.

The graph-scale benchmarks use fixed public-API workloads and verify results;
they are diagnostics, not application capacity certifications. SQLite uses WAL
and `synchronous=FULL`, so filesystem flush behavior can dominate durable cases.

## Single-run checkpoint scaling (format 4)

`BenchmarkSingleRunCheckpoint` seeds one valid execution outside the timer. It
contains the stated number of waiting invocations, one activation group, one
ready loop node, and the same 16-entry application state for every invocation.
`load` reads a full checkpoint; `cold-recover` loads it and advances one node;
`warm-advance` continues a live handle after one untimed advance; `checkpoint`
materializes an independent full snapshot from that handle. Each advance accepts
one node commit while preserving all waiting branches. Setup executes the graph
through the public Runner, then creates a new run at revision 1; it does not
hand-assemble checkpoint topology.

Measurements below are medians of three samples at `-benchtime=150ms`,
`GOMAXPROCS=1`, on Windows/amd64, Go 1.27.0, AMD Ryzen 7 8845H (8 cores, 16
logical processors). Values are `ns/op / B/op / allocs/op`:

| Waiting invocations | Store | Load | Cold Recover | Warm Advance | Checkpoint |
| ---: | --- | ---: | ---: | ---: | ---: |
| 32 | Memory | 44,782 / 65,680 / 179 | 59,689 / 94,747 / 437 | 5,481 / 4,327 / 28 | 53,864 / 65,680 / 179 |
| 32 | SQLite | 355,377 / 125,516 / 1,093 | 1,189,332 / 157,110 / 1,448 | 728,259 / 8,263 / 157 | 67,724 / 65,680 / 179 |
| 256 | Memory | 523,993 / 410,320 / 1,301 | 740,783 / 591,422 / 3,296 | 8,148 / 4,326 / 28 | 486,070 / 410,320 / 1,301 |
| 256 | SQLite | 2,222,363 / 835,109 / 8,033 | 3,738,607 / 1,019,184 / 10,127 | 753,719 / 8,269 / 157 | 507,211 / 410,320 / 1,301 |
| 2,048 | Memory | 4,821,753 / 3,893,200 / 10,266 | 7,915,805 / 5,310,947 / 26,626 | 9,248 / 4,327 / 28 | 4,820,641 / 3,893,200 / 10,266 |
| 2,048 | SQLite | 18,464,044 / 7,338,020 / 64,064 | 23,386,762 / 8,759,337 / 80,529 | 703,899 / 8,269 / 157 | 4,456,485 / 3,893,200 / 10,266 |

Reproduce the table with:

```sh
go test -run '^$' -bench '^BenchmarkSingleRunCheckpoint$' -benchmem -benchtime=150ms -count=3 -cpu 1 ./internal/capacitytest
```

Warm Memory advances stay near 5–9 μs and allocate about 4.3 KB across these
checkpoint sizes because they check only the header and process one changed
node state. SQLite warm advances are about 0.7–0.75 ms here: they also perform
the header query and durable delta commit. Explicit checkpoint materialization
scales with the full state and has the same cost as full Memory Load. A separate
CPU 1 profile of SQLite cold recovery at 2,048 waiters showed cumulative
CPU samples concentrated in `sqlite.Store.Load` (68.6% of total samples) and
`encoding/json.Unmarshal` (50.6%; cumulative profile entries overlap). The
allocation profile attributed 70.2% cumulatively to `sqlite.Store.Load`, with
JSON decoding and map construction prominent below it. These profiles include
checkpoint materialization and state decoding; they do not measure time blocked
waiting for a filesystem flush. They do not by themselves justify changing the
SQLite Commit contract or predict a production SLO.

The workload serializes its single ready node and keeps payload size fixed.
It isolates record-count costs; it is not a multi-run throughput or production
capacity test. There is no cross-machine threshold.

## Recent runtime changes

### Runtime FanOut and checkpoint format 5

Measured base `6f8381f` against the format-5 implementation on Windows/amd64,
Go 1.26.5, AMD Ryzen 7 6800H. Runs were serial on the same machine. Values are
median `ns/op / B/op / allocs/op`:

| Existing workload | CPU | Format-4 base | Format-5 |
| --- | ---: | ---: | ---: |
| 16-step sequential | 1 | 30,294 / 3,992 / 81 | 32,181 / 4,008 / 81 |
| 16-step sequential | 8 | 27,162 / 6,680 / 81 | 23,954 / 7,192 / 81 |
| Width-32 static fan-out/join, default concurrency | 1 | 182,426 / 53,016 / 294 | 177,718 / 53,040 / 295 |
| Width-32 static fan-out/join, default concurrency | 8 | 246,023 / 56,439 / 301 | 216,101 / 56,974 / 302 |
| Durable width-32 Memory fan-out, default concurrency | 1 | 709,730 / 393,401 / 1,724 | 665,714 / 394,392 / 1,729 |
| Durable width-32 Memory fan-out, default concurrency | 8 | 644,322 / 397,062 / 1,732 | 709,657 / 398,605 / 1,737 |
| Warm Memory advance, 32 waiters | 1 | 10,401 / 4,326 / 28 | 11,279 / 4,342 / 28 |

Sequential and warm Memory latency samples overlap between versions and do not
establish a stable regression. The durable Memory FanOut benchmark runs one
dynamic parent Merge per cycle over a 16-entry map state. Format 5 copies the
accepted parent state before Merge and the Store clones the changed parent on
commit; that necessary ownership boundary accounts for about 1 KB and five extra
allocations per measured cycle here. Transition's branch descriptor also adds
a fixed buffer-size cost to existing scheduler workloads without adding an
allocation in the sequential case.

The new `BenchmarkDynamicFanOut` exercises zero, one, two, eight, 32 and 128
runtime branches targeting the same declared Worker, with serial and default
concurrency. Representative CPU-1 default-concurrency medians are:

| Branches | `ns/op / B/op / allocs/op` |
| ---: | ---: |
| 0 | 10,777 / 4,848 / 40 |
| 1 | 20,637 / 6,280 / 55 |
| 32 | 176,921 / 51,968 / 260 |
| 128 | 629,623 / 194,152 / 758 |

This workload has no old equivalent: the format-4 path could not express zero,
one or repeated targets as independent children. The repeated-target case
isolates invocation creation, per-child Clone, Join and checkpoint commits; it
does not include a real model provider or application state beyond one integer
per branch.

Reproduce these workloads with:

```sh
go test -run '^$' -bench '^BenchmarkSequentialExecution$/steps=16$' -benchmem -benchtime=150ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkFanoutJoin$/width=32/concurrency=default$' -benchmem -benchtime=150ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkDurableFanoutJoin$/storage=memory/width=32/concurrency=default$' -benchmem -benchtime=150ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkSingleRunCheckpoint$/storage=memory/waiters=32/mode=warm-advance$' -benchmem -benchtime=150ms -count=3 -cpu 1 ./internal/capacitytest
go test -run '^$' -bench '^BenchmarkDynamicFanOut$' -benchmem -benchtime=150ms -count=3 -cpu '1,8' .
```

These are same-host diagnostics, not production throughput promises. SQLite
continues to use schema 2; SQLite recovery benchmarks and process-kill tests
continue to exercise durable commits.

### Format 6 and runtime control-flow follow-ups

As a follow-up, the pre-FanOut base `f483b01` was compared with the cumulative
format-6 candidate `f426a3c`, which also includes runtime branch timeouts,
speculative joins and branch pruning. Runs used Windows/amd64, Go 1.26.5, AMD
Ryzen 7 6800H, and three serial 150 ms samples per case. Values are median
`ns/op / B/op / allocs/op`:

| Workload | CPU | Base | Candidate |
| --- | ---: | ---: | ---: |
| 16-step sequential | 1 | 25,378 / 3,992 / 81 | 28,379 / 4,200 / 81 |
| 16-step sequential | 8 | 28,138 / 6,680 / 81 | 27,304 / 7,736 / 81 |
| Width-32 fan-out/join, default concurrency | 1 | 147,093 / 53,016 / 294 | 146,727 / 67,112 / 292 |
| Width-32 fan-out/join, default concurrency | 8 | 166,652 / 56,466 / 301 | 192,790 / 71,428 / 299 |
| Durable width-32 Memory fan-out, default concurrency | 1 | 549,937 / 393,407 / 1,724 | 646,222 / 431,345 / 1,726 |
| Durable width-32 Memory fan-out, default concurrency | 8 | 661,681 / 397,073 / 1,732 | 731,146 / 435,910 / 1,735 |
| Warm Memory advance, 32 waiters | 1 | 9,394 / 4,326 / 28 | 13,148 / 4,534 / 28 |

The cumulative candidate keeps sequential allocation counts stable and reduces
the static fan-out counts by two, while increasing bytes per operation for the
fan-out and warm-checkpoint cases. CPU-8 and durable timings vary across samples;
these short runs do not establish a broad latency conclusion.

To locate the warm Memory change, the 32-waiter benchmark was run at each commit
with five serial 300 ms samples, CPU 1. Values are median
`ns/op / B/op / allocs/op`:

| Commit | Change | Warm Memory advance |
| --- | --- | ---: |
| `f483b01` | Pre-FanOut base | 9,271 / 4,327 / 28 |
| `bff31d3` | Runtime FanOut, format 5 | 10,859 / 4,343 / 28 |
| `aedc648` | Branch outcomes, format 6 | 11,394 / 4,503 / 28 |
| `58c1681` | Timeout controls | 10,977 / 4,535 / 28 |
| `f426a3c` | Speculative joins and pruning | 11,150 / 4,535 / 28 |

The base samples ranged from 8,644 to 9,633 ns/op; the FanOut samples ranged
from 10,148 to 11,915 ns/op. This run first shows the change at `bff31d3`, which
adds the branch slice to the public `Transition`; `B/op` rises by 16 while the
allocation count stays fixed. Format 6 adds persisted branch error and target
metadata to `Invocation`, and timeout controls add its deadline field. The
current candidate uses 208 more bytes per operation than the base, still with
28 allocations.

Five-second CPU and allocation profiles of the base and candidate show the same
broad path through `driveLive`, task preparation, state cloning and Memory Store
commit. The allocation profile includes invocation-slice resizing in both
versions, with the candidate carrying the larger persisted `Invocation` value;
the CPU comparison does not identify a single new algorithmic hotspot. The
measured cost follows the larger transition and checkpoint values needed by
the added runtime behavior. No runtime representation change is made from these
profiles alone because reducing the persisted footprint would require a
separate checkpoint representation design.

An internal compact worker-result experiment changed the warm Memory median from
12,207 to 11,606 ns/op and reduced it by 16 B/op, but the five-sample latency
ranges overlapped. Width-32 dynamic FanOut's default-concurrency median changed
from 145,749 to 151,545 ns/op, with one extra allocation and 8 more bytes per
operation. The experiment showed no repeatable latency gain and was not
retained.

The current format-6 `BenchmarkDynamicFanOut` run covers zero, one, two, eight,
32 and 128 repeated-target branches. The table gives CPU-1 default-concurrency
medians from three 150 ms samples, in `ns/op / B/op / allocs/op`:

| Branches | Format-6 candidate |
| ---: | ---: |
| 0 | 13,495 / 5,328 / 42 |
| 1 | 21,608 / 6,984 / 56 |
| 2 | 28,198 / 9,336 / 66 |
| 8 | 54,421 / 18,160 / 117 |
| 32 | 181,349 / 66,304 / 257 |
| 128 | 763,040 / 254,057 / 753 |

Format 4 has no equivalent for runtime-created groups, so these numbers have no
pre-FanOut latency comparison. Run the first four commands below in both the
`f483b01` and `f426a3c` checkouts to reproduce the cumulative comparison; the
dynamic benchmark is available only in the candidate checkout:

```sh
go test -run '^$' -bench '^BenchmarkSequentialExecution$/steps=16$' -benchmem -benchtime=150ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkFanoutJoin$/width=32/concurrency=default$' -benchmem -benchtime=150ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkDurableFanoutJoin$/storage=memory/width=32/concurrency=default$' -benchmem -benchtime=150ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkSingleRunCheckpoint$/storage=memory/waiters=32/mode=warm-advance$' -benchmem -benchtime=150ms -count=3 -cpu 1 ./internal/capacitytest
go test -run '^$' -bench '^BenchmarkDynamicFanOut$' -benchmem -benchtime=150ms -count=3 -cpu '1,8' .
```

### Executor responsibility separation

Compared base `3bf2e69` with the ready-queue/candidate-workspace refactor on
Windows/amd64, Go 1.26.5, AMD Ryzen 7 6800H. Benchmarks ran serially on the same
machine. Values are medians of `ns/op / B/op / allocs/op`:

| Workload | CPU | Base | Refactor |
| --- | ---: | ---: | ---: |
| 16-step sequential | 1 | 26,615 / 3,992 / 81 | 27,722 / 3,992 / 81 |
| 16-step sequential | 8 | 30,765 / 6,680 / 81 | 25,831 / 6,680 / 81 |
| Width-32 fan-out/join, default concurrency | 1 | 161,138 / 53,016 / 294 | 137,506 / 53,016 / 294 |
| Width-32 fan-out/join, default concurrency | 8 | 187,966 / 56,454 / 301 | 169,437 / 56,465 / 301 |
| Durable width-32 Memory fan-out, default concurrency | 1 | 548,255 / 393,406 / 1,724 | 494,061 / 393,416 / 1,724 |
| Durable width-32 Memory fan-out, default concurrency | 8 | 834,697 / 397,143 / 1,733 | 801,515 / 397,140 / 1,733 |
| Durable width-32 SQLite fan-out, default concurrency | 1 | 82,255,600 / 617,536 / 9,630 | 84,965,400 / 617,608 / 9,634 |
| Durable width-32 SQLite fan-out, default concurrency | 8 | 84,669,900 / 622,488 / 9,633 | 86,168,750 / 626,208 / 9,644 |
| Warm Memory advance, 32 waiters | 1 | 10,362 / 4,326 / 28 | 10,172 / 4,326 / 28 |
| Warm Memory advance, 2,048 waiters | 1 | 15,601 / 4,327 / 28 | 12,145 / 4,327 / 28 |
| Warm SQLite advance, 32 waiters | 1 | 2,503,381 / 9,374 / 179 | 2,332,837 / 9,374 / 179 |
| Warm SQLite advance, 2,048 waiters | 1 | 2,506,685 / 9,419 / 179 | 2,346,057 / 9,437 / 179 |

Sequential measurements used five 300 ms samples after an initial noisy batch;
CPU-8 durable Memory used five 1 s samples from the already compiled binaries.
The remaining rows used three 150 ms samples. Sequential, non-persistent
fan-out, durable Memory fan-out and warm advances preserve their allocation
counts. Timing ranges overlap, so these medians are not a claim of throughput
improvement. SQLite fan-out had only two iterations per sample and the base
included a 388 ms outlier; its small allocation/timing differences are inconclusive.

The initial CPU-8 durable Memory medians were 609,807 versus 667,627 ns/op. A
five-sample 300 ms repeat later measured 849,030 versus 1,101,479 ns/op. The
longer final binary runs produced the table above, with overlapping ranges
(base 812,868–933,164; refactor 675,834–864,958 ns/op) and identical allocations.
Separate 1 s CPU profiles concentrated samples in runtime locking/scheduling,
map operations, state copying and GC; candidate collection/change was a small
part of the sampled CPU. These short profiles cannot attribute wall-time noise
to a specific cause. The varying latency comparisons do not establish a stable
regression or improvement; the allocation regression from separate collection
maps was removed by workspace reuse and local capacity reservation.

Candidate collection and diffing reuse one workspace ID map in sequential
phases. A wide join reserves its known local child count when collection crosses
the existing indexing threshold. This prevents extra map-growth allocations
without loading or indexing unrelated invocations. The single-invocation fast
path and queue/index thresholds are unchanged.

Reproduce the focused workloads with:

```sh
go test -run '^$' -bench '^BenchmarkSequentialExecution$/steps=16$' -benchmem -benchtime=300ms -count=5 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkFanoutJoin$/width=32/concurrency=default$' -benchmem -benchtime=150ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkDurableFanoutJoin$/storage=(memory|sqlite)/width=32/concurrency=default$' -benchmem -benchtime=150ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkDurableFanoutJoin$/storage=memory/width=32/concurrency=default$' -benchmem -benchtime=1s -count=5 -cpu 8 .
go test -run '^$' -bench '^BenchmarkSingleRunCheckpoint$/storage=(memory|sqlite)/waiters=(32|2048)/mode=warm-advance$' -benchmem -benchtime=150ms -count=3 -cpu 1 ./internal/capacitytest
```

### Continuation identity and nil-input changes

Compared base `721e087` with the continuation fix and required ResumeInput.CallID
on Windows/amd64, Go 1.26.5, AMD Ryzen 7 6800H. Runs were serial on the same
machine with CPU 1. Values below are median `ns/op / B/op / allocs/op`:

| Workload | Base | Changed |
| --- | ---: | ---: |
| Scalar continuation adapter | 10.94 / 0 / 0 | 10.69 / 0 / 0 |
| Three-int struct adapter | 32.96 / 24 / 1 | 34.15 / 24 / 1 |
| Nil-interface adapter | Unsupported | 48.16 / 16 / 1 |
| 128 paused runs, Memory advance | 22,557 / 11,286 / 73 | 21,929 / 11,286 / 73 |
| 128 paused runs, SQLite advance | 5,039,992 / 24,145 / 459 | 4,854,618 / 24,139 / 459 |

Adapter measurements used three 300 ms samples. The adapter benchmark file was
also run against the base implementation for scalar/struct cases; nil interface
inputs previously panicked and have no valid latency baseline. Wrapping the
decoded value preserves its generic type and nil semantics; an interface wrapper
uses a 16-byte allocation here. Scalar and struct allocation counts are unchanged.

Paused advances used 150 ms samples. SQLite used three samples. An initial Memory
comparison measured 20,491 versus 24,496 ns/op; isolated five-sample repeats on
the changed tree and a clean base checkout produced the Memory medians above,
with overlapping ranges (base 20,811–24,860; changed 20,535–24,353 ns/op).
These samples do not establish a repeatable latency regression or a throughput
improvement. Recovery allocation counts stayed unchanged. The fixture now keeps
each run's original waiting CallID as application bookkeeping and supplies it on
resume; this does not add a Store read.

Reproduce the changed measurements with:

```sh
go test -run '^$' -bench '^BenchmarkContinuationAdapter$' -benchmem -benchtime=300ms -count=3 -cpu 1 ./internal/definition
go test -run '^$' -bench '^BenchmarkCapacityPaused$/store=memory/runs=128/mode=advance$' -benchmem -benchtime=150ms -count=5 -cpu 1 ./internal/capacitytest
go test -run '^$' -bench '^BenchmarkCapacityPaused$/store=sqlite/runs=128/mode=advance$' -benchmem -benchtime=150ms -count=3 -cpu 1 ./internal/capacitytest
```

The adapter benchmark isolates Decode/Apply adaptation. The paused workload
includes recovery, state copying and two commits; SQLite also includes durable
flushes. Neither is an application production-capacity claim.

### Earlier format-4 comparison

The comparison below uses baseline `a3ad161` and format-4 candidate `ab2c8e0` on
Windows/amd64, Go 1.27.0, AMD Ryzen 7 8845H. Baselines used three 300 ms samples.
Candidate sequential and non-persistent fan-out cases used five samples; durable
cases used three samples except SQLite width-32 fan-out, which used five isolated
samples. CPU counts are shown per case. Values are median `ns/op / B/op / allocs/op`:

| Workload | Baseline | Format-4 candidate |
| --- | --- | --- |
| 16-step sequential, CPU 1 | 16,667 / 3,184 / 76 | 16,690 / 3,912 / 79 |
| Width-32 fan-out/join, CPU 8 | 99,738 / 32,137 / 211 | 143,894 / 56,511 / 299 |
| Durable 16-step Memory recovery, CPU 1 | 50,320 / 38,678 / 232 | 55,783 / 49,790 / 299 |
| Durable width-32 Memory fan-out, CPU 8 | 1,606,407 / 1,418,080 / 5,326 | 488,394 / 358,704 / 1,597 |
| Durable width-32 SQLite fan-out, CPU 8 | 26,388,500 / 488,246 / 22,116 | 83,439,275 / 509,380 / 7,560 |
| Durable 16-step SQLite recovery, CPU 8 | 9,208,626 / 50,117 / 941 | 10,310,683 / 104,593 / 1,977 |

Ordinary sequential latency is near baseline, with higher allocation volume.
Memory durable width-32 fan-out is about 3.3x faster and uses 70% fewer
allocations. Non-persistent width-32 fan-out is about 44% slower, with 76% more
bytes and 42% more allocations. Durable sequential Memory and SQLite cases use
about 29% and 110% more allocations.

The final SQLite width-32 fan-out samples ranged from 77 to 130 ms, while an
earlier same-host candidate batch measured about 26 ms. Treat that latency
comparison as inconclusive until repeated on an otherwise idle machine; the
measured allocation count was about 66% lower. These local results are evidence
for review, not production SLOs.

## Repository checks

The project checks formatting, all tests, race behavior, static analysis, and an
85% statement-coverage minimum per production package:

```sh
gofmt -l .
go test ./...
go test -race ./...
go vet ./...
go test -coverpkg=./... -coverprofile=coverage.out ./...
go run ./internal/coveragecheck -profile coverage.out
```

CI runs on Windows and Linux; the race job runs on Linux. Keep benchmark outputs
and profiles local unless a reproducible result is needed in project docs.
