# Capacity and sustained execution baseline

Capacity is a measured property of a workload, Store, and machine. This suite
supports the goal of serving as a generic graph substrate for a large Agent OS;
it does not introduce agent concepts or claim a production capacity limit.

All fixtures live in `internal/capacitytest`, separated into state, graph,
Store, worker, benchmark, isolation, lifecycle, and measurement files. They use
the public API, a strongly typed state containing a mutable 16-entry map, and
a deep-copy Clone. No runtime API or persistence boundary changes are required.

## Repeatable workloads

| Benchmark | Sizes | One measured operation |
| --- | --- | --- |
| `BenchmarkCapacityCompile` | 128 / 1,024 / 8,192 sequential nodes | Compile an already constructed definition. A complete execution validates the fixture before timing. |
| `BenchmarkCapacityFanout` | 32 / 128 / 512 branches; node concurrency 1 / 8 | Start and complete one fan-out/join round without a Store. |
| `BenchmarkCapacityShared` | 16 / 64 / 256 fixed runs; 1 / 8 / 32 caller workers | Advance every run once using one shared Runner, with no Store, Memory, or SQLite. |
| `BenchmarkCapacityPaused` | Memory: 128 / 1,024 / 8,192; SQLite: 128 / 1,024 paused runs | Recover one run without input (`inspect`) or apply a typed continuation and advance it to the next wait (`advance`). |

Graph construction, Store creation, initial checkpoints, and caller worker
construction are outside timing. Assertions inside each measured operation
verify execution progress; persisted results are reloaded after timing. The
durable cases reuse fixed run IDs, so iterations do not grow the row count.
Caller workers never advance the same run concurrently. CAS remains an
optimistic commit check, not a run lease.

`ns/op`, `B/op`, and `allocs/op` include assertion and harness work. The shared
benchmark is a **batch** score: divide by `executions/op` for average cost per
run; it is not the latency of an individual concurrent call. `-cpu` sets
GOMAXPROCS independently of node concurrency and caller workers.

Paused benchmarks also report `seed-heap-B`, `paused-runs`, and
`idle-goroutines`. Heap is the positive difference of process-wide Go live heap
after GC before and after seeding, including the harness. It excludes database
file size, SQLite native allocations, filesystem cache, and resident memory.
The idle goroutine count is process-wide. A paused run does not own a waiting
goroutine in these fixtures; Memory deliberately retains its latest checkpoint.

```sh
go test -run '^$' -bench '^BenchmarkCapacity' -benchmem -benchtime=100ms -count=3 -cpu '1,8' ./internal/capacitytest
```

Short SQLite cases may execute only once per sample. Use longer measurement
intervals and an otherwise idle machine when comparing implementations.

## Bounded correctness checks

Ordinary `go test ./...` and `go test -race ./...` include:

- 128 branches with mutable maps, deterministic join results, and node
  concurrency at most eight.
- 64 independently waiting runs sharing one Runner and Store, advanced by
  eight caller workers with Memory and SQLite.
- 64 runs sharing a Runner and Store while one is cancelled, one commits a
  `FailExecution` terminal, and one receives an injected CAS conflict. Other
  runs complete; conflict recovery replays the same CallID without committing
  the first attempt's state, and callbacks drain before public calls return.
- Repeated fan-out, join, step budgets, typed continuation input, and recovery:
  Memory uses width 32 for 32 rounds; SQLite uses width eight for eight rounds.
  Checks cover branch isolation, CallIDs, revisions, cumulative steps, wait
  boundaries, active groups, and retained terminal/failure history.
- Bounded latency sample accounting and invalid soak duration settings.

These supplement the crash, replay, uncertain acknowledgement, and cross-process
CAS tests described in [performance.md](performance.md). They run in existing
CI jobs without a scheduled workflow or a fixed speed/heap threshold.

## Manual sustained execution

`TestCapacitySoak` is skipped unless `LUNEGRAPH_SOAK_DURATION` is set. A malformed,
zero, or negative Go duration fails. The duration applies **to each Store**.

```sh
LUNEGRAPH_SOAK_DURATION=60s go test -run '^TestCapacitySoak$' -count=1 -v -timeout 5m ./internal/capacitytest
```

PowerShell:

```powershell
$env:LUNEGRAPH_SOAK_DURATION = '60s'
go test -run '^TestCapacitySoak$' -count=1 -v -timeout 5m ./internal/capacitytest
Remove-Item Env:LUNEGRAPH_SOAK_DURATION
```

Choose `-timeout` to allow both Store intervals, fixture setup, draining, and
recovery. The workload has 64 fixed runs, eight caller workers, width-eight
fan-out, node concurrency eight, and a wait every four rounds. Each successful
call commits one round: ten node outcomes plus any continuation acceptance.
Joins have an outgoing edge, so history does not accumulate. The fixtures model
bounded checkpoint shape, not an application that retains every event or artifact.

Resources are sampled every 500 ms and JSON progress is logged every five
seconds. Only the latest 1,024 successful call latencies are retained. Reported
p50/p95 are sorted rolling-window order statistics at `floor((n-1)*q)`; they
are not whole-run or per-node percentiles. Heap and goroutine peaks are sampled
peaks, so brief spikes can be missed. Throughput counts only fully successful
calls; committed work inside a cancelled call is excluded from step totals.

At expiry the context cancels active calls, all workers and the sampler drain,
and GC resource snapshots are taken with the Store still alive. Every checkpoint
is then reloaded: cancellation or an uncertain acknowledgement may have left
part of a round committed. A fresh context finishes only the current partial
round (or a round whose continuation was already accepted). The final revision,
state, wait boundary, group/history shape, and stored counters are verified.
These post-drain recovery calls are excluded from measured throughput and
latency. Resource snapshots after recovery retain the live Store explicitly.

Intentional retained checkpoints, harness bookkeeping, caches, and runtime
initialization must be distinguished from transient leaks. A one-minute test
does not prove leak freedom or production durability over days.

## Measured sample

Collected on 2026-09-30 with Go 1.26.5, Windows/amd64, and an AMD Ryzen 7 6800H.
The benchmark command above completed 168 samples: 28 workloads, two
GOMAXPROCS settings, three repetitions. Tables below report the eight-P samples:
latency is median [observed range]; allocation columns are medians. These are
short diagnostics, not pass/fail limits. SQLite shared batches with 64 or 256
runs have just one iteration per sample.

### Definition and fan-out

| Workload | ns/op, median [range] | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Compile, 128 nodes | 174,466 [164,721–193,900] | 118,101 | 690 |
| Compile, 1,024 nodes | 2,219,113 [2,035,510–2,606,775] | 975,248 | 5,201 |
| Compile, 8,192 nodes | 18,257,217 [16,884,233–18,978,500] | 7,817,841 | 41,249 |
| Fan-out, width 32, concurrency 1 | 512,869 [488,826–529,909] | 139,553 | 753 |
| Fan-out, width 32, concurrency 8 | 395,991 [389,367–396,382] | 140,430 | 753 |
| Fan-out, width 128, concurrency 1 | 2,707,563 [2,562,600–3,230,660] | 539,075 | 2,780 |
| Fan-out, width 128, concurrency 8 | 2,597,635 [2,505,837–2,597,716] | 539,896 | 2,779 |
| Fan-out, width 512, concurrency 1 | 21,617,100 [20,515,983–21,664,133] | 2,149,792 | 11,619 |
| Fan-out, width 512, concurrency 8 | 22,980,380 [22,258,120–23,520,320] | 2,150,614 | 11,620 |

### Shared Runner and Store

These scores are for one full batch, with one node and one continuation per run.

| Store | Runs / caller workers | ns/op, median [range] | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| None | 16 / 1 | 265,514 [260,231–275,769] | 62,596 | 609 |
| None | 64 / 8 | 243,313 [233,982–266,623] | 250,491 | 2,448 |
| None | 256 / 32 | 1,040,537 [976,824–1,122,450] | 995,855 | 9,220 |
| Memory | 16 / 1 | 393,832 [387,305–412,538] | 119,515 | 875 |
| Memory | 64 / 8 | 477,507 [452,658–510,328] | 477,957 | 3,490 |
| Memory | 256 / 32 | 2,162,280 [2,088,198–2,407,221] | 1,907,425 | 13,572 |
| SQLite | 16 / 1 | 73,893,600 [71,241,700–76,836,350] | 211,780 | 3,896 |
| SQLite | 64 / 8 | 285,898,900 [283,214,200–290,549,700] | 879,352 | 16,177 |
| SQLite | 256 / 32 | 1,201,352,400 [1,183,552,500–1,497,964,900] | 3,508,048 | 64,646 |

### Paused population

Each score recovers one paused run from the named population.

| Store | Runs / operation | ns/op, median [range] | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Memory | 128 / inspect | 3,900 [3,727–4,928] | 2,861 | 17 |
| Memory | 128 / advance | 18,673 [18,653–19,918] | 7,450 | 53 |
| Memory | 1,024 / inspect | 4,984 [4,801–5,181] | 2,862 | 17 |
| Memory | 1,024 / advance | 22,486 [20,783–22,745] | 7,450 | 53 |
| Memory | 8,192 / inspect | 4,808 [4,698–5,668] | 2,860 | 17 |
| Memory | 8,192 / advance | 17,938 [16,572–19,694] | 7,443 | 53 |
| SQLite | 128 / inspect | 72,088 [70,638–81,289] | 6,548 | 104 |
| SQLite | 128 / advance | 4,462,204 [4,446,746–4,677,996] | 13,317 | 243 |
| SQLite | 1,024 / inspect | 59,296 [56,661–67,662] | 6,547 | 104 |
| SQLite | 1,024 / advance | 4,522,223 [4,436,688–4,698,438] | 13,308 | 243 |

The inspect cases report these post-seed heap differences and process-wide
idle goroutine counts; heap values are medians [ranges] over three repetitions.

| Store | Paused runs | seed-heap-B | Idle goroutines |
| --- | ---: | ---: | ---: |
| Memory | 128 | 220,552 [220,552–220,776] | 3 |
| Memory | 1,024 | 1,766,032 [1,765,744–1,766,128] | 3 |
| Memory | 8,192 | 14,134,224 [14,133,696–14,134,576] | 3 |
| SQLite | 128 | 39,136 [38,376–40,936] | 4 |
| SQLite | 1,024 | 243,328 [241,968–246,416] | 4 |

SQLite stores checkpoints on disk rather than retaining their user maps in Go
heap; the SQLite rows above therefore do not measure total storage cost.

### Sustained execution sample

The fixed-workload test passed for 60 seconds **per Store** with
GOMAXPROCS eight. The exact command was the manual command above with `-cpu 8`.
Warmup, post-drain recovery, and their calls are outside the measured totals.
There are 64 coexisting runs and at most eight concurrent public calls.

| Store | Successful calls | Successful-call node steps | Calls/s | Rolling p50 / p95, ms | Cancelled calls |
| --- | ---: | ---: | ---: | --- | ---: |
| Memory | 423,146 | 4,231,460 | 7,052.31 | 1.0775 / 1.8674 | 8 |
| SQLite | 2,470 | 24,700 | 41.16 | 183.7852 / 270.3258 | 8 |

Both latency windows contain the latest 1,024 successful calls, rather than
the whole duration. Rates use elapsed time through worker drain (60.0011 and
60.0027 seconds respectively), including assertions and measurement overhead.

| Store | Before seed GC heap, B | After seed GC heap, B | Sampled peak heap, B | After drain GC heap, B | After recovery GC heap, B |
| --- | ---: | ---: | ---: | ---: | ---: |
| Memory | 420,392 | 656,776 | 3,610,728 | 842,152 | 777,832 |
| SQLite | 685,416 | 730,976 | 3,146,528 | 773,168 | 715,592 |

| Store | Before / after seed goroutines | Sampled peak goroutines | After drain / recovery goroutines |
| --- | --- | ---: | --- |
| Memory | 3 / 3 | 60 | 3 / 3 |
| SQLite | 4 / 4 | 15 | 4 / 4 |

Every run passed the final reload and round-boundary checks. Sampled heap did
not show sustained growth during these intervals, and goroutine counts returned
to their pre-worker levels. Post-GC heap differences include intentional live
checkpoints, partial-round state before recovery, and process/runtime caches.
SQLite starts after the Memory subtest in the same process, so the two heap
baselines are not independent process measurements. Longer soaks and application
state shapes are needed before drawing conclusions about production retention.

Verification passed with `go test ./...`, `go test -race ./...`, and
`go vet ./...`. A separate one-second-per-Store manual soak also passed under
the race detector with `-cpu 8`; the ordinary race command skips the opt-in soak.

### Profile and next optimization priority

A separate profile of the width-512, concurrency-eight, no-Store workload used
`-benchtime=3s -cpu=8`. Profiles are process-wide and include fixture setup and
benchmark calibration. Profile files and the generated binary were kept in a
temporary directory outside the repository.

```sh
go test -run '^$' -bench '^BenchmarkCapacityFanout/width=512/concurrency=8$' -benchtime=3s -cpu 8 -cpuprofile /tmp/capacity-cpu.pprof -memprofile /tmp/capacity-memory.pprof -o /tmp/capacity.test ./internal/capacitytest
go tool pprof -top /tmp/capacity-cpu.pprof
go tool pprof -top -sample_index=alloc_space /tmp/capacity-memory.pprof
go tool pprof -top -sample_index=inuse_space /tmp/capacity-memory.pprof
```

Use equivalent temporary paths on Windows. The sampled CPU profile attributes
24.1% cumulative time to `nextReady`, 19.9% to `indexedInvocation`, 17.5% to
`invocationNumber`, and 12.2% to `model.copyInvocations`. These cumulative stacks
overlap and must not be added. The allocation profile attributes 68.6% of bytes
to the fixture's map Clone, 9.2% to `appendInvocation`, and 3.2% to invocation
slice resizing. End-of-profile retained heap samples show runtime/platform
initialization, with no application checkpoint allocation site standing out;
sampling cannot prove absence of a leak.

Priorities supported by this baseline:

1. Examine wide-graph scheduling: repeated ready scans, numeric invocation-ID
   parsing, lookup, and structure copying before adding further core features.
   Preserve ordering, cancellation/draining, and replay identities; compare
   widths 32, 128, and 512 on the same host after any change.
2. Treat application Clone cost separately from scheduler overhead. Deep map
   copies are necessary for this workload's independent mutable branches;
   removing those copies would change its isolation contract.
3. Keep SQLite commit and checkpoint boundaries explicit. More caller workers
   do not remove the single-writer/FULL-sync cost visible in these samples.
   Changing checkpoint frequency requires a separately justified semantics
   decision. Earlier durable profiles in [performance.md](performance.md)
   identify the SQLite commit/flush path.

The samples establish exercised sizes, recovery costs, and optimization inputs.
They do not establish a maximum number of executions, a distributed scheduler,
a run lease, or an Agent OS production readiness claim.
