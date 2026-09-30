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
| `BenchmarkCapacityGroups` | 8 / 32 / 128 nested groups, eight branches each; node concurrency 1 / 8 | Resume an immutable seed with all activations established and finish both levels of joins without a Store. Includes one extra root group. |
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
- Nine nested activations recovered across seven-step budgets with Memory and
  SQLite, node concurrency one/eight, ordered merges, and final state validation.
- Continuation input batches with join-input Clone failure, rejected join CAS,
  or terminal join failure: prior inputs remain committed; continuation and join
  replay retain their CallIDs. These run against both Memory and SQLite.

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

## Scheduling optimization sample

The follow-up uses the same Go 1.26.5, Windows/amd64, Ryzen 7 6800H host.
Baseline source is commit `fcf5732`. Both versions were measured serially;
baseline executables and profiles were kept outside the repository. Numeric
ordering and string-keyed invocation lookup are now transient call-local
metadata. Checkpoint format, cursor commits, state copying, and Store commit
frequency are unchanged. See [performance.md](performance.md) for index lifetime
and complexity details.

```sh
go test -run '^$' -bench '^BenchmarkCapacityFanout$' -benchmem -benchtime=500ms -count=5 -cpu '1,8' ./internal/capacitytest
go test -run '^$' -bench '^BenchmarkSequentialExecution$' -benchmem -benchtime=500ms -count=5 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkDurableFanoutJoin/storage=(memory|sqlite)/width=(2|8|32)/concurrency=default$' -benchmem -benchtime=200ms -count=3 -cpu '1,8' .
```

### Wide fan-out comparison

Each latency is median [observed range] in ns/op over five samples. Percentages
describe these medians, not a machine-independent speed guarantee.

| Width | Node concurrency | Ps | Before ns/op | After ns/op | Median reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| 32 | 1 | 1 | 299,126 [263,895–368,317] | 269,985 [242,381–331,105] | 9.7% |
| 32 | 1 | 8 | 503,300 [499,789–514,271] | 462,647 [461,400–521,071] | 8.1% |
| 32 | 8 | 1 | 334,186 [309,312–357,352] | 333,269 [308,951–336,535] | 0.3% |
| 32 | 8 | 8 | 339,880 [333,512–344,969] | 369,584 [368,178–372,841] | -8.7% |
| 128 | 1 | 1 | 1,701,660 [1,631,198–1,778,724] | 1,619,514 [1,594,328–1,700,793] | 4.8% |
| 128 | 1 | 8 | 2,019,944 [1,926,501–2,287,198] | 2,322,208 [2,278,100–2,493,258] | -15.0% |
| 128 | 8 | 1 | 1,983,523 [1,948,988–2,201,669] | 1,464,765 [1,427,826–1,508,364] | 26.2% |
| 128 | 8 | 8 | 1,919,454 [1,880,343–1,931,553] | 1,910,323 [1,889,788–1,936,119] | 0.5% |
| 512 | 1 | 1 | 15,411,008 [14,666,428–16,052,816] | 13,184,581 [12,874,865–13,967,790] | 14.4% |
| 512 | 1 | 8 | 16,879,189 [15,996,261–17,193,886] | 15,667,738 [15,117,350–16,440,994] | 7.2% |
| 512 | 8 | 1 | 13,536,044 [12,871,871–13,982,114] | 9,987,876 [9,666,797–10,413,644] | 26.2% |
| 512 | 8 | 8 | 18,306,437 [17,297,442–18,914,950] | 15,526,667 [15,460,228–16,199,767] | 15.2% |

The transient string-keyed map and numeric ordering add about 1,792 B/op at
width 32, 6,528 B/op at width 128, and 27,264 B/op at width 512, about 1.2–1.3%
of total allocated bytes in this fixture. Allocation counts increase by one or
two per operation in these samples. The index uses O(invocation count) space;
this is a speed/space tradeoff, not an allocation reduction.

The two slower rows prompted a focused check of widths 32/128 with eight Ps,
alternating baseline and new binaries three times with 500ms per case. The
following medians [ranges] did not reproduce those regressions; the disagreement
limits any latency conclusion for narrower graphs.

| Width / node concurrency | Before ns/op | After ns/op |
| --- | ---: | ---: |
| 32 / 1 | 403,231 [358,096–403,858] | 362,743 [349,557–382,174] |
| 32 / 8 | 318,871 [302,031–320,754] | 307,516 [295,781–312,656] |
| 128 / 1 | 2,011,067 [1,978,227–2,095,729] | 1,826,145 [1,722,151–1,826,955] |
| 128 / 8 | 1,851,706 [1,732,077–2,022,106] | 1,531,905 [1,458,415–1,695,639] |

### Small and durable cases

Sequential 1/16/128-step executions retain allocation counts 13/118/902 on
both one and eight Ps. One-P B/op medians are respectively 1,256/5,856/40,289
before and 1,256/5,856/40,290 after, with an observed difference of one byte.
One-step/eight-P latency medians were 4,744 before [4,529–4,955] and 4,940 after
[4,681–6,491]. Longer sequential cases had lower measured medians, but host
variation is substantial and they do not use the large-set index.

Durable comparison below reports eight-P medians from three 200ms samples.
Width eight has **nine** invocations including its group parent and therefore
uses the large-set index. Rebuilding and maintaining metadata in durable rounds
can add more allocations than in a single nonpersistent fan-out.

| Store / width | Before ns/op | After ns/op | Before → after B/op | Before → after allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Memory / 2 | 67,314 | 64,479 | 26,795 → 26,783 | 174 → 174 |
| Memory / 8 | 238,193 | 203,757 | 136,337 → 137,585 | 680 → 683 |
| Memory / 32 | 2,208,061 | 2,296,249 | 1,419,260 → 1,424,638 | 5,517 → 5,520 |
| SQLite / 2 | 9,205,488 | 9,828,996 | 40,714 → 40,458 | 678 → 678 |
| SQLite / 8 | 23,497,367 | 23,639,811 | 171,394 → 171,753 | 3,502 → 3,505 |
| SQLite / 32 | 84,292,400 | 85,600,300 | 1,488,925 → 1,494,168 | 38,611 → 38,616 |

SQLite width-32 samples had only two or three measured iterations. Durable
timings remain Store- and scheduler-dependent; this change does not establish
a persistence speedup. One-P durable Memory width-eight medians increased from
179,240 to 209,432 ns/op, while eight-P medians decreased. These mixed results
are retained rather than generalized into an improvement claim.

### Profile and semantic checks

Fresh baseline and new profiles used the same width-512/concurrency-eight
selection with `-benchtime=3s -cpu=8`; setup and calibration are included.
Baseline `nextReady` accounted for 22.6% cumulative CPU time; new `selectReady`
accounted for 0.16%. Numeric parsing no longer appears on the large lookup or
dispatch path. Remaining cumulative stacks include invocation lookup (18.4%),
`copyInvocations` (19.9%), and `settleGroups` (23.7%); these overlap and cannot
be added. Relative shares describe separate sampled executions, not absolute
time savings. The new allocation profile still attributes 67.5% of bytes to
map Clone. Retained-heap samples show runtime/platform sites rather than
application index or checkpoint allocations.

Tests compare selection against an independent eligible-ID ordering oracle
through 1,024 fixed-seed mixed-state/topology stages. Explicit cases cover
unordered IDs, numeric rather than lexical order, near-limit IDs, cursor
wrap/removal, active membership, compaction, buffer exchange, and index release.
Memory and SQLite integration tests verify wide round-robin starts across
one-step budgets and typed wait/resume. Nested group failure tests inject a CAS
conflict after candidate compaction and join: the last acknowledged checkpoint
retains both groups, and recovery replays identical node and join CallIDs before
completing unaffected branches.

The optimized version passed the 60-second-per-Store soak with eight Ps and
the same 64-run/eight-caller/width-eight workload. Results below use the
post-drain timing and the latest 1,024 successful call latencies.

| Store | Successful calls | Calls/s | Rolling p50 / p95, ms | Cancelled calls | Sampled peak heap, B | After drain / recovery GC heap, B | Seed / peak / drain goroutines |
| --- | ---: | ---: | --- | ---: | ---: | --- | --- |
| Memory | 299,161 | 4,985.97 | 1.0674 / 1.8018 | 8 | 4,527,848 | 851,448 / 767,080 | 3 / 53 / 3 |
| SQLite | 2,441 | 40.68 | 186.9521 / 289.1620 | 8 | 3,101,744 | 787,928 / 749,600 | 4 / 14 / 4 |

All checkpoints passed reload and partial-round recovery, and goroutines
returned to seed levels. Memory's interval includes transient latency spikes
(a progress window had p95 65.2 ms); its total throughput is lower than the
earlier sample and does not establish a throughput improvement or isolate the
cause of that difference. These soaks validate recovery/resource behavior;
focused before/after benchmarks and profiles provide the optimization evidence.

Full tests, the race suite, and vet passed. A separate one-second-per-Store
manual soak under the race detector also passed, exercising concurrent wide
executions sharing a Runner and Store with the new transient indexes.

The remaining optimization candidates are checkpoint structure copying and
repeated group-readiness lookup. Their ownership and commit boundaries need
separate designs; this change leaves both intact.

## Group readiness optimization sample

This comparison uses baseline commit `e29d65c` and the confirmed-terminal
prefix implementation on 2026-09-30, Go 1.26.5, Windows/amd64, AMD Ryzen 7 6800H.
Baseline binaries were built from an archive of that commit with the identical
new benchmark fixture copied in. Results are machine-specific measurements,
not a capacity guarantee or an exactly-once side-effect claim.

The executor now separates transition/routing, join application, and readiness
progress into distinct files. Each drive or continuation-input batch owns its
progress. Once a child is joined, ended, or failed, it cannot execute again
within that activation. A cursor skips that confirmed prefix on subsequent
checks; a missing or nonterminal child still blocks the group. Recovery rebuilds
all metadata from the checkpoint. Errors and rejected commits discard the local
cache along with the unsuccessful candidate.

Zero/one group uses an inline record. Multiple groups use ordered ID/prefix
records for checks and an ID map to preserve progress when topology changes.
Identity checks are combined with the readiness traversal; the map is consulted
only during reconciliation. Removed IDs are cleared; shrinking to one group
releases both map and records. Space follows the call's peak group count, with
no state values or invocation pointers retained. Readiness can still traverse
all groups and repeatedly inspect an unresolved child; it is not universally
constant time. Group choice and join input order continue to follow checkpoint
slice order, including nested cascading joins and failure compaction.

An initial multi-group implementation reconciled dictionary entries for every
result. Measurements exposed its overhead with short eight-child groups.
The final implementation reads ordered records directly, checks identity during
the readiness traversal, and compacts records directly when groups are removed
without reordering survivors. Replacement or reordering uses the ID map.

`BenchmarkCapacityGroups` constructs 8/32/128 nested groups, each with eight
leaf branches, beneath one root group. Before timing, a serial seed executes
the root fork and every nested fork (`count+1` steps). It contains `count+1`
groups and `1+9*count` invocations, with no leaf yet executed. Each timed Resume
finishes `8*count` leaves and one final node; joins do not consume node steps.
The final total is `8*count`, with `2+9*count` cumulative steps. The seed is reused
without a Store, and its structure and mutable maps are checked after timing.
The nested fixture adds a group tag to the usual 16-entry map. Compilation,
seeding, and immutable-seed checks are outside timing; merge-order and final
state assertions are inside.

```sh
go test -run '^$' -bench '^BenchmarkCapacity(Fanout|Groups)$' -benchmem -benchtime=500ms -count=5 -cpu '1,8' ./internal/capacitytest
```

Correctness checks compare cached readiness to an independent full-child scan
under forward, reverse, and fixed-seed mixed completion, all terminal statuses,
nonterminal states, missing children, unordered groups, topology replacement,
buffer exchange, nested cascading completion, and cache contraction. Public API
tests cover nine nested activations across recovery budgets, both node
concurrency settings, and both Stores. Resume batches test join-input Clone
failure, CAS rejection after merge, and persisted terminal join failure:
the first input's commit survives, uncommitted state does not, and replay keeps
continuation/join CallIDs. Existing nested FailGroup, cancellation/drain,
SQLite crash/reopen, and uncertain acknowledgement tests remain part of the gate.

### Wide and nested workloads

Five 500ms samples per configuration report median [minimum–maximum] ns/op.
Positive reduction means lower measured time; negative means higher time.

| Width | Node concurrency | Ps | Before ns/op | After ns/op | Reduction |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 1 | 1 | 247,323 [237,518–265,280] | 257,380 [250,912–302,527] | -4.1% |
| 32 | 1 | 8 | 383,611 [354,045–390,092] | 453,778 [449,751–472,082] | -18.3% |
| 32 | 8 | 1 | 276,966 [259,239–286,850] | 303,953 [299,505–333,642] | -9.7% |
| 32 | 8 | 8 | 302,075 [296,992–318,963] | 362,916 [361,201–375,758] | -20.1% |
| 128 | 1 | 1 | 1,354,276 [1,322,772–1,508,973] | 1,413,637 [1,363,805–1,553,441] | -4.4% |
| 128 | 1 | 8 | 1,861,576 [1,846,588–1,995,164] | 2,113,739 [2,000,146–2,120,984] | -13.5% |
| 128 | 8 | 1 | 1,345,686 [1,305,170–1,390,825] | 1,457,508 [1,398,792–1,594,083] | -8.3% |
| 128 | 8 | 8 | 1,496,862 [1,448,255–1,558,060] | 1,646,281 [1,612,532–1,755,146] | -10.0% |
| 512 | 1 | 1 | 13,090,616 [11,830,115–15,036,697] | 9,462,477 [8,942,050–9,780,808] | 27.7% |
| 512 | 1 | 8 | 13,397,269 [12,535,931–13,878,940] | 11,999,204 [11,759,182–13,275,052] | 10.4% |
| 512 | 8 | 1 | 8,995,360 [8,509,072–9,914,103] | 9,722,107 [8,987,623–10,021,620] | -8.1% |
| 512 | 8 | 8 | 12,418,198 [12,366,189–14,028,321] | 11,017,339 [10,863,436–11,912,572] | 11.3% |

| Nested groups | Node concurrency | Ps | Before ns/op | After ns/op | Reduction |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 1 | 1 | 611,323 [600,216–650,944] | 637,715 [566,306–700,209] | -4.3% |
| 8 | 1 | 8 | 720,496 [714,662–758,938] | 753,846 [716,043–784,708] | -4.6% |
| 8 | 8 | 1 | 640,926 [612,055–654,446] | 649,136 [613,913–687,137] | -1.3% |
| 8 | 8 | 8 | 653,543 [605,664–693,870] | 589,930 [576,429–591,444] | 9.7% |
| 32 | 1 | 1 | 4,547,049 [4,401,748–5,709,375] | 4,098,084 [3,774,307–4,304,102] | 9.9% |
| 32 | 1 | 8 | 4,313,169 [3,977,419–5,195,339] | 3,942,938 [3,918,322–4,079,687] | 8.6% |
| 32 | 8 | 1 | 4,233,933 [4,051,776–4,451,808] | 4,339,841 [4,142,601–4,437,526] | -2.5% |
| 32 | 8 | 8 | 3,513,623 [3,443,444–3,677,893] | 3,349,436 [3,224,305–3,538,869] | 4.7% |
| 128 | 1 | 1 | 37,390,967 [35,290,139–42,224,979] | 32,364,000 [31,617,400–33,503,695] | 13.4% |
| 128 | 1 | 8 | 35,952,574 [33,353,856–41,260,863] | 31,970,730 [29,911,863–32,479,105] | 11.1% |
| 128 | 8 | 1 | 36,068,358 [34,988,729–36,382,593] | 33,611,130 [32,798,881–34,481,280] | 6.8% |
| 128 | 8 | 8 | 32,833,056 [31,846,375–34,469,995] | 31,809,533 [30,949,495–34,174,711] | 3.1% |


A final alternating fan-out comparison repeated all widths three times,
500ms per configuration on one/eight Ps. The broad narrower-graph regressions
against the earlier baseline did not persist: width 128 medians decreased in
all four configurations, and width 512 decreased by 5.3–31.3%. Width 32 with
node concurrency eight remains 0.8% slower on one P and 5.9% slower on eight Ps.
The retained ranges limit any general latency claim; this remains workload-
and completion-order-dependent.

| Width | Node concurrency | Ps | Before ns/op | After ns/op | Reduction |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 1 | 1 | 277,602 [233,728–278,442] | 256,298 [250,470–334,255] | 7.7% |
| 32 | 1 | 8 | 374,215 [350,310–392,446] | 345,942 [321,256–384,580] | 7.6% |
| 32 | 8 | 1 | 270,066 [249,760–280,801] | 272,276 [268,046–287,062] | -0.8% |
| 32 | 8 | 8 | 296,966 [282,311–313,416] | 314,468 [278,777–322,746] | -5.9% |
| 128 | 1 | 1 | 1,453,032 [1,406,619–1,526,327] | 1,337,575 [1,217,443–1,486,345] | 7.9% |
| 128 | 1 | 8 | 1,872,419 [1,754,135–1,902,455] | 1,618,875 [1,604,256–1,686,238] | 13.5% |
| 128 | 8 | 1 | 1,351,429 [1,259,718–1,414,903] | 1,261,396 [1,225,184–1,393,166] | 6.7% |
| 128 | 8 | 8 | 1,514,247 [1,510,568–2,082,961] | 1,301,107 [1,279,313–1,339,696] | 14.1% |
| 512 | 1 | 1 | 11,799,458 [11,184,907–13,348,918] | 8,589,010 [8,413,648–8,793,271] | 27.2% |
| 512 | 1 | 8 | 12,973,279 [12,633,467–14,079,391] | 9,624,220 [9,338,420–10,661,984] | 25.8% |
| 512 | 8 | 1 | 8,899,114 [8,565,467–10,445,884] | 8,428,296 [8,415,667–8,735,775] | 5.3% |
| 512 | 8 | 8 | 12,977,270 [12,146,693–14,182,771] | 8,911,408 [8,911,051–9,334,674] | 31.3% |

The multiple-group cache adds about 888/3,288/12,758 B/op for nested counts
8/32/128 in the one-P/serial medians, with five extra allocations per operation.
Single-group fan-out allocation counts remain unchanged; the cache is a space
tradeoff for reduced repeated child lookup, not an allocation optimization.

An alternating baseline/new check repeated the 32-nested-group cases three
times, 500ms per case. It no longer reproduced the earlier 9–10% concurrency-eight
regression from the intermediate implementation; the one-P case remains about
2.4% slower in these medians. It does not establish a universal multi-group speedup.

| Node concurrency | Ps | Before ns/op | After ns/op |
| ---: | ---: | ---: | ---: |
| 1 | 1 | 3,766,735 [3,737,285–3,841,305] | 3,713,549 [3,659,346–4,272,114] |
| 1 | 8 | 4,313,454 [4,197,865–4,393,798] | 4,095,355 [4,010,406–4,492,048] |
| 8 | 1 | 3,975,208 [3,772,971–4,298,145] | 4,068,694 [3,921,677–4,334,373] |
| 8 | 8 | 3,605,435 [3,534,932–3,686,681] | 3,553,187 [3,371,471–3,863,939] |

### Small and durable checks

Sequential cases used five 500ms samples per configuration on one/eight Ps.
Allocation counts remain 13/118/902 for 1/16/128 steps; B/op differences are
zero or one byte in the medians. Latency medians remain mixed, including the
one-P 16-step increase below, so narrower/empty-group timings are not claimed
as improvements.

| Steps | Ps | Before ns/op | After ns/op | Before → after B/op | allocs/op |
| ---: | ---: | ---: | ---: | --- | ---: |
| 1 | 1 | 2,551 | 2,659 | 1,256 → 1,256 | 13 |
| 1 | 8 | 4,775 | 4,688 | 1,928 → 1,928 | 13 |
| 16 | 1 | 35,137 | 38,579 | 5,856 → 5,856 | 118 |
| 16 | 8 | 66,765 | 61,064 | 6,532 → 6,533 | 118 |
| 128 | 1 | 261,729 | 259,581 | 40,289 → 40,289 | 902 |
| 128 | 8 | 493,017 | 481,385 | 40,985 → 40,984 | 902 |

Durable rounds use three 200ms samples, default node concurrency, and one/eight
Ps. The table shows eight-P medians. SQLite width-32 samples contain only
two or three measured iterations. One-P Memory width-eight medians increased
from 171,235 to 199,240 ns/op while eight-P medians decreased; eight-P SQLite
width-eight medians increased. There is no demonstrated persistence speedup.

| Store / width | Before ns/op | After ns/op | Before → after B/op | Before → after allocs/op |
| --- | ---: | ---: | --- | --- |
| Memory / 2 | 65,184 | 69,219 | 26,783 → 26,783 | 174 → 174 |
| Memory / 8 | 232,221 | 225,487 | 137,584 → 137,580 | 683 → 683 |
| Memory / 32 | 2,130,108 | 1,954,302 | 1,424,650 → 1,424,645 | 5520 → 5521 |
| Sqlite / 2 | 9,662,138 | 9,402,637 | 40,468 → 40,418 | 678 → 678 |
| Sqlite / 8 | 25,798,150 | 29,737,088 | 172,195 → 172,170 | 3505 → 3505 |
| Sqlite / 32 | 83,322,667 | 82,773,167 | 1,493,938 → 1,500,338 | 38613 → 38616 |

### Profiles and sustained recovery

Fresh profiles used width 512, node concurrency eight, `-benchtime=3s -cpu=8`.
Baseline `settleGroups` accounted for 22.7% cumulative CPU time; the final
version accounted for 6.8%. Invocation lookup fell from 17.2% to 2.9%. Remaining
stacks include `copyInvocations` at 24.8%, and Clone still accounts for 69.3%
of allocation bytes. Shares overlap and are not absolute elapsed-time savings;
setup and calibration are included. Checkpoint structural copying and application
Clone remain separate optimization candidates.

The final runtime passed the 60-second-per-Store soak on eight Ps with the same
64-run/eight-caller/width-eight workload. This workload exercises the single-group
path; nested-group correctness and costs are covered by the tests and benchmarks
above. Results freeze elapsed time after draining and use the last 1,024 successful
call latencies, excluding post-drain recovery work.

| Store | Successful calls | Calls/s | Rolling p50 / p95, ms | Cancelled calls | Sampled peak heap, B | Drain / recovery GC heap, B | Seed / peak / drain goroutines |
| --- | ---: | ---: | --- | ---: | ---: | --- | --- |
| Memory | 412,935 | 6,882.18 | 1.0855 / 1.8635 | 8 | 4,207,000 | 850,488 / 775,240 | 3 / 50 / 3 |
| SQLite | 2,486 | 41.43 | 184.9800 / 282.6951 | 8 | 3,128,880 | 786,272 / 735,160 | 4 / 14 / 4 |

Reload, partial-round recovery, and resource checks passed; goroutines returned
to seed levels. These sustained samples verify recovery and resource behavior,
not a throughput comparison between versions. Full tests, race tests, vet,
and the separate one-second-per-Store race soak passed. Benchmark binaries,
raw samples, and profiles remain in the temporary directory, outside the repository.
