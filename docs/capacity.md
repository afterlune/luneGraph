# Capacity and sustained execution baseline

Capacity is a measured property of a workload, Store, and machine. This suite
supports the goal of serving as a generic graph substrate for a large Agent OS;
it does not introduce agent concepts or claim a production capacity limit.

All fixtures live in `internal/capacitytest`, separated into state, graph,
Store, worker, benchmark, isolation, lifecycle, and measurement files. They use
the public API, a strongly typed state containing a mutable 16-entry map, and
a deep-copy Clone. The fixtures require no workload-specific runtime APIs.

## Repeatable workloads

The interruption reliability workloads additionally exercise mutable state
through repeated node, join, and continuation interruptions. Their fixtures
live alongside the existing capacity tests and use only the public API.

`TestCapacityInterruptionLifecycle` runs four rounds with no Store, Memory,
and SQLite, widths 8/128, and node concurrency 1/8.
`TestCapacitySharedInterruptionLifecycle` runs eight rounds across 16 runs,
four callers, and eight branches, sharing one compiled Runner and Store.
Normal tests also cover state isolation from mutable-map changes, fresh-Runner
SQLite reopen for all three callback kinds, deterministic overlapping recovery
outcomes, and waiting for a callback that delays its response to cancellation.
These tests run in the existing Windows/Linux CI without enabling a soak.

The interruption controller requests interruption on the first attempt of
each targeted callback, then permits replay of the same RunID/CallID with the
same source state. It retains only pending identities and removes committed
ones after each call. Persisted slots keep counters and input addresses, not
duplicate application states. At each returned checkpoint, active topology
is bounded by `width + 1` invocations and one group. Round boundaries collapse
to one waiting invocation with no group and no retained controller entries.
This is a test readiness protocol, not a production lease or an effect receipt.

Interruption benchmarks include the test controller, deep-copy state,
state/counter checks, and persisted reload checks. Setup and the first round
are outside timing. Each timed operation completes a new round from a waiting
checkpoint, including replay of a continuation input, fork/branch callbacks,
and a join. A round commits ten node steps and one continuation application;
the number of interrupted public calls can vary with scheduling.

| Benchmark | Sizes | One measured operation |
| --- | --- | --- |
| `BenchmarkCapacityCompile` | 128 / 1,024 / 8,192 sequential nodes | Compile an already constructed definition. A complete execution validates the fixture before timing. |
| `BenchmarkCapacityFanout` | 32 / 128 / 512 branches; node concurrency 1 / 8 | Start and complete one fan-out/join round without a Store. |
| `BenchmarkCapacityGroups` | 8 / 32 / 128 nested groups, eight branches each; node concurrency 1 / 8 | Resume an immutable seed with all activations established and finish both levels of joins without a Store. Includes one extra root group. |
| `BenchmarkCapacityShared` | 16 / 64 / 256 fixed runs; 1 / 8 / 32 caller workers | Advance every run once using one shared Runner, with no Store, Memory, or SQLite. |
| `BenchmarkCapacityPaused` | Memory: 128 / 1,024 / 8,192; SQLite: 128 / 1,024 paused runs | Recover one run without input (`inspect`) or apply a typed continuation and advance it to the next wait (`advance`). |
| `BenchmarkCapacityHistory` | 32 / 128 / 512 rounds; terminal / failure / mixed history; no Store / Memory / SQLite | Inspect one waiting execution after a fixed number of branch-outcome rounds, using Resume without a Store or Recover with a Store. |
| `BenchmarkCapacityInterruption` | Memory / SQLite; eight branches; node concurrency 1 / 8 | Complete one round from a waiting checkpoint, including continuation, node, and join interruptions and their recovery. |

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

## Format-3 scale and cost diagnosis

Collected on 2026-09-30 at commit `d6ed67e93757b0bb7b54f639fadf56e01590d86c`,
with Go 1.26.5, Windows/amd64, and an AMD Ryzen 7 6800H. Capacity and control
commands ran serially with 500ms intervals, three repetitions, and one/eight Ps:
186 capacity samples (31 workloads) and 150 control samples (25 workloads).
Every result check passed. No runtime, API, format, or Store settings changed.
SQLite used its existing WAL, `synchronous=FULL`, and one open connection.

```sh
go test -run '^$' -bench '^(BenchmarkCapacityFanout|BenchmarkCapacityGroups|BenchmarkCapacityShared|BenchmarkCapacityPaused)$' -benchmem -benchtime=500ms -count=3 -cpu '1,8' ./internal/capacitytest
go test -run '^$' -bench '^(BenchmarkSequentialExecution|BenchmarkFanoutJoin|BenchmarkDurableSequentialExecution|BenchmarkDurableFanoutJoin)$' -benchmem -benchtime=500ms -count=3 -cpu '1,8' .
```

Tables show median [observed range] in microseconds unless labelled otherwise;
allocation columns are eight-P medians. These are diagnostics on an otherwise
idle workload, not production limits or a comparison of format versions.
Independent profile runs are excluded from these tables. The already completed
[format-3 history measurements](#format-3-comparison) and
[four sustained workload/Store checks](#format-3-sustained-validation) remain
current evidence and were not repeated in this measurement-only change.

### Wide and nested executions

| Workload / size | Node concurrency | One P us/op [range] | Eight Ps us/op [range] | B/op, eight Ps | allocs/op, eight Ps |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fanout / 32 | 1 | 242.087 [237.409–260.952] | 366.284 [354.758–385.398] | 141,344 | 754 |
| Fanout / 32 | 8 | 263.504 [259.572–296.241] | 281.287 [275.674–292.233] | 142,238 | 754 |
| Fanout / 128 | 1 | 1,116.335 [1,111.729–1,182.030] | 1,480.083 [1,392.777–1,495.888] | 545,623 | 2,781 |
| Fanout / 128 | 8 | 1,266.850 [1,225.653–1,361.407] | 1,263.669 [1,202.689–1,527.681] | 546,477 | 2,781 |
| Fanout / 512 | 1 | 9,026.209 [8,673.413–9,053.144] | 10,967.448 [10,900.071–11,029.235] | 2,177,088 | 11,621 |
| Fanout / 512 | 8 | 8,136.961 [8,051.828–8,340.763] | 21,486.580 [9,162.984–21,964.219] | 2,177,908 | 11,621 |
| Groups / 8 | 1 | 593.029 [548.593–594.408] | 673.279 [650.896–714.800] | 233,540 | 1,325 |
| Groups / 8 | 8 | 608.297 [595.658–616.918] | 553.556 [500.717–579.961] | 234,443 | 1,325 |
| Groups / 32 | 1 | 3,605.018 [3,453.515–3,660.641] | 3,478.647 [3,405.077–3,513.371] | 934,939 | 5,516 |
| Groups / 32 | 8 | 3,870.813 [3,737.425–4,148.558] | 2,910.575 [2,886.936–2,917.595] | 935,778 | 5,516 |
| Groups / 128 | 1 | 28,107.529 [28,080.042–29,136.045] | 27,652.495 [27,187.355–29,645.747] | 3,713,417 | 22,615 |
| Groups / 128 | 8 | 34,270.525 [34,160.742–39,097.255] | 33,067.672 [32,090.881–33,297.117] | 3,714,271 | 22,615 |

Fan-out includes Start, the fork, all branches, join, and final node. Groups
measure Resume from a seed with every nested activation established: each
listed group has eight branches plus one shared root group. Thus group size
128 resumes 1,024 branches with 129 activation groups and 1,153 invocations at
its seed. State starts with a mutable 16-entry map; nested forks add a group key.

Eight-P fan-out allocation bytes grow about 15.4 times from width 32 to 512,
while serial-node median time grows about 29.9 times. Nested groups grow four
times from 32 to 128, but serial-node time grows about eight times. These
observations are consistent with repeatedly copying and compacting the active
execution position; they are not an asymptotic complexity proof. Node
parallelism does not guarantee lower cost for these small callbacks.
Width-512/concurrency-eight spans 9.163–21.964ms in its three eight-P samples;
that variation is retained rather than described as a regression or speedup.

### Shared executions

One operation advances a complete fixed batch, once per run. Caller workers
never advance the same run concurrently. The per-execution figure divides
batch time by run count; it is an average throughput cost, not call latency.

| Store | Runs / callers | One P batch ms [range] | Eight Ps batch ms [range] | Eight Ps average us/execution | Eight Ps B/batch | Eight Ps allocs/batch |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| None | 16 / 1 | 0.235 [0.190–0.278] | 0.266 [0.266–0.267] | 16.644 | 62,728 | 621 |
| None | 64 / 8 | 0.650 [0.619–1.020] | 0.202 [0.200–0.242] | 3.149 | 250,934 | 2,488 |
| None | 256 / 32 | 2.563 [2.545–2.569] | 1.068 [1.016–1.071] | 4.172 | 1,002,398 | 9,813 |
| Memory | 16 / 1 | 0.285 [0.258–0.297] | 0.306 [0.290–0.404] | 19.143 | 119,707 | 892 |
| Memory | 64 / 8 | 1.034 [1.012–1.059] | 0.495 [0.478–0.520] | 7.742 | 478,793 | 3,564 |
| Memory | 256 / 32 | 4.857 [4.151–5.515] | 1.921 [1.844–1.982] | 7.503 | 1,913,148 | 14,047 |
| SQLite | 16 / 1 | 73.837 [72.493–92.736] | 75.096 [71.642–77.150] | 4,693.491 | 210,283 | 3,892 |
| SQLite | 64 / 8 | 287.981 [285.503–299.505] | 291.227 [286.273–305.856] | 4,550.417 | 868,140 | 16,151 |
| SQLite | 256 / 32 | 1,139.325 [1,133.848–1,852.410] | 1,345.003 [1,344.401–1,419.940] | 5,253.920 | 3,510,608 | 64,657 |

None and Memory allocate roughly proportional to batch size. The None fixture
retains checkpoints in its slots; persisted slots retain only bookkeeping,
with the Store owning the checkpoint. Counts and worker counts change together,
so this matrix does not independently measure caller-count scaling.
SQLite's 64-run cases have only two timed batches per sample and its 256-run
cases only one. With one connection and a transaction per accepted input and
node outcome, independent callers do not remove the durable write cost.

### Paused population

| Store / runs | Mode | One P us/op [range] | Eight Ps us/op [range] | B/op, eight Ps | allocs/op, eight Ps | Eight Ps seed-heap bytes | Idle goroutines |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory / 128 | inspect | 4.953 [4.170–5.088] | 3.968 [3.854–3.985] | 2,861 | 17 | 212,472 | 3 |
| Memory / 128 | advance | 14.879 [14.564–16.367] | 16.793 [16.140–17.744] | 7,469 | 54 | 212,360 | 3 |
| Memory / 1,024 | inspect | 5.134 [4.843–5.253] | 4.761 [4.760–5.569] | 2,861 | 17 | 1,700,240 | 3 |
| Memory / 1,024 | advance | 24.271 [22.289–25.568] | 18.575 [18.189–22.521] | 7,451 | 53 | 1,700,304 | 3 |
| Memory / 8,192 | inspect | 6.341 [5.722–6.919] | 5.293 [5.257–5.957] | 2,860 | 17 | 13,610,352 | 3 |
| Memory / 8,192 | advance | 22.969 [21.812–25.332] | 22.888 [22.770–23.108] | 7,443 | 53 | 13,610,016 | 3 |
| SQLite / 128 | inspect | 65.529 [64.838–67.134] | 52.134 [51.224–68.470] | 6,515 | 104 | 35,336 | 4 |
| SQLite / 128 | advance | 4,783.744 [4,596.940–6,712.539] | 5,252.151 [4,454.895–7,793.274] | 13,159 | 243 | 34,632 | 4 |
| SQLite / 1,024 | inspect | 57.793 [56.897–63.924] | 54.271 [53.552–54.284] | 6,518 | 104 | 214,032 | 4 |
| SQLite / 1,024 | advance | 4,558.508 [4,541.833–4,598.366] | 4,594.048 [4,531.398–4,903.238] | 13,091 | 243 | 214,768 | 4 |

Population grows 64 times from 128 to 8,192 for Memory; eight-P inspection grows
from 3.968 to 5.293us/op, while GC seed heap grows from 212,472 to 13,610,352
bytes in the inspection fixtures. Recovery loads one run rather than scanning
all stored runs. Idle goroutines remain three for Memory and four for SQLite.
These observations distinguish per-run recovery cost from retained population
cost; they do not claim constant latency at arbitrary population sizes.

Seed heap is the fixture's positive process-wide GC difference, including
bookkeeping and caches, not a precise per-run state budget. A missing negative
metric must not be interpreted as zero retention. SQLite Go heap excludes its
database/WAL files, native allocations, and filesystem cache. Each inspect and
advance row is independently seeded, so heap differences are sampling results.
The Stores continue retaining completed checkpoints until discarded by the
application; these measurements introduce no retention or cleanup policy.

### Sequential and durable controls

Scalar controls use an integer state; durable controls use a 16-entry map.
Durable fan-out loops keep their active group at the call boundary and persist
every node outcome. Their per-operation costs include Recover and a complete
round, not just a node callback. `default` node concurrency follows the P count.

| Workload | One P us/op [range] | Eight Ps us/op [range] | B/op, eight Ps | allocs/op, eight Ps |
| --- | ---: | ---: | ---: | ---: |
| Scalar sequential / 1 steps | 2.869 [2.744–2.953] | 6.664 [6.532–6.766] | 1,928 | 13 |
| Scalar sequential / 16 steps | 34.665 [34.640–34.932] | 93.748 [85.585–94.520] | 6,532 | 118 |
| Scalar sequential / 128 steps | 280.897 [248.796–294.254] | 709.281 [709.150–723.338] | 40,987 | 902 |
| Scalar fan-out / 2 / serial | 12.878 [12.391–13.538] | 30.548 [28.976–30.600] | 4,042 | 51 |
| Scalar fan-out / 2 / default | 13.012 [12.733–14.641] | 36.185 [35.634–36.215] | 4,715 | 51 |
| Scalar fan-out / 8 / serial | 36.899 [35.437–38.383] | 76.114 [75.373–76.296] | 12,223 | 114 |
| Scalar fan-out / 8 / default | 41.750 [37.843–47.273] | 53.732 [50.055–64.930] | 12,896 | 114 |
| Scalar fan-out / 32 / serial | 148.621 [144.094–155.469] | 251.213 [250.534–252.061] | 37,430 | 339 |
| Scalar fan-out / 32 / default | 186.079 [151.423–204.082] | 194.010 [188.068–207.419] | 38,106 | 339 |
| Memory sequential / 1 steps | 8.278 [8.261–8.838] | 17.127 [16.556–17.276] | 5,466 | 36 |
| Memory sequential / 16 steps | 97.496 [93.070–103.618] | 182.361 [182.240–184.593] | 42,207 | 292 |
| Memory fan-out / 2 / serial | 52.020 [45.276–52.156] | 80.038 [79.216–81.795] | 26,109 | 174 |
| Memory fan-out / 2 / default | 49.539 [47.682–50.193] | 80.156 [80.047–85.660] | 26,784 | 175 |
| Memory fan-out / 8 / serial | 252.225 [218.326–252.783] | 284.153 [284.096–288.199] | 136,905 | 684 |
| Memory fan-out / 8 / default | 230.606 [226.748–246.033] | 243.969 [241.297–246.889] | 137,585 | 684 |
| Memory fan-out / 32 / serial | 1,984.525 [1,912.795–2,008.720] | 2,259.321 [2,256.079–2,395.053] | 1,423,984 | 5,523 |
| Memory fan-out / 32 / default | 1,827.316 [1,760.930–2,031.331] | 2,101.407 [2,091.450–2,203.068] | 1,424,690 | 5,523 |
| SQLite sequential / 1 steps | 2,385.584 [2,381.657–2,416.193] | 2,370.413 [2,335.155–2,418.723] | 10,429 | 174 |
| SQLite sequential / 16 steps | 36,671.133 [35,754.393–37,869.453] | 36,478.175 [36,392.200–37,519.633] | 64,067 | 1,228 |
| SQLite fan-out / 2 / serial | 9,286.114 [9,196.409–9,560.745] | 9,411.618 [9,376.692–9,694.763] | 39,379 | 690 |
| SQLite fan-out / 2 / default | 9,576.291 [9,291.856–10,245.685] | 9,760.698 [9,337.446–9,771.448] | 40,132 | 690 |
| SQLite fan-out / 8 / serial | 24,178.171 [23,455.062–42,309.956] | 24,885.571 [24,183.782–25,119.388] | 169,685 | 3,532 |
| SQLite fan-out / 8 / default | 23,907.977 [23,696.920–24,638.492] | 24,039.748 [23,487.552–37,160.448] | 170,350 | 3,534 |
| SQLite fan-out / 32 / serial | 86,428.150 [85,390.167–106,887.014] | 91,016.483 [85,964.167–524,141.400] | 1,491,662 | 38,705 |
| SQLite fan-out / 32 / default | 83,719.350 [83,608.486–85,937.543] | 84,409.100 [83,873.767–84,576.143] | 1,497,829 | 38,712 |

The SQLite width-32 serial eight-P case includes a 524.141ms sample with only
one timed iteration, versus 85.964–91.016ms for its other samples. The outlier
is retained. Differences from the earlier 100ms control or format-2 samples
cannot be attributed to a code change: this stage changes no runtime code.

### Profiles and next optimization candidate

Five separate CPU/memory profiles used `-benchtime=3s -count=1 -cpu=8`.
Each memory profile was read with both `alloc_space` and `inuse_space`.
CPU percentages below are cumulative sampled CPU; child and parent rows
within a stack overlap and must not be added. They do not measure elapsed
waiting time or predict a percentage improvement from removing a function.

| Profile | Selected cumulative CPU | Allocation evidence |
| --- | --- | --- |
| Fan-out 512, node concurrency 8 | Clone 17.94%; CopyInto 17.05%; invocation copying 15.28%; old-state clearing 8.88% | Application map Clone accounts for 66.96% of allocated bytes. |
| Nested groups 128, node concurrency 8 | settleGroups 32.88%; CopyInto 19.00%; removeInvocations 18.60%; readyGroup 8.76%; old-state clearing 5.93% | Map Clone accounts for 55.05%; CopyInto for 12.44%. |
| Memory durable width 32, default concurrency | Checkpoint.Clone 37.77%; Store CAS 36.77%; application Clone 36.77% | Application map Clone accounts for 81.97%; Store CAS for 86.69%, including Clone. |
| SQLite durable width 32, default concurrency | Store CAS 93.73%; Windows FlushFileBuffers path 79.85%; JSON Append 5.97% | JSON Append accounts for 76.40%; mapEncoder for 72.27%, including its descendants. |
| Memory shared 256 runs / 32 callers | Application Clone 20.27%; Store CAS 17.38%; CopyInto 5.06%; RWMutex.Lock 4.03% | Map Clone accounts for 65.73%; CopyInto for 16.16%. |

The shared lock/block profiles were collected separately from CPU/memory, with
`-mutexprofilefraction=10 -blockprofilerate=1000000`. Store CAS appears in 82.19%
of sampled aggregate mutex delay and 78.38% of sampled aggregate blocking delay;
channels and the worker harness also contribute. Total delay is summed across
waiting goroutines and can exceed wall time. This identifies contention on the
Store-wide RWMutex, not that 82% of normal runtime is blocked.

An initial attempt combined CPU/memory with sampling every mutex contention
and blocking event. It was intentionally stopped after 237.391s because its
instrumentation dominated the diagnostic run. Its output is retained as aborted,
not counted as a production test failure or a benchmark sample. Separate normal
CPU/memory and sampled lock/block runs passed in 3.917s and 4.429s respectively.
The profiling runs' timing does not replace the uninstrumented baseline.

End-of-run inuse profiles are dominated by runtime/platform initialization
(about 2–4MiB) and do not show a prominent retained checkpoint subtree. Stores
and fixtures are no longer kept alive at this profile boundary, and heap
profiles are sampled. This is not a live-Store footprint or proof of leak freedom;
the population GC measurements and existing soaks address different lifetimes.

### Invocation compaction follow-up

The bounded `removeInvocations` optimization reuses the existing position map:
it deletes entries for removed IDs and updates only survivors that move during
slice compaction. Invocation order, numeric ready selection, the small-index
threshold, removed-reference clearing, checkpoint ownership, and replay
semantics remain covered by the executor tests.

The baseline and candidate capacity-test binaries were built from commit
`f1be45a` and its working-tree change on 2026-09-30, using Go 1.26.5 on
Windows/amd64 with an AMD Ryzen 7 6800H. Each selected benchmark ran five
one-second samples at `-cpu=8`; the order was reversed for the second batch.

| Workload | Baseline median, batch 1 | Candidate median, batch 1 | Baseline median, batch 2 | Candidate median, batch 2 |
| --- | ---: | ---: | ---: | ---: |
| Nested groups 128, concurrency 8 | 27.942 ms/op | 26.815 ms/op (-4.0%) | 27.889 ms/op | 27.008 ms/op (-3.2%) |
| Fan-out 512, concurrency 8 | 8.339 ms/op | 7.438 ms/op (-10.8%) | 7.610 ms/op | 7.876 ms/op (+3.5%) |

The nested-group median was lower in both run orders, suggesting a 3–4% gain
for this workload on this host. Fan-out results varied in direction between
batches; the ranges overlapped in the second batch, so they do not establish a
separate gain or a material regression. Bytes and allocations per operation
were effectively unchanged.
A sampled candidate profile still attributes about 20% cumulative CPU to
`removeInvocations`; profile percentages are noisy and map updates for moved
survivors and numeric-order lookups remain. These measurements are evidence for
this bounded change, not a machine-independent threshold.

Structural copying is the next executor CPU cost to monitor; avoiding full
copies would require an ownership argument, not just a profile percentage.
Application map Clone is the leading allocation cost for Memory workloads and
cannot be skipped under the independent-state/Store ownership contracts.
SQLite's durable commit path remains the leading sampled cost; changing commit
points or synchronous mode is outside this diagnosis. Shared Memory contention
is a separate Store concern, not evidence for changing scheduler semantics.

### Reproduction artifacts

Raw outputs, a checked median/range `summary.json`, five CPU/memory profile
pairs, the separate shared mutex/block profiles, pprof top/cumulative/list
reports, test binaries, and command scripts are outside the repository at:

```text
C:/Users/Code/AppData/Local/Temp/lunegraph-format3-d6ed67e-20260930-153420/
```

`manifest.json` records the commit, machine and settings. `capacity.txt` and
`controls.txt` contain the 336 uninstrumented samples. `history-reused.txt` and
`soak-reused.txt` preserve the already completed format-3 checks. Temporary
artifacts are local diagnostics, not a committed or permanently hosted dataset.

Use the following exact selectors for separate profiles; the first, second,
and fifth belong to `./internal/capacitytest`, and the durable ones to `.`:

```text
^BenchmarkCapacityFanout/width=512/concurrency=8$
^BenchmarkCapacityGroups/groups=128/concurrency=8$
^BenchmarkDurableFanoutJoin/storage=memory/width=32/concurrency=default$
^BenchmarkDurableFanoutJoin/storage=sqlite/width=32/concurrency=default$
^BenchmarkCapacityShared/store=memory/runs=256/workers=32$
```

For each, add `-run '^$' -benchmem -benchtime=3s -count=1 -cpu=8`,
`-cpuprofile <temp>/name-cpu.pprof -memprofile <temp>/name-memory.pprof`,
and `-o <temp>/name.test.exe` to `go test -bench '<selector>' <package>`.
For the separate shared diagnostic omit CPU/memory flags and add
`-mutexprofile <temp>/shared-mutex.pprof -mutexprofilefraction=10`
and `-blockprofile <temp>/shared-block.pprof -blockprofilerate=1000000`.
Read CPU with `go tool pprof -top -cum`, memory with `-sample_index=alloc_space`
and `-sample_index=inuse_space`, and delay profiles with `-top`. Profile captures
include setup/calibration and test harness activity; none are added to benchmark
medians. Paths must be adapted on another machine.

Verification passed: `go test -count=1 ./...` on a serial retry,
`go test -race -count=1 ./...`, `go vet ./...`, `gofmt -l`,
`git diff --check`, and all 15 relative Markdown file/anchor links. The initial
ordinary full suite hit the previously observed Windows subprocess termination
error in `TestRecoveryAfterEffectCommitAndProcessCrash`:
`TerminateProcess: Access is denied`. The isolated test passed on retry, as did
the subsequent full suite. Its root cause was not established or repaired by
this documentation-only change; the failed attempt is not hidden.

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
  boundaries, active groups, and bounded failure metadata.
- Bounded latency sample accounting and invalid soak duration settings.
- Nine nested activations recovered across seven-step budgets with Memory and
  SQLite, node concurrency one/eight, ordered merges, and final state validation.
- Continuation input batches with join-input Clone failure, rejected join CAS,
  or terminal join failure: prior inputs remain committed; continuation and join
  replay retain their CallIDs. These run against both Memory and SQLite.
- History-producing loops with three branches: two end or fail locally while
  one reaches the join and continues. Terminal-only, failure-only, and mixed
  cases run for 128 rounds without a Store and with Memory, and 32 with SQLite,
  at node concurrency one/eight. Every round consumes a five-node step budget;
  every fourth round waits for typed continuation input. Checks cover exact
  local-failure flags, independent mutable state, fresh CallIDs, revisions,
  cumulative steps, compacted topology, and unchanged recovery without input.
- 64 newly created and completed runs sharing a Runner and Store with eight
  callers, for Memory and SQLite. All checkpoints remain loadable, returned
  state mutation cannot change stored state, and repeated completed recovery
  produces neither callbacks nor writes. Before/after GC heap and goroutine
  counts are logged with the Store alive, without fixed resource thresholds.

These supplement the crash, replay, uncertain acknowledgement, and cross-process
CAS tests described in [performance.md](performance.md). They run in existing
CI jobs without a scheduled workflow or a fixed speed/heap threshold.

## Manual sustained execution

`TestCapacitySoak` is skipped unless `LUNEGRAPH_SOAK_DURATION` is set. A malformed,
zero, or negative Go duration fails. The duration applies **to each workload/Store pair**.

```sh
LUNEGRAPH_SOAK_DURATION=60s go test -run '^TestCapacitySoak$' -count=1 -v -timeout 6m ./internal/capacitytest
```

PowerShell:

```powershell
$env:LUNEGRAPH_SOAK_DURATION = '60s'
go test -run '^TestCapacitySoak$' -count=1 -v -timeout 6m ./internal/capacitytest
Remove-Item Env:LUNEGRAPH_SOAK_DURATION
```

Choose `-timeout` to allow all four workload/Store intervals, fixture setup, draining, and
recovery. Both workloads have 64 fixed runs, eight caller workers, node concurrency
eight, and a wait every four rounds. Fan-out uses eight branches and ten node
outcomes per round. The mixed-outcome loop uses three branches: one joins, one
ends, and one fails locally, for five node outcomes per round. Continuation
acceptance commits separately. Both workloads maintain bounded topology. The fixtures model
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
state, wait boundary, group shape and local-failure flags, and stored counters are verified.
These post-drain recovery calls are excluded from measured throughput and
latency. Resource snapshots after recovery retain the live Store explicitly.

Intentional retained checkpoints, harness bookkeeping, caches, and runtime
initialization must be distinguished from transient leaks. A one-minute test
does not prove leak freedom or production durability over days.

## Retained history and completed executions

`BenchmarkCapacityHistory` complements the fixed-shape soak with one looping
execution that repeatedly ends or fails branches. Each round starts three independent branches:
one reaches the join, while two call `EndBranch`, fail under `FailInvocation`,
or do one of each. The join continues the healthy state; a boundary node waits
every fourth round. There are five node outcomes per round, one join callback,
and one continuation application after each previous wait. Active topology
returns to one invocation and no groups; only the local-failure flag remains persisted.

The benchmark seeds 32, 128, or 512 rounds at node concurrency eight, then
repeatedly inspects the waiting run without input. No-Store uses `Resume`;
Memory and SQLite use `Recover`. Setup, state validation, JSON-size measurement,
and final full-checkpoint comparison are outside timing. Timed assertions
check status, revision, and local-failure flags. This measures inspection recovery,
including Load/decode or copying and validation, rather than continuation
application, checkpoint commits, or cold-disk startup.

```sh
go test -run '^$' -bench '^BenchmarkCapacityHistory$' -benchmem -benchtime=100ms -count=3 -cpu=8 ./internal/capacitytest
go test -run '^(TestHistoryAcrossBudgetWaitAndRecovery|TestCompletedPopulationRetentionAndRecovery)$' -count=1 -v ./internal/capacitytest
```

The additional metrics are JSON `checkpoint-B`,
`idle-goroutines`, and `seed-heap-B`. Unlike the older paused benchmark,
`seed-heap-B` reports the **signed** process-wide GC heap difference. Negative
values expose sampling/cache variation rather than being clipped. Persisted
fixtures discard the returned checkpoint before the after-seed GC snapshot;
the Store remains live. The reference checkpoint loaded afterward for result
comparison is excluded from this snapshot. No-Store retains its one checkpoint.
SQLite caches and buffer pools can contribute to Go heap; database files,
native allocations, filesystem caches, and resident memory are excluded.

The semantic tests verify bounded topology and the local-failure flag
after every round. They verify independent maps, fresh callback IDs, exact
revisions and steps, wait boundaries, and saved checkpoint equality. A no-input recovery
preserves the waiting checkpoint and invokes no callback.

Both built-in Stores retain completed runs as well as active ones.
`TestCompletedPopulationRetentionAndRecovery` creates 64 distinct two-node
executions through eight callers, mutates each returned final state to verify
Store ownership, then reloads every run and recovers it twice. Observation
checks that terminal recovery performs no callbacks or writes; all revisions
remain three. This is a bounded creation/retention check, not a cleanup policy
or a continuous-arrival capacity claim.

In format 2, terminal history retained application state, including each mutable
map; failure history retained IDs, messages, and any panic stacks. Keeping the whole
history made checkpoint size grow with historical events even when active
topology was bounded. Structural copying and Store cloning/encoding also processed
that history as execution advanced. Completed runs add one retained checkpoint
per distinct run ID. These were costs of the format-2 retention contract; format 3
removes those histories.
Executor indices and group progress are invocation-local and do not form a
persistent history cache. Resource comparisons must account for these live
records, Store/runtime caches, and harness data before attributing growth to
transient leaks. The measurements do not establish leak freedom or a production
capacity limit.

### Format-3 comparison

Measured on the same machine and Go version with the history benchmark command
above: 81 samples, three repetitions, eight Ps. Median JSON sizes are identical
across Stores. The remaining growth is from counter and ID digit lengths, not
retained branch outcomes.

| Outcomes | Checkpoint bytes, 32 rounds | 128 rounds | 512 rounds |
| --- | ---: | ---: | ---: |
| Terminal | 591 | 597 | 600 |
| Failure | 590 | 596 | 599 |
| Mixed | 590 | 596 | 599 |

| Store / outcomes | Inspect us/op, 32 rounds | 128 rounds | 512 rounds | B/op at 512 | allocs/op at 512 |
| --- | ---: | ---: | ---: | ---: | ---: |
| None / terminal | 1.886 | 2.580 | 2.607 | 1,680 | 12 |
| None / failure | 2.802 | 3.056 | 2.949 | 1,680 | 12 |
| None / mixed | 3.122 | 2.902 | 2.689 | 1,680 | 12 |
| Memory / terminal | 4.993 | 5.675 | 5.539 | 2,873 | 18 |
| Memory / failure | 4.217 | 5.454 | 6.451 | 2,873 | 18 |
| Memory / mixed | 6.103 | 4.680 | 3.925 | 2,873 | 18 |
| SQLite / terminal | 79.935 | 63.702 | 59.788 | 6,675 | 106 |
| SQLite / failure | 64.605 | 76.963 | 64.710 | 6,675 | 106 |
| SQLite / mixed | 54.429 | 54.776 | 60.187 | 6,675 | 106 |

At 512 rounds, Memory terminal inspection falls from 1,726.674 to 5.539 us/op,
and SQLite from 10,784.167 to 59.788 us/op. The mixed checkpoint falls from
171,634 to 599 bytes. These are short diagnostic samples; they show the removal
of history copying/decoding costs, not production latency guarantees. SQLite
terminal/512 spans 55.434–65.629 us/op; mixed/512 spans 52.272–69.683 us/op.

Median signed seed-heap deltas across 32/128/512 rounds are 2,312–4,552 bytes
without a Store, 1,624–2,600 bytes for Memory, and 9,504–19,368 bytes for SQLite.
The fixture, pools, and GC sampling contribute to these process-wide values;
SQLite file growth is excluded. Idle goroutines remain three without a Store
or with Memory and four with SQLite. Completed run retention still adds one
checkpoint per run ID; removing histories does not implement Store cleanup.

### Control workloads

The following command ran before and after the runtime change, with three
repetitions at one and eight Ps (150 samples per version). Both runs passed.
These workloads do not accumulate branch outcomes. Selected eight-P results
are medians [observed range]; timing remains sensitive to scheduling and disk
variation in the short intervals.

```sh
go test -run '^$' -bench '^(BenchmarkSequentialExecution|BenchmarkFanoutJoin|BenchmarkDurableSequentialExecution|BenchmarkDurableFanoutJoin)$' -benchmem -benchtime=100ms -count=3 -cpu '1,8' .
```

| Workload | Format 2 us/op [range] | Format 3 us/op [range] | Format 2 / 3 B/op | Format 2 / 3 allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Sequential, 128 steps | 722.341 [715.651–766.280] | 548.286 [462.827–565.230] | 40,987 / 40,986 | 902 / 902 |
| Fan-out, width 32, default | 181.382 [174.841–183.563] | 129.155 [120.996–142.704] | 38,107 / 38,105 | 339 / 339 |
| Memory sequential, 16 steps | 137.404 [128.349–170.325] | 145.053 [123.011–155.538] | 42,210 / 42,207 | 291 / 291 |
| SQLite sequential, 16 steps | 37,055.633 [36,494.167–38,612.067] | 43,819.367 [40,378.000–44,497.933] | 65,898 / 64,773 | 1,215 / 1,212 |
| Memory fan-out, width 8, serial | 236.327 [227.812–247.342] | 314.802 [288.410–325.954] | 136,897 / 136,894 | 683 / 683 |
| Memory fan-out, width 8, default | 321.265 [204.082–327.796] | 273.091 [254.487–280.458] | 137,589 / 137,583 | 683 / 683 |
| SQLite fan-out, width 32, default | 92,849.650 [90,918.700–101,146.300] | 96,562.900 [84,255.650–98,474.650] | 1,500,940 / 1,490,180 | 38,613 / 38,604 |

Eight-P median changes across all control cases range from -33.7% to +33.2%.
Memory allocations remain effectively unchanged, while SQLite allocation counts
vary by small amounts from buffer/iteration effects. These controls do not
establish a uniform speedup or rule out small regressions; the decisive result
is bounded checkpoint size and history-independent copying in the outcome loop.
Raw samples were kept in temporary files outside the repository.

### Format-3 sustained validation

On 2026-09-30, both workloads ran for 60 seconds per Store, with 64 fixed runs,
eight callers, node concurrency eight, Go 1.26.5, and GOMAXPROCS 16. All four
passed cancellation drain, partial-round recovery, stored revision/counter
checks, and bounded topology/local-failure checks. Successful calls exclude
partial work committed by cancelled calls. Latencies are the last 1,024 calls;
heap is process-wide with the Store kept alive.

| Workload / Store | Successful calls | Calls/s | Rolling p50 / p95 ms | GC heap after seed / recovery bytes | Sampled peak heap bytes | Goroutines after recovery / sampled peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fanout/memory | 483,488 | 8,058.12 | 1.0905 / 1.9451 | 680,024 / 1,031,544 | 5,052,488 | 3 / 43 |
| fanout/sqlite | 2,426 | 40.43 | 194.3225 / 293.9796 | 996,288 / 994,216 | 3,177,032 | 4 / 16 |
| history/memory | 1,440,190 | 24,003.11 | 0.0000 / 0.6851 | 1,057,904 / 1,050,168 | 4,295,928 | 3 / 26 |
| history/sqlite | 4,768 | 79.46 | 95.4144 / 163.4444 | 987,096 / 978,688 | 3,372,584 | 4 / 15 |

The mixed Memory loop completed over a million rounds across its fixed runs
without retaining branch history. Sampled heap fluctuated with allocation and
GC; recovered heap and goroutine counts remained bounded in this one-minute
sample. A reported zero-millisecond rolling p50 is an observed timer result,
not a latency guarantee. SQLite still pays a transaction per committed outcome.
These measurements do not establish leak freedom over days, a production
capacity limit, or a cleanup policy for distinct completed run IDs.

After the migration, `go test ./...`, `go test -race ./...`, `go vet ./...`,
`gofmt`, and `git diff --check` passed. This includes crash/replay, CAS,
uncertain acknowledgement, cancellation, local failures, and completed-run
recovery checks.

### Format-2 retention baseline (historical)

Collected before the format-3 migration on 2026-09-30 with Go 1.26.5,
Windows/amd64, and an AMD Ryzen 7 6800H, using the commands above. The benchmark completed 81 samples: 27 cases,
three repetitions, eight Ps. Tables report medians. These short diagnostics
establish exercised history sizes and costs, not performance targets.

Each history mode retains two records per round. JSON sizes are identical
across the three Store choices for the same history and round count.

| History | Checkpoint bytes, 32 rounds | 128 rounds | 512 rounds |
| --- | ---: | ---: | ---: |
| Terminal | 15,752 | 61,804 | 247,887 |
| Failure | 6,364 | 23,960 | 95,387 |
| Mixed | 11,055 | 42,879 | 171,634 |

| Store / history | Inspect us/op, 32 rounds | 128 rounds | 512 rounds |
| --- | ---: | ---: | ---: |
| None / terminal | 4.278 | 15.506 | 62.143 |
| None / failure | 5.212 | 12.428 | 46.775 |
| None / mixed | 5.241 | 12.422 | 45.868 |
| Memory / terminal | 101.253 | 433.879 | 1,726.674 |
| Memory / failure | 8.724 | 21.997 | 79.559 |
| Memory / mixed | 51.991 | 202.531 | 1,001.094 |
| SQLite / terminal | 934.664 | 3,078.867 | 10,784.167 |
| SQLite / failure | 195.898 | 606.977 | 2,157.676 |
| SQLite / mixed | 575.303 | 2,133.062 | 7,513.407 |

The SQLite terminal/512 case has only 12 timed iterations per sample and an
observed range of 9,873.858–15,606.767 us/op. No-Store terminal/32 ranges from
2.979 to 7.916 us/op. Timing variation is reported rather than treated as a
change in execution semantics. These inspection calls perform no commits.

| Store / history | GC seed-heap delta bytes, 32 rounds | 128 rounds | 512 rounds |
| --- | ---: | ---: | ---: |
| None / terminal | 72,280 | 274,504 | 1,083,800 |
| None / failure | 9,224 | 29,688 | 109,160 |
| None / mixed | 39,720 | 151,720 | 604,760 |
| Memory / terminal | 69,880 | 275,112 | 1,081,160 |
| Memory / failure | 8,984 | 29,416 | 109,288 |
| Memory / mixed | 39,544 | 151,864 | 606,336 |
| SQLite / terminal | 73,600 | 126,112 | -7,752 |
| SQLite / failure | 54,200 | 68,128 | 232,416 |
| SQLite / mixed | 59,032 | 183,504 | 5,896 |

The SQLite terminal/512 heap differences range from -13,336 to 3,784 bytes;
failure/512 ranges from 225,264 to 366,064 bytes. SQLite's pool and runtime
variation makes these values unsuitable for inferring bytes per persisted
record. No-Store and Memory retain the history maps in Go heap; SQLite retains
checkpoint bytes on disk. Idle goroutine counts are three for None/Memory and
four for SQLite in every sample, independent of history size.

At 512 rounds, Memory terminal inspection allocates a median 1,125,655 B/op
and 4,117 allocs/op, versus failure inspection's 150,415 B/op and 20 allocs/op.
SQLite terminal inspection allocates 2,734,357 B/op and 43,129 allocs/op,
versus failure inspection's 478,401 B/op and 3,190 allocs/op. Terminal state
deep copies and JSON decoding include the fixture's 16-entry maps.

The separate 64-completed-run test logged GC heap increases of 75,584 bytes
for Memory and 4,424 bytes for SQLite. Goroutines returned to the pre-worker
counts of three and four respectively. All 64 rows/checkpoints remained
loadable, and both repeated recoveries preserved each revision without
callbacks or writes. These are single-run, process-wide heap samples; they
exclude SQLite file growth and do not establish a per-run storage budget.
Raw benchmark samples were kept in a temporary file outside the repository.
At the format-2 fixture stage, full tests, race tests, vet, and formatting
checks passed. The opt-in soak was not rerun at that stage. The format-3
workload/Store soak results are recorded above.

## Measured sample (format-2 baseline)

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

## Scheduling optimization sample (format 2)

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

## Group readiness optimization sample (format 2)

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
| SQLite / 2 | 9,662,138 | 9,402,637 | 40,468 → 40,418 | 678 → 678 |
| SQLite / 8 | 25,798,150 | 29,737,088 | 172,195 → 172,170 | 3505 → 3505 |
| SQLite / 32 | 83,322,667 | 82,773,167 | 1,493,938 → 1,500,338 | 38613 → 38616 |

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

## Checkpoint structural copy optimization sample (format 2)

The baseline is commit `53849a5`, compared on 2026-09-30 with Go 1.26.5,
Windows/amd64, AMD Ryzen 7 6800H. Baseline test binaries were built from an
archive of that commit with the identical new microbenchmark copied in.
The microbenchmark lives in `internal/model`; existing public API capacity
fixtures and Store benchmarks measure the broader execution path.

Structural-copy functions now live separately from checkpoint types and Clone.
Lists of at most eight invocations retain the direct loop. Larger lists copy
consecutive entries whose `Next` is nil in batches through built-in `copy`.
Non-nil route lists retain independent copy and buffer reuse, including
normalizing a non-nil empty list to nil. Obsolete destination route buffers
are cleared before replacement. Group children, terminals, failures, and Final
retain their existing copy behavior. Application state stays shallow here;
Store Clone and callback Clone remain the application-state ownership boundary.
There is no API, checkpoint format, scheduler, or commit-point change.

`BenchmarkCheckpointCopyInto` warms a separate destination before timing,
then repeatedly copies the same source. Reference state contains a mutable map;
invocation counts are 1/128/512 and routing is none, all waiting, or alternating
waiting/ready. Additional width-512 controls use an integer and a 512-byte array.
Graph execution, validation, Clone, persistence, and clearing spare state are
excluded. Final structure/source assertions are outside timing. These scores
measure the primitive, not durable call latency.

```sh
go test -run '^$' -bench '^BenchmarkCheckpointCopyInto$' -benchmem -benchtime=500ms -count=5 -cpu '1,8' ./internal/model
go test -run '^$' -bench '^BenchmarkCapacity(Fanout|Groups)$' -benchmem -benchtime=500ms -count=5 -cpu '1,8' ./internal/capacitytest
```

An independent allocation-based oracle checks every checkpoint field for
scalar, reference-containing, and 512-byte value states through repeated
0/1/8/9/128/512-entry growth, shrinkage, and buffer reuse. Checks cover route
independence, nil/empty normalization, obsolete route clearing, invocation/group
tail clearing, independent Final pointers, and the intentionally shallow S.
Existing runtime tests cover partial input commits, Clone errors, nested group
failure, join compaction/CAS replay, cancellation, SQLite crash/reopen, and
uncertain commit acknowledgements.

### Copy primitive

Five 500ms samples per configuration report median [minimum–maximum] ns/op.
All warmed primitive cases report **0 B/op and 0 allocs/op** before and after.
Positive reduction means less measured time; negative means more. The one-entry
mixed case has the same route geometry as the waiting case.

| State | Invocations | Routes | Ps | Before ns/op | After ns/op | Reduction |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| reference | 1 | none | 1 | 36.03 [34.94–41.42] | 37.28 [35.75–38.92] | -3.5% |
| reference | 1 | none | 8 | 37.55 [36.74–38.13] | 36.93 [34.72–37.36] | 1.7% |
| reference | 1 | waiting | 1 | 40.53 [39.72–41.34] | 40.89 [39.13–43.15] | -0.9% |
| reference | 1 | waiting | 8 | 42.39 [40.13–44.91] | 44.60 [41.32–47.75] | -5.2% |
| reference | 1 | mixed | 1 | 39.89 [38.89–40.71] | 42.47 [41.88–47.28] | -6.5% |
| reference | 1 | mixed | 8 | 41.27 [39.63–43.68] | 43.89 [42.32–48.03] | -6.3% |
| reference | 128 | none | 1 | 1,297 [1,293–1,347] | 578 [576–598] | 55.4% |
| reference | 128 | none | 8 | 1,350 [1,276–1,422] | 576 [531–596] | 57.3% |
| reference | 128 | waiting | 1 | 2,333 [2,303–2,639] | 2,163 [2,106–2,400] | 7.3% |
| reference | 128 | waiting | 8 | 2,206 [2,131–2,334] | 2,143 [2,132–2,232] | 2.9% |
| reference | 128 | mixed | 1 | 1,797 [1,774–1,880] | 1,867 [1,818–1,938] | -3.9% |
| reference | 128 | mixed | 8 | 1,840 [1,804–1,856] | 1,975 [1,830–2,009] | -7.3% |
| reference | 512 | none | 1 | 6,310 [5,921–7,312] | 2,705 [2,587–2,806] | 57.1% |
| reference | 512 | none | 8 | 6,001 [5,848–6,538] | 2,703 [2,662–2,820] | 55.0% |
| reference | 512 | waiting | 1 | 8,566 [8,406–8,788] | 8,537 [8,093–9,656] | 0.3% |
| reference | 512 | waiting | 8 | 8,439 [7,964–8,872] | 8,948 [8,417–9,331] | -6.0% |
| reference | 512 | mixed | 1 | 7,275 [6,918–7,454] | 7,184 [6,413–7,557] | 1.3% |
| reference | 512 | mixed | 8 | 6,823 [6,719–7,032] | 7,331 [7,067–7,569] | -7.4% |
| scalar | 512 | none | 1 | 6,455 [6,189–6,638] | 2,707 [2,677–2,801] | 58.1% |
| scalar | 512 | none | 8 | 6,190 [6,012–6,431] | 2,599 [2,534–2,707] | 58.0% |
| value512 | 512 | none | 1 | 16,130 [15,651–17,449] | 9,376 [9,157–9,727] | 41.9% |
| value512 | 512 | none | 8 | 15,950 [15,733–16,891] | 9,121 [8,917–9,446] | 42.8% |

Reference-state nil-route lists of 128/512 entries have 55–57% lower medians;
scalar width-512 controls have about 58% lower medians, and 512-byte value
controls about 42%. Waiting and mixed lists do not show uniform improvements,
including slower eight-P waiting/mixed cases. Short-list timing differences
must be checked with alternating runs rather than treated as a speedup.

The first implementation issued an empty-range `copy` for each waiting entry.
The final version skips those calls and separates the large-list loop from the
short-list loop. It keeps the original small-list behavior and does not add
per-entry auxiliary metadata.

A three-round alternating baseline/new check used 500ms per configuration.
The nil-route width-512 advantage repeats at 51–54%. Fully waiting width-512
copies remain about 8% slower: skipping empty copies removes avoidable work,
but the extra route classification still has a cost. One-entry medians differ
by about 0–2 ns, including a 5.5% increase for eight-P waiting. This is a measured
tradeoff; the runtime sequential controls below show no sustained regression.

| Invocations / routes | Ps | Before ns/op | After ns/op |
| --- | ---: | ---: | ---: |
| 1 / none | 1 | 35.67 [35.15–40.68] | 36.87 [36.21–37.94] |
| 1 / none | 8 | 36.22 [36.04–36.70] | 36.41 [35.12–36.78] |
| 1 / waiting | 1 | 40.93 [38.63–41.71] | 40.92 [40.05–43.72] |
| 1 / waiting | 8 | 39.99 [38.50–40.97] | 42.18 [40.97–43.10] |
| 512 / none | 1 | 5,342 [5,255–5,828] | 2,460 [2,376–2,673] |
| 512 / none | 8 | 5,246 [5,101–5,414] | 2,559 [2,434–2,582] |
| 512 / waiting | 1 | 7,495 [7,395–7,845] | 8,057 [7,988–8,110] |
| 512 / waiting | 8 | 7,428 [6,975–7,635] | 8,055 [7,664–8,355] |

### Execution and persistence

Wide and nested workloads use five 500ms samples per configuration on one/eight
Ps. Eight-P medians [ranges] are shown below; one-P nested-group reductions
are 3–30%, and one-P serial width-512 fan-out is 16.7% lower. Several fan-out
eight-P medians against the earlier baseline are slower, so a separate alternating
comparison follows rather than attributing those differences to the copy primitive.

| Workload / size | Node concurrency | Before ns/op | After ns/op | Reduction |
| --- | ---: | ---: | ---: | ---: |
| Fanout / 32 | 1 | 356,375 [333,604–367,126] | 427,116 [416,628–486,703] | -19.9% |
| Fanout / 32 | 8 | 316,447 [292,984–321,815] | 366,704 [352,185–372,012] | -15.9% |
| Fanout / 128 | 1 | 1,600,948 [1,518,150–1,679,568] | 1,961,238 [1,876,482–1,997,534] | -22.5% |
| Fanout / 128 | 8 | 1,287,683 [1,256,605–1,380,558] | 1,497,044 [1,443,766–1,582,139] | -16.3% |
| Fanout / 512 | 1 | 10,037,522 [9,148,055–10,316,439] | 11,032,342 [10,681,064–11,146,280] | -9.9% |
| Fanout / 512 | 8 | 8,624,139 [8,257,112–9,320,978] | 8,259,941 [7,156,922–9,586,203] | 4.2% |
| Groups / 8 | 1 | 720,606 [707,589–747,486] | 691,131 [677,166–722,357] | 4.1% |
| Groups / 8 | 8 | 591,330 [564,641–636,614] | 558,572 [520,250–591,651] | 5.5% |
| Groups / 32 | 1 | 3,928,291 [3,749,280–4,226,505] | 3,597,994 [3,423,518–3,739,474] | 8.4% |
| Groups / 32 | 8 | 3,667,851 [3,541,980–3,927,479] | 3,122,377 [2,952,490–3,184,636] | 14.9% |
| Groups / 128 | 1 | 38,786,964 [37,713,127–41,800,700] | 28,921,467 [26,953,827–31,699,543] | 25.4% |
| Groups / 128 | 8 | 37,050,869 [36,869,273–40,310,867] | 25,788,400 [25,563,041–28,207,629] | 30.4% |

An alternating fan-out comparison ran all widths three times, 500ms per case,
on one/eight Ps. It did not reproduce the earlier fan-out regressions:
width 32 medians decreased 2–10%, width 128 9–16%, and width 512 16–19%.
The disagreement between phases still limits any universal timing conclusion.

| Width | Node concurrency | Ps | Before ns/op | After ns/op |
| ---: | ---: | ---: | ---: | ---: |
| 32 | 1 | 1 | 260,576 [225,772–279,933] | 240,883 [226,510–256,777] |
| 32 | 1 | 8 | 359,259 [347,318–369,114] | 332,218 [317,586–333,390] |
| 32 | 8 | 1 | 263,232 [262,269–294,439] | 257,408 [251,844–266,657] |
| 32 | 8 | 8 | 310,410 [302,885–326,995] | 280,246 [275,191–300,564] |
| 128 | 1 | 1 | 1,244,821 [1,228,274–1,262,382] | 1,128,548 [1,085,139–1,197,963] |
| 128 | 1 | 8 | 1,713,173 [1,584,873–1,719,491] | 1,464,014 [1,427,994–1,537,288] |
| 128 | 8 | 1 | 1,418,835 [1,342,272–1,441,819] | 1,188,277 [1,187,057–1,300,280] |
| 128 | 8 | 8 | 1,346,471 [1,335,262–1,362,062] | 1,151,316 [1,151,012–1,195,509] |
| 512 | 1 | 1 | 8,835,982 [8,809,232–8,878,559] | 7,151,630 [6,913,256–7,375,549] |
| 512 | 1 | 8 | 10,504,981 [10,025,878–10,949,264] | 8,783,381 [8,672,616–8,815,530] |
| 512 | 8 | 1 | 9,095,655 [8,597,313–9,191,263] | 7,474,294 [7,168,591–7,992,688] |
| 512 | 8 | 8 | 9,158,978 [8,585,852–9,272,153] | 7,545,825 [6,986,495–7,598,415] |

No auxiliary allocations are introduced. One-P fan-out allocation counts
remain 754/2,780/11,618 for widths 32/128/512 in these samples; one-P nested
group counts remain 1,325/5,514/22,613. Small byte/count variation on eight Ps
reflects worker and map allocation behavior; it is recorded in the raw samples.

Sequential cases use five 500ms samples. Counts remain 13/118/902 for 1/16/128
steps. These graphs stay on the short-list path; their changing times do not
demonstrate benefits from bulk copying.

| Steps | Ps | Before ns/op | After ns/op | Before → after B/op | allocs/op |
| ---: | ---: | ---: | ---: | --- | ---: |
| 1 | 1 | 2,712 | 2,555 | 1,256 → 1,256 | 13 |
| 1 | 8 | 5,309 | 4,634 | 1,928 → 1,928 | 13 |
| 16 | 1 | 39,424 | 34,282 | 5,856 → 5,856 | 118 |
| 16 | 8 | 95,262 | 61,861 | 6,532 → 6,532 | 118 |
| 128 | 1 | 291,170 | 263,440 | 40,289 → 40,289 | 902 |
| 128 | 8 | 734,490 | 707,355 | 40,983 → 40,985 | 902 |

Durable cases use three 200ms samples and default node concurrency; the table
shows eight-P medians. SQLite width-32 has only two or three measured iterations.
Memory width-two medians are slower, while several wider cases are faster.
This does not establish a Store-wide persistence speedup.

| Store / width | Before ns/op | After ns/op | Before → after B/op | Before → after allocs/op |
| --- | ---: | ---: | --- | --- |
| Memory / 2 | 65,104 | 73,630 | 26,783 → 26,786 | 174 → 174 |
| Memory / 8 | 242,042 | 242,772 | 137,591 → 137,585 | 683 → 683 |
| Memory / 32 | 2,415,681 | 2,125,254 | 1,424,642 → 1,424,636 | 5520 → 5520 |
| SQLite / 2 | 9,395,017 | 9,190,348 | 40,462 → 40,512 | 678 → 678 |
| SQLite / 8 | 24,869,911 | 23,059,844 | 171,802 → 171,528 | 3504 → 3504 |
| SQLite / 32 | 89,083,233 | 80,756,033 | 1,489,482 → 1,500,418 | 38613 → 38618 |

### Profile and reliability

Fresh CPU/memory profiles used width 512, node concurrency eight, `-cpu=8`,
`-benchtime=3s`. Baseline `copyInvocations` accounted for 25.2% cumulative CPU;
the final version accounts for 16.3%, including the new run-copy helper at 16.0%.
Those shares overlap and must not be added. Whole `CopyInto` drops from 26.2%
to 19.2%. These are separately sampled executions including setup/calibration,
not absolute elapsed-time savings. Application map Clone remains 69.1% of
allocation bytes and is still a major cost; its ownership contract is unchanged.

Full tests, race tests, vet, and format checks passed. The final runtime also
passed the same 60-second-per-Store workload with 64 runs, eight callers,
width eight, node concurrency eight, and eight Ps. Width eight has nine
invocations during its activation, exercising the bulk-copy path. Elapsed time
is frozen after drain; rolling latency uses the latest 1,024 successful calls.

| Store | Successful calls | Calls/s | Rolling p50 / p95, ms | Cancelled calls | Sampled peak heap, B | Drain / recovery GC heap, B | Seed / peak / drain goroutines |
| --- | ---: | ---: | --- | ---: | ---: | --- | --- |
| Memory | 455,497 | 7,591.60 | 1.0566 / 1.6725 | 8 | 3,741,976 | 844,048 / 773,048 | 3 / 49 / 3 |
| SQLite | 2,525 | 42.08 | 182.5193 / 280.2083 | 8 | 3,101,784 | 784,656 / 732,984 | 4 / 15 / 4 |

Reload, partial-round recovery, and resource checks passed; goroutines returned
to seed levels. A separate one-second-per-Store race soak passed. These soaks
validate resource and recovery behavior, not a version-to-version throughput
claim. Raw benchmark samples, compiler diagnostics, profiles, and binaries
remain in the temporary directory outside the repository.

## Recoverable interruption capacity validation (format 3)

The runtime at commit `72f255a`, with the new interruption capacity fixtures,
was measured on 2026-10-09 using Go 1.26.5, Windows/amd64, and an AMD Ryzen 7
6800H. This validation adds tests and measurements, with no public API,
checkpoint-format, Store, or runtime changes.

The manual `TestCapacityInterruptionSoak` is skipped unless the existing
`LUNEGRAPH_SOAK_DURATION` variable is set. It runs Memory and SQLite separately,
using 64 runs, eight caller workers, eight branches, and node concurrency eight.
One complete round per run is seeded before timing; each run then starts at a
waiting continuation. The Store population is fixed. Each caller exclusively
advances its assigned runs; same-run competition uses separate deterministic
tests with channel barriers rather than this throughput workload.

```sh
go test -run '^$' -bench '^BenchmarkCapacityInterruption$' -benchmem -benchtime=500ms -count=3 -cpu '1,8' ./internal/capacitytest
LUNEGRAPH_SOAK_DURATION=60s go test -run '^TestCapacityInterruptionSoak$' -count=1 -v -timeout 4m -cpu 8 ./internal/capacitytest
LUNEGRAPH_SOAK_DURATION=1s go test -race -run '^TestCapacityInterruptionSoak$' -count=1 -v -timeout 2m -cpu 8 ./internal/capacitytest
```

PowerShell enables the same manual test as follows:

```powershell
$env:LUNEGRAPH_SOAK_DURATION = '60s'
try {
    go test -run '^TestCapacityInterruptionSoak$' -count=1 -v -timeout 4m -cpu 8 ./internal/capacitytest
} finally {
    Remove-Item Env:LUNEGRAPH_SOAK_DURATION
}
```

Unlike the earlier soak's `successful_calls`, these reports distinguish
`normal_calls`, `interrupted_calls`, and `cancelled_calls`. Non-cancelled
throughput and the rolling p50/p95 include both normal and interrupted public
calls, using the last 1,024 such latencies. Their duration includes persisted
reload and result checks by the harness. Step accounting uses returned
checkpoint deltas, not callback attempts or a fixed steps-per-call estimate.
An interrupted call can contain an earlier committed prefix; a cancelled call
can have an uncertain acknowledgement. After draining, the harness reloads
all runs and separately reports authoritative total steps and recovery steps.
The seeded population accounts for 640 steps in that authoritative total.

Resources are sampled every 500ms, with progress logs every five seconds.
Heap is process-wide Go live heap, including test state, pending readiness
records, runtime caches, and measurements; SQLite file sizes, native memory,
and filesystem cache are excluded. Measurements are bounded: the controller
retains only current callback identities and per-run active callback counts,
and latency storage is a fixed ring. Each public call must drain its own
callbacks before returning, independently of other active runs.

After cancellation, a fresh context reloads each authoritative checkpoint,
resubmits only still-waiting inputs, and advances partial rounds to their next
waiting boundary. If a continuation has interrupted without committing,
that input is also retried. Every final run must have one waiting invocation,
no groups, valid owner/state/counters, no failure marker, and no pending test
controller entries. Drain and recovery elapsed times are reported separately;
post-drain recovery does not contribute to throughput or latency samples.

These measurements establish behavior for this bounded test workload, not a
production capacity limit or a comparison with the successful-only workloads.
There are no cross-machine heap, latency, or throughput gates. Sampled goroutine
counts supplement deterministic callback-draining assertions; process-wide
resource samples alone are not a proof of absence of all resource leaks.

### Interruption round benchmarks

Three 500ms samples per configuration report median [minimum–maximum] in
milliseconds. Allocation columns are medians. SQLite executes 18–22 rounds
per sample here; these are diagnostic measurements rather than tail-latency
or sustained throughput estimates. No baseline comparison is implied.

| Store | Node concurrency | Ps | ms/round [range] | B/round | allocs/round |
| --- | ---: | ---: | --- | ---: | ---: |
| Memory | 1 | 1 | 0.869 [0.834–0.966] | 510,893 | 6,894 |
| Memory | 1 | 8 | 1.403 [1.240–1.416] | 511,148 | 6,894 |
| Memory | 8 | 1 | 0.574 [0.559–0.584] | 264,245 | 2,730 |
| Memory | 8 | 8 | 0.726 [0.710–0.779] | 264,543 | 2,732 |
| SQLite | 1 | 1 | 31.609 [30.573–33.173] | 956,272 | 17,741 |
| SQLite | 1 | 8 | 31.322 [30.854–31.535] | 966,867 | 17,753 |
| SQLite | 8 | 1 | 28.809 [27.613–28.946] | 384,811 | 7,480 |
| SQLite | 8 | 8 | 27.955 [27.400–28.405] | 393,085 | 7,493 |

Parallel first attempts can request interruption before their sibling results
are drained, letting a subsequent public call replay several branches together.
Serial first attempts instead require more public calls and more fixture
reloads and comparisons. The differing allocation counts therefore include
controller and harness behavior; they are not isolated scheduler allocation
measurements or an argument to increase concurrency for every application.

### Sixty-second interruption soak

Each Store ran separately on eight Ps. Duration is frozen after callers and
the sampler drain; latencies exclude post-drain recovery. Normal calls in this
workload reach a waiting round boundary; interrupted calls may still have
committed earlier results. Callback first-attempt counts also include seeding
and final recovery, and can exceed public interruption counts because of
discarded parallel results.

| Store | Normal / interrupted / cancelled calls | Non-cancelled calls/s | Rolling p50 / p95, ms | Observed committed steps |
| --- | --- | ---: | --- | ---: |
| Memory | 105,880 / 423,677 / 5 | 8,825.93 | 0.5367 / 2.6346 | 1,058,831 |
| SQLite | 2,173 / 8,869 / 5 | 183.95 | 33.7065 / 116.2744 | 21,966 |

| Store | Seed GC heap, B | Sampled peak heap, B | Drain / recovery GC heap, B | Seed / peak / drain / recovery goroutines | Drain / recovery, ms |
| --- | ---: | ---: | --- | --- | --- |
| Memory | 666,584 | 5,051,056 | 1,589,368 / 1,002,432 | 3 / 76 / 3 / 3 | 0.0000 / 43.5990 |
| SQLite | 788,536 | 3,010,008 | 1,127,296 / 851,912 | 4 / 78 / 4 / 4 | 26.1713 / 855.8139 |

Drain timing starts when the coordinator observes context cancellation, so a
rounded zero means the workers had already drained at that observation point.
It does not establish zero callback cancellation latency. Remaining post-GC
heap includes persisted runs, test metadata, and runtime caches; no fixed heap
threshold is applied.

Reload confirmed total committed steps of 1,059,471 for Memory and 22,607 for
SQLite, including the 640 seeded steps. Memory's returned-delta count matches
the loaded count after subtracting seeding. SQLite committed one additional
step across the cancellation/acknowledgement boundary, confirmed only by
reload. Recovery then committed 609 and 333 more steps respectively to finish
pending rounds. All 64 runs per Store ended at checked waiting boundaries;
pending controller entries and active callback counts were zero, and sampled
goroutine counts returned to seed levels.

The separate one-second-per-Store race soak also passed. Raw benchmark and
soak logs remain in the temporary directory outside the repository. The normal
CI tests exercise the same assertions at fixed round counts; the manual soaks
remain opt-in.


## Shared callback budget and result resolution

Collected on 2026-10-09 against baseline `d2baa6e`, with Go 1.26.5,
Windows/amd64, and AMD Ryzen 7 6800H. The baseline was extracted into a separate
source directory. Before/after benchmark commands ran serially; results remain
machine-specific. No Store settings, checkpoint format or commit boundaries
changed. SQLite retained WAL, synchronous FULL, and one open connection.

### Workload and correctness

`TestCapacitySharedLimiter` uses 64 persisted executions, eight caller workers,
eight branches, local node concurrency eight, and shared callback capacity one
or eight, with Memory and SQLite. Each run starts at a fork, joins independently
cloned mutable map states, waits, accepts typed continuation input, and repeats.
Seeding creates the first ten-step round; two subsequent rounds run concurrently
in normal tests. Returned and reloaded checkpoints validate owner, counters,
branch sums, topology and bounded state. A lightweight shared observer checks
node/join/Apply lifetimes, the global callback bound, and one committed
resolution for every started callback. Persisted slots retain references and
counters instead of duplicate application state.

Public API tests additionally cover different Runner/state types, cancellation
under an exhausted budget, invalid construction, clone failure, callback panic,
failure and interruption, and permit reuse. Observation tests cover CAS conflict,
uncertain writes both with and without a write, Store cancellation/deadline errors,
nested joins discarded together, accepted input prefixes, stable replay IDs,
observer panics, and draining callbacks after an escaping Store panic without
fabricating public completion or result resolution.

```sh
go test -run 'TestLimiter|TestSharedLimiter' .
go test -run '^TestCapacitySharedLimiter$' ./internal/capacitytest
go test -run 'TestResolved|TestStorePanic' ./internal/observationtest
go test -run '^$' -bench '^BenchmarkCapacitySharedLimiter$' -benchmem -benchtime=500ms -count=3 -cpu 8 ./internal/capacitytest
```

One shared-budget benchmark operation advances all 64 runs by one complete
round: 640 committed node steps, 64 joins and 64 continuation applications.
Store creation, graph compilation, seeding, caller construction and final
reload validation are outside timing. Callback observation, resolution, state
checks, individual public-call timing, batch samples and a 10 ms resource
sampler are included. Rows below select the sample with median batch time from
three final samples, retaining that sample's coherent latency/resource record.
The rolling latency window has at most 1,024 calls; it is not whole-run latency.

| Store | Shared capacity | Timed rounds per run | Duration, s | Batch, ms | Calls/s | Rolling p50 / p95, ms | Observed callback peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 1 | 33 | 0.6293 | 19.072 | 3,356.0 | 2.122 / 2.946 | 1 |
| Memory | 8 | 100 | 1.1188 | 11.189 | 5,720.2 | 1.149 / 1.901 | 6 |
| SQLite | 1 | 1 | 1.6622 | 1,661.780 | 38.5 | 195.453 / 278.896 | 1 |
| SQLite | 8 | 1 | 1.6243 | 1,624.315 | 39.4 | 186.135 / 286.708 | 4 |

| Store / capacity | Seed / sampled peak / drained Go heap, bytes | Seed / sampled peak / drained goroutines |
| --- | ---: | ---: |
| Memory / 1 | 594,184 / 3,043,840 / 678,688 | 3 / 20 / 3 |
| Memory / 8 | 774,704 / 3,656,712 / 783,152 | 3 / 63 / 3 |
| SQLite / 1 | 750,368 / 2,987,600 / 792,536 | 4 / 22 / 4 |
| SQLite / 8 | 766,816 / 3,199,624 / 796,816 | 4 / 78 / 4 |

All measured cases drained and reloaded 64 valid waiting checkpoints. Shared
capacity is an upper bound, not a promised attained concurrency. SQLite final
samples each completed only one timed batch despite 500 ms requests; these
finite runs do not establish sustained capacity or prove absence of leaks.
Resources are process-wide Go measurements, exclude SQLite native memory and
filesystem cache, and sampled peaks may miss shorter spikes. Callers and idle
workers are outside the callback bound. These fixtures do not implement host
admission, FIFO scheduling, ownership or an external-effect receipt protocol.

### Runtime cost and observation tradeoffs

The initial 100 ms / three-sample scan covered sequential sizes 1/16/128,
fan-out widths 2/8/32, paused inspect/advance with 128 runs, and the existing
observation workloads. A short sequential-128 sample initially suggested an
83% one-P slowdown. The longer paired 500 ms / three-sample recheck did not
reproduce it. The following table reports median [range] in microseconds from
the longer recheck, rather than treating separated short samples as a stable
speed comparison:

| Workload | Ps | Baseline, us | Updated, us | Baseline / updated allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Sequential, 128 steps | 1 | 196.538 [175.621–197.426] | 182.251 [181.449–191.912] | 524 / 524 |
| Sequential, 128 steps | 8 | 543.794 [529.413–544.365] | 554.066 [401.579–567.244] | 531 / 524 |
| Fan-out, width 32, serial | 1 | 92.267 [89.798–105.300] | 89.352 [82.793–93.341] | 204 / 204 |
| Fan-out, width 32, serial | 8 | 215.472 [214.556–217.939] | 217.714 [156.133–219.577] | 204 / 204 |
| Fan-out, width 32, default | 1 | 99.345 [95.961–101.782] | 104.865 [99.426–118.612] | 204 / 204 |
| Fan-out, width 32, default | 8 | 155.017 [154.698–163.733] | 161.193 [161.115–164.913] | 211 / 211 |

```sh
go test -run '^$' -bench '^BenchmarkSequentialExecution$/^steps=128$' -benchmem -benchtime=500ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkFanoutJoin$/^width=32$' -benchmem -benchtime=500ms -count=3 -cpu '1,8' .
go test -run '^$' -bench '^BenchmarkCapacityPaused/store=(memory|sqlite)/runs=128/' -benchmem -benchtime=100ms -count=3 -cpu 8 ./internal/capacitytest
go test -run '^$' -bench '^BenchmarkObservation$' -benchmem -benchtime=100ms -count=3 -cpu 8 ./internal/observationtest
```

Paired timing ranges overlap and do not support a general throughput claim.
Lazy workers do reduce repeatable allocation counts: eight-P sequential work
uses seven fewer worker allocations; width-two default fan-out uses six fewer.
Paused inspect creates no workers: Memory drops from 28 to 20 allocs/op and
SQLite from 117 to 109. Their short-sample median inspect times were 12.035 to
6.716 us and 55.229 to 46.927 us respectively; these timings are descriptive,
not a portable target. Existing advance allocation counts were unchanged.

Enabled observation pays for result correlation and one extra event per
execution callback. In the short eight-P sequential observer samples, no-op
observation changed from 84 to 96 allocs/op and median 61.053 to 100.421 us;
Debug JSON slog changed from 151 to 195 allocations and 220.595 to 365.749 us.
Width-32 fan-out no-op observation changed from 208 to 249 allocations and
123.450 to 209.368 us; slog from 348 to 457 allocations and 241.536 to
524.640 us. These fixtures have very small callbacks and make observer costs
visible. Nil observation has no session, ledger, operation IDs or clock reads.
The resolution delivery buffer is reused and cleared after delivery, and the
ledger retains unresolved metadata only. Consumers should budget for enabled
observation and logging rather than assuming the additional phase is free.

Raw logs were saved outside the repository under the local temporary directory
as `lunegraph-limiter-baseline-{root,paused,observe}.txt`,
`lunegraph-limiter-after-{root,paused,observe}.txt`,
`lunegraph-limiter-paired-{before,after}.txt`,
`lunegraph-limiter-fanout-{before,after}.txt`, and
`lunegraph-limiter-capacity-bench.txt`.
