# Performance, concurrency, and reliability baseline

The project uses three engineering standards:

- **High performance:** keep repeatable benchmarks for graph scheduling and checkpoint persistence. Compare results on the same Go version and machine before and after runtime changes.
- **High concurrency:** measure parallel branches inside one run and independent runs that share a compiled runner and a store. The runner and built-in stores are expected to support the concurrency documented by their APIs.
- **High reliability:** verify execution and recovery semantics with semantic and fault tests, including cancellation, replay, compare-and-swap conflicts, process crashes, and uncertain commit acknowledgements. Reliability is not represented by a numeric benchmark or an exactly-once claim.

These standards do not set machine-independent latency targets or a CI performance gate. Benchmark results depend on hardware, operating system, filesystem, Go version, and SQLite settings.

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

The durable execution benchmarks create their initial run before timing and repeatedly call `Recover` with the same run ID. Every executed node outcome is persisted through the normal checkpoint path. The loop leaves a bounded checkpoint row with ready work after each measured call, so iterations measure recovery and execution without growing the number of stored runs. Each case loads the final checkpoint after timing and checks it against the returned result.

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

Invocation lookup uses a temporary ID index when a checkpoint has more than eight invocations. Shorter lists use a direct scan to avoid map setup cost. The index follows fan-out insertion and group compaction, is rebuilt when a saved execution resumes, and is never written to a checkpoint.

## Reliability checks

Run the correctness and static checks alongside benchmark work:

```sh
go test ./...
go test -race ./...
go vet ./...
```

The reliability baseline is the semantic test suite, including the SQLite process-crash and reopen tests in `checkpoint/sqlite`, commit-acknowledgement ambiguity tests, cross-process CAS tests, and execution tests for replay identities, cancellation, and failure handling. A change to execution or persistence semantics must preserve or update those checks. No benchmark score substitutes for them.
