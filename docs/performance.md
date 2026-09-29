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

“Reopen” means the SQLite store and database handles are closed and opened again for each measured recovery. The operating system's filesystem cache is not flushed, so this does not claim a cold-disk measurement.

For comparisons, run the command more than once on an otherwise idle machine and keep the Go version and `-cpu` values fixed. The tests validate every result inside the measured path; this adds a small amount of assertion work to each operation.

## Reliability checks

Run the correctness and static checks alongside benchmark work:

```sh
go test ./...
go test -race ./...
go vet ./...
```

The reliability baseline is the semantic test suite, including the SQLite process-crash and reopen tests in `checkpoint/sqlite`, commit-acknowledgement ambiguity tests, cross-process CAS tests, and execution tests for replay identities, cancellation, and failure handling. A change to execution or persistence semantics must preserve or update those checks. No benchmark score substitutes for them.
