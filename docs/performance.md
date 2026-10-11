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

## Recent format-4 comparison

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
