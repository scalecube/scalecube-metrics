# scalecube-metrics

High-performance KPI and telemetry library for Java, designed for ultra-low-latency and precise
metrics. Built on Agrona Counters and HdrHistograms, it supports both real-time monitoring and
historical data captures. The library is ideal for systems that require fine-grained performance
insights, high-frequency metrics, and minimal runtime overhead. Lightweight and thread-safe, it can
be integrated into performance-critical applications such as trading platforms, messaging systems,
or any low-latency environment.

## Configuration

`Context` settings come from a `java.util.Properties` object; the no-arg constructor reads
`System.getProperties()`. Any property set to `@null` is treated as not set (default applies), so a
layered or templated config can unset an inherited value.

## CountersStat

`io.scalecube.metrics.CountersStat` prints the counters a process maintains in its
memory-mapped `counters.dat`. The file lives in shared memory, so the tool reads it from a separate
process without touching the one being observed. Agrona reads the mapped file through
`jdk.internal.misc.Unsafe`, so the tool needs the same `--add-opens` the observed process runs with:

```
java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp "app.jar:lib/*" \
  io.scalecube.metrics.CountersStat
```

```
15:23:16 - Counters Stat (/dev/shm/counters-app), pid 210056, started 2026-09-16 15:23:13, up 0:00:03
======================================================================
    0:                  1,204,551 - duty_cycle_max_time_ns{roleName=WORKER}
    1:                     12,483 - errors_total{reason=BACK_PRESSURE, roleName=SENDER}
    2:                          3 - resources_total{action=ADDED, kind=connection}
    3:                          0 - tcp.address=0.0.0.0:11080{counterVisibility=PRIVATE}
--
```

A counter name on its own is not unique - the same name is allocated many times with different tags

- so the tags are part of the identity and are rendered next to the name, sorted by tag name so
  consecutive redraws are diffable.

| Argument              | Meaning                            | Default |
|-----------------------|------------------------------------|---------|
| `delay=<seconds>`     | redraw interval                    | `1`     |
| `watch=<true\|false>` | loop or single shot                | `true`  |
| `name=<regex>`        | filter on counter name             | -       |
| `type=<regex>`        | filter on `typeId`                 | -       |
| `tag=<regex>`         | filter on any rendered `tag=value` | -       |

`-?` / `-h` / `-help` prints usage. Filters are `Pattern.find()` and are applied together. The
counters directory is resolved through `CountersRegistry.Context`, so the property and the default
are the observed process's own:
`-Dscalecube.metrics.counters.countersDirectoryName=<dir>`.

The header carries the `pid` and the start timestamp recorded in the file, and marks the pid
`(not running)` when that process is gone - a counters file left behind by a crashed process does
not look healthy. A file that is truncated, not yet initialised, or shorter than its own header
declares is reported rather than read. When the file is missing, the tool lists any directory next
to the one asked for, next to the library default, or in the temp directory that does hold a
`counters.dat` - the case where the observed process runs as a different user.

Unlike `CountersReaderAgent`, the tool prints `PRIVATE`-visibility counters too: that is where
`PropertiesRegistry` puts values such as a bound address, and an operator wants to see them.

## JVM metrics

`io.scalecube.metrics.jvm.JvmMetricsReaderAgent` reads JVM metrics of another JVM from its HotSpot
`hsperfdata` file (`/tmp/hsperfdata_<user>/<pid>`), which every HotSpot JVM writes by default.
Resident memory, including off-heap, comes from the JVM's `/proc/<pid>/statm` (optional). The agent
reads the files it is given; finding them (e.g. through `/proc/<pid>/root`) is up to the caller.

Every read interval the file is mapped read-only, parsed and unmapped. Values go to a
`CountersHandler` as `CounterDescriptor`s (metric name as label, `gc` tag for GC metrics), so
`CountersPrometheusAdapter` exposes them as is. A missing, not yet initialised, or unsupported file
yields an empty list, so the handler never keeps values of a JVM that has gone.

Times are converted from ticks with `sun.os.hrt.frequency`. The JVM updates values without a lock,
so one read may mix values from just before and just after a GC: each value is right on its own,
don't alert on a relation between two values of the same read.

### Out of process: no influence on the target JVM

The agent runs in another process and only reads what the JVM and the kernel maintain anyway:
`hsperfdata`, which the JVM writes whether or not anyone reads it, and the kernel's `statm`
counters. Nothing of the agent runs in the target JVM:

- no thread, no allocation on its heap, no class loaded, no MXBean call: the agent adds no GC, JIT
  or CPU work to the target, and its own GC pauses, hangs or crashes do not reach it;
- no attach: the Attach API starts a thread in the target and runs commands there, many at a
  safepoint;
- no safepoint, no handshake, no signal, no lock that the target's threads take (see below for
  `statm`);
- no code or dependency in the target: any HotSpot JVM can be read, including third-party ones.

How often the agent reads costs only the agent. An in-process agent (MXBeans, JMX, a metrics
library) shares heap, CPU, GC and safepoints with the application it measures.

What remains: a read pulls the `hsperfdata` cache lines into the reader's CPU, so the JVM's next
write to each of them misses its cache once; and `statm` takes the main thread's `task_lock` for an
instant. The JVM's own cost of writing `hsperfdata` is there without the agent: on a disk-backed
`/tmp`, writeback of the file can stall the JVM, which is why some deployments run with
`-XX:+PerfDisableSharedMem`; put `/tmp` on tmpfs instead, the agent needs the shared file.

### Why `statm`, not `status`, `smaps` or `smaps_rollup`

The reader must not take a lock that the JVM's own threads take. Of the four files, only `statm`
meets that.

| File           | What the kernel does on read                                    | Effect on the JVM                                                                                                          |
|----------------|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `statm`        | reads the process's page counters                               | none: the only lock is the main thread's `task_lock`, for an instant, to get its memory descriptor                         |
| `status`       | the same counters, plus signal state copied under the `siglock` | a JVM thread getting a signal waits (HotSpot raises `SIGSEGV` for implicit null checks); thread start and exit take it too |
| `smaps`        | walks the page tables of every mapping under the `mmap_lock`    | `mmap`, `munmap`, `mprotect` (heap commit and uncommit, thread stacks) wait for the walk, which grows with resident memory |
| `smaps_rollup` | the same walk, summed                                           | the same as `smaps`                                                                                                        |

Given up: per-mapping breakdown and proportional (PSS) sizes. Resident, anonymous and shared
totals are enough for the off-heap bounds below.

### Metrics

| Metric                                      | Type    | Meaning                                                                                                                                                                                  | Source                                                  |
|---------------------------------------------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| `jvm_safepoints_total`                      | counter | Stop-the-world operations since JVM start: GC pauses, deoptimization, class redefinition, thread dumps, ...                                                                              | `sun.rt.safepoints`                                     |
| `jvm_safepoint_time_nanoseconds_total`      | counter | Time Java threads were stopped at safepoints, from the moment all had stopped. Excludes reaching the safepoint: whole pause = this + sync time                                           | `sun.rt.safepointTime`                                  |
| `jvm_safepoint_sync_time_nanoseconds_total` | counter | Time to bring all Java threads to safepoints (time to safepoint). High when a thread runs long without a safepoint poll, while the others are already stopped                            | `sun.rt.safepointSyncTime`                              |
| `jvm_gc_collections_total{gc}`              | counter | Collections per collector. `gc`: `G1 young`, `G1 full`, `G1 concurrent cycle pauses`; `ZGC minor`, `ZGC major collection pauses`; `Parallel`, `Serial` `young`, `full collection pauses` | `sun.gc.collector.N.invocations`, `gc` = `.name`        |
| `jvm_gc_pause_nanoseconds_total{gc}`        | counter | Pause time of that collector (G1 concurrent cycle: its remark and cleanup pauses). Shenandoah: includes concurrent cycles                                                                | `sun.gc.collector.N.time`                               |
| `jvm_memory_heap_used_bytes`                | gauge   | Heap occupied by objects, live and not yet collected. G1: follows allocation between GCs; ZGC, Parallel, Serial: as of the last GC                                                       | sum of `sun.gc.generation.N.space.M.used`               |
| `jvm_memory_heap_committed_bytes`           | gauge   | Heap taken from the OS; resident only once touched (see below)                                                                                                                           | sum of `sun.gc.generation.N.capacity`                   |
| `jvm_memory_heap_max_bytes`                 | gauge   | Heap limit, `-Xmx`                                                                                                                                                                       | `sun.gc.generation.N.maxCapacity`: equal → it, else sum |
| `jvm_threads_live`                          | gauge   | Live Java platform threads, daemon included. JIT and GC threads, and virtual threads, are not counted                                                                                    | `java.threads.live`                                     |
| `jvm_threads_daemon`                        | gauge   | Live daemon threads, part of live                                                                                                                                                        | `java.threads.daemon`                                   |
| `jvm_threads_peak`                          | gauge   | Highest live since JVM start                                                                                                                                                             | `java.threads.livePeak`                                 |
| `jvm_threads_started_total`                 | counter | Threads started since JVM start; its rate is thread churn                                                                                                                                | `java.threads.started`                                  |
| `jvm_memory_metaspace_used_bytes`           | gauge   | Class metadata in use. As of the last GC, 0 before the first                                                                                                                             | `sun.gc.metaspace.used`                                 |
| `jvm_memory_metaspace_committed_bytes`      | gauge   | Memory taken from the OS for class metadata. As of the last GC, 0 before the first                                                                                                       | `sun.gc.metaspace.capacity`                             |
| `jvm_resident_memory_bytes`                 | gauge   | Physical memory of the whole process (RSS): heap, off-heap, metaspace, code cache, stacks, mapped files, libraries                                                                       | `statm` resident                                        |
| `jvm_resident_memory_anon_bytes`            | gauge   | Resident memory not backed by a file: heap (not ZGC), direct buffers, native allocations, metaspace, stacks                                                                              | `statm` resident − shared                               |
| `jvm_resident_memory_shared_bytes`          | gauge   | Resident memory backed by a file or shared memory: mapped files (e.g. Aeron in `/dev/shm`), ZGC heap, JVM libraries and jars                                                             | `statm` shared                                          |

Off-heap memory (direct buffers, Agrona/Aeron off-heap, native) is not measured directly; it is
derived from resident memory, and which part holds the heap depends on the collector:

| Collector            | Anonymous memory holds               | Shared memory holds                   | Off-heap                                                                 |
|----------------------|--------------------------------------|---------------------------------------|--------------------------------------------------------------------------|
| G1, Parallel, Serial | heap + off-heap + metaspace + stacks | mapped files (e.g. Aeron, `/dev/shm`) | `anon − heap committed` (lower bound) … `anon − heap used` (upper bound) |
| ZGC                  | off-heap + metaspace + stacks        | heap + mapped files                   | `anon`                                                                   |

Heap committed but never touched is not resident unless the JVM runs with `-XX:+AlwaysPreTouch`,
hence the bounds. Shared memory counts only pages the process touched, and a file mapped by two
processes (e.g. an Aeron media driver and its client) counts in both.

### Requirements and tests

The target JVM must not run with `-XX:-UsePerfData` or `-XX:+PerfDisableSharedMem`.

The counter names are JDK internals and may change in any JDK release.
`JvmMetricsReaderAgentJdkTest` checks every metric against an oracle that shares no code with the
agent, on the JDK the build runs on, so CI must run on the JDK the target services run on. Its
javadoc lists the oracle and the test of each metric.

### Example

In `metrics-examples` (Linux): start `io.scalecube.metrics.jvm.JvmMetricsSourceRunner` with
`-Xmx64m`, a JVM holding a direct buffer and a mapped `/dev/shm` file; it prints its pid. Then
`io.scalecube.metrics.jvm.JvmMetricsReaderRunner <pid>` prints its JVM metrics every 3 seconds.
