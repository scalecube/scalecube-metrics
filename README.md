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

Times are in nanoseconds, sizes in bytes. A counter only grows (use `rate` or `increase`), a gauge
is the current value. Exposed with no `# TYPE`, so Prometheus takes them as untyped; the functions
below work the same.

### Safepoint metrics

**Metric:** `jvm_safepoints_total`, **Source:** `sun.rt.safepoints`, **Type:** counter\
**Usage:**
- `rate(jvm_safepoints_total[1m])`: safepoints per second
- `increase(jvm_safepoints_total[1h])`: safepoints in the last hour

**Description:** Safepoints since JVM start: every Java thread stopped at once. GC pauses of every
collector (ZGC's short ones included), class redefinition (agents, hot swap), thread and heap dumps
(`jstack`, `jmap`, `ThreadMXBean.dumpAllThreads`), ... Operations that stop one thread at a time
(handshakes) are not counted.

---

**Metric:** `jvm_safepoint_time_nanoseconds_total`, **Source:** `sun.rt.safepointTime`, **Type:** counter\
**Usage:**
- `(rate(jvm_safepoint_time_nanoseconds_total[1m]) + rate(jvm_safepoint_sync_time_nanoseconds_total[1m])) / 1e9`:
  share of time the application was stopped (0.01 = 1%)
- `rate(jvm_safepoint_time_nanoseconds_total[5m]) / rate(jvm_safepoints_total[5m]) / 1e6`: average
  time at a safepoint, ms

**Description:** Time Java threads were held at safepoints, from the moment the last one stopped
until they were released. Reaching the safepoint is not included: that is the sync time, and the
whole stop is the two together. GC pauses are part of it (also counted in
`jvm_gc_pause_nanoseconds_total`), so don't add the two. Threads in native code keep running during
a safepoint, and stop only when they return to Java.

---

**Metric:** `jvm_safepoint_sync_time_nanoseconds_total`, **Source:** `sun.rt.safepointSyncTime`, **Type:** counter\
**Usage:**
- `rate(jvm_safepoint_sync_time_nanoseconds_total[5m]) / rate(jvm_safepoints_total[5m]) / 1e6`:
  average time to safepoint, ms
- `rate(jvm_safepoint_sync_time_nanoseconds_total[5m]) > rate(jvm_safepoint_time_nanoseconds_total[5m])`:
  threads wait longer for the safepoint than at it

**Description:** Time from requesting a safepoint until the last Java thread stopped (time to
safepoint). The threads that stopped first wait for the last one, so this is pause time too. High
when a thread runs long without reaching a safepoint poll (e.g. a long loop in JIT-compiled code, a
large array copy), or is not on a CPU when the safepoint is requested (CPU limits, more threads
than cores).

### GC metrics

**Metric:** `jvm_gc_collections_total{gc}`, **Source:** `sun.gc.collector.N.invocations`, **Type:** counter\
**Usage:**
- `rate(jvm_gc_collections_total[1m])`: pauses per second, per collector
- `increase(jvm_gc_collections_total{gc=~".*full.*"}[1h]) > 0`: a full GC happened in the last hour

**Description:** Pauses per collector. For young and full collectors one pause is one collection;
G1's concurrent cycle and ZGC have several pauses per collection. `gc` is `sun.gc.collector.N.name`:
- `G1 young collection pauses`: young pauses, concurrent start and mixed ones included
- `G1 full collection pauses`: full collections, `System.gc()` included (unless
  `-XX:+ExplicitGCInvokesConcurrent`); the JDK calls it itself when direct memory runs out
- `G1 concurrent cycle pauses`: remark and cleanup, 2 per concurrent cycle
- `ZGC minor collection pauses`, `ZGC major collection pauses`: about 3 per minor collection, 5 per
  major
- `Parallel young collection pauses`, `Parallel full collection pauses`, `Serial young collection
  pauses`, `Serial full collection pauses`: one per collection

Shenandoah reports everything under `Shenandoah full`, with counts that do not match its
collections; it is not covered by the tests.

---

**Metric:** `jvm_gc_pause_nanoseconds_total{gc}`, **Source:** `sun.gc.collector.N.time`, **Type:** counter\
**Usage:**
- `rate(jvm_gc_pause_nanoseconds_total[1m]) / 1e9`: share of time in GC pauses, per collector
- `rate(jvm_gc_pause_nanoseconds_total[5m]) / rate(jvm_gc_collections_total[5m]) / 1e6`: average
  pause, ms, per collector

**Description:** Total time of the pauses counted in `jvm_gc_collections_total`. Concurrent work
(G1's marking, almost all of ZGC's work) runs beside the application and is not included. A total
only: no maximum or distribution of single pauses, so one long pause shows as a spike of the rate,
not as its length; GC logs or JFR have each pause. Shenandoah: includes concurrent work, see above.

### Heap metrics

**Metric:** `jvm_memory_heap_used_bytes`, **Source:** sum of `sun.gc.generation.N.space.M.used`, **Type:** gauge\
**Usage:**
- `jvm_memory_heap_used_bytes / jvm_memory_heap_max_bytes`: heap occupancy
- `min_over_time(jvm_memory_heap_used_bytes[15m])`: the low point, close to the live data after
  GC; rising over hours suggests a leak

**Description:** Heap occupied by objects, live ones and garbage not yet collected. When it changes
depends on the collector: G1 follows allocation between GCs; ZGC, Parallel and Serial update it at
GC only, so between GCs it holds the value of the last GC.

---

**Metric:** `jvm_memory_heap_committed_bytes`, **Source:** sum of `sun.gc.generation.N.capacity`, **Type:** gauge\
**Usage:**
- `jvm_memory_heap_committed_bytes / jvm_memory_heap_max_bytes`: how far the heap has grown
- `jvm_resident_memory_anon_bytes - jvm_memory_heap_committed_bytes`: non-heap memory, lower bound
  (G1, Parallel, Serial)

**Description:** Heap memory taken from the OS. Constant when `-Xms` equals `-Xmx`, otherwise the GC
grows and shrinks it. Resident only once touched, unless the JVM runs with `-XX:+AlwaysPreTouch`.
Parallel and Serial: both survivor spaces are included, while their `MemoryPoolMXBean`s show one.

---

**Metric:** `jvm_memory_heap_max_bytes`, **Source:** `sun.gc.generation.N.maxCapacity` (all equal: that value, else their sum), **Type:** gauge\
**Usage:**
- `min_over_time(jvm_memory_heap_used_bytes[5m]) / jvm_memory_heap_max_bytes > 0.9`: the heap stays
  nearly full even after GCs

**Description:** Heap limit: `-Xmx`, or when it is not set, the JVM default: a quarter of RAM, or of
the container's memory limit (`-XX:MaxRAMPercentage`). Live data near it ends in back-to-back full
GCs and `OutOfMemoryError`.

### Thread metrics

**Metric:** `jvm_threads_live`, **Source:** `java.threads.live`, **Type:** gauge\
**Usage:**
- `jvm_threads_live`: thread count
- `delta(jvm_threads_live[1h]) > 0`: thread count still growing

**Description:** Live Java platform threads, daemon ones included: the application's and the JVM's
own Java threads (`main`, `Reference Handler`, `Finalizer`, `Signal Dispatcher`, `Common-Cleaner`,
...). JIT compiler and GC threads are not counted, so the process has more OS threads than this.
Virtual threads are not counted; their carrier threads (`ForkJoinPool-1-worker-N`) are.

---

**Metric:** `jvm_threads_daemon`, **Source:** `java.threads.daemon`, **Type:** gauge\
**Usage:**
- `jvm_threads_live - jvm_threads_daemon`: non-daemon threads, the ones that keep the JVM running

**Description:** Live daemon threads, part of live.

---

**Metric:** `jvm_threads_peak`, **Source:** `java.threads.livePeak`, **Type:** gauge\
**Usage:**
- `jvm_threads_peak - jvm_threads_live`: how far above now the thread count has been

**Description:** Highest live thread count since JVM start, or since
`ThreadMXBean.resetPeakThreadCount()` was called in the target JVM.

---

**Metric:** `jvm_threads_started_total`, **Source:** `java.threads.started`, **Type:** counter\
**Usage:**
- `rate(jvm_threads_started_total[5m])`: thread churn, threads started per second; near 0 in a
  steady state

**Description:** Platform threads started since JVM start, ended ones included; virtual threads are
not counted. Steady growth means threads are created per task instead of being pooled.

### Metaspace metrics

**Metric:** `jvm_memory_metaspace_used_bytes`, **Source:** `sun.gc.metaspace.used`, **Type:** gauge\
**Usage:**
- `delta(jvm_memory_metaspace_used_bytes[1h])`: class metadata growth; steady growth is classes
  generated and never unloaded (proxies, lambdas, scripts, redeploys)

**Description:** Class metadata in use, class space included. Updated at GC only: 0 before the
first GC, then the value of the last GC, so with rare GCs it lags behind class loading. Not limited
by default: without `-XX:MaxMetaspaceSize` a class leak grows native memory until the process runs
out of it.

---

**Metric:** `jvm_memory_metaspace_committed_bytes`, **Source:** `sun.gc.metaspace.capacity`, **Type:** gauge\
**Usage:**
- `jvm_memory_metaspace_committed_bytes - jvm_memory_metaspace_used_bytes`: committed but unused

**Description:** Memory taken from the OS for class metadata, class space included; this is what
`-XX:MaxMetaspaceSize` limits. Updated at GC only, like used. Counted in
`jvm_resident_memory_anon_bytes` once touched.

### Resident memory metrics

**Metric:** `jvm_resident_memory_bytes`, **Source:** `statm` resident, **Type:** gauge\
**Usage:**
- `jvm_resident_memory_bytes`: what the process takes in RAM
- `max_over_time(jvm_resident_memory_bytes[1d])`: daily peak

**Description:** Physical memory of the whole process (RSS): heap, metaspace, JIT code cache, GC
data structures, thread stacks, direct buffers and other native memory, mapped files, libraries.
Pages swapped out are not included. Only pages the process touched count, and a page shared with
another process (e.g. an Aeron log buffer mapped by the media driver and its client) counts in
both. Not the same as a container's memory usage: the cgroup also charges page cache and
`/dev/shm` files, and charges a shared page once.

---

**Metric:** `jvm_resident_memory_anon_bytes`, **Source:** `statm` resident − shared, **Type:** gauge\
**Usage:**
- `jvm_resident_memory_anon_bytes - jvm_memory_heap_committed_bytes`: non-heap memory, lower bound
  (G1, Parallel, Serial)
- `jvm_resident_memory_anon_bytes - jvm_memory_heap_used_bytes`: non-heap memory, upper bound (G1,
  Parallel, Serial)
- `jvm_resident_memory_anon_bytes`: non-heap memory (ZGC)
- `jvm_resident_memory_anon_bytes - jvm_memory_heap_committed_bytes - jvm_memory_metaspace_committed_bytes`:
  non-heap memory besides metaspace, lower bound (G1, Parallel, Serial)

**Description:** Resident memory not backed by a file: the heap (except ZGC), direct buffers and
other native allocations, metaspace, JIT code cache, GC data structures, thread stacks. So anonymous
minus heap is all non-heap memory of the JVM, not only direct buffers. It is a range because heap
committed but never touched is not resident, unless the JVM runs with `-XX:+AlwaysPreTouch`.

---

**Metric:** `jvm_resident_memory_shared_bytes`, **Source:** `statm` shared, **Type:** gauge\
**Usage:**
- `jvm_resident_memory_shared_bytes`: mapped files and libraries (G1, Parallel, Serial)
- `jvm_resident_memory_shared_bytes - jvm_memory_heap_committed_bytes`: mapped files and
  libraries (ZGC with `-XX:+AlwaysPreTouch`)

**Description:** Resident memory backed by a file or shared memory: mapped files (e.g. Aeron log
buffers in `/dev/shm`), the ZGC heap, the JVM's libraries, jars and CDS archive.

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
