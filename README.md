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
yields an empty list.

The JVM deletes its `hsperfdata` file on a clean exit only: after `kill -9`, an out-of-memory kill
or a crash the file stays, valid, with the last values. With `statm` given, the agent takes an
unreadable `statm`, or one with 0 resident pages (a killed process its parent has not reaped yet),
as the process gone and yields an empty list. Without it, the agent cannot tell
a gone JVM from a live one, and that is up to the caller; reading the file through
`/proc/<pid>/root` does it, as that path goes away with the process.

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

The collector decides how several metrics behave. ZGC (generational, JDK 25) comes first below; G1,
Parallel and Serial follow where they differ.

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

**Metric:** `jvm_safepoint_time_nanoseconds_total`, **Source:** `sun.rt.safepointTime`, **Type:**
counter\
**Usage:**

-
`(rate(jvm_safepoint_time_nanoseconds_total[1m]) + rate(jvm_safepoint_sync_time_nanoseconds_total[1m])) / 1e9`:
share of time the application was stopped (0.01 = 1%)
- `rate(jvm_safepoint_time_nanoseconds_total[5m]) / rate(jvm_safepoints_total[5m]) / 1e6`: average
  time at a safepoint, ms

**Description:** Time Java threads were held at safepoints, from the moment the last one stopped
until they were released. Reaching the safepoint is not included: that is the sync time, and the
whole stop is the two together. GC pauses are part of it (also counted in
`jvm_gc_pause_nanoseconds_total`), so don't add the two. Threads in native code keep running during
a safepoint, and stop only when they return to Java.

---

**Metric:** `jvm_safepoint_sync_time_nanoseconds_total`, **Source:** `sun.rt.safepointSyncTime`,
**Type:** counter\
**Usage:**

- `rate(jvm_safepoint_sync_time_nanoseconds_total[5m]) / rate(jvm_safepoints_total[5m]) / 1e6`:
  average time to safepoint, ms
-
`rate(jvm_safepoint_sync_time_nanoseconds_total[5m]) > rate(jvm_safepoint_time_nanoseconds_total[5m])`:
threads wait longer for the safepoint than at it

**Description:** Time from requesting a safepoint until the last Java thread stopped (time to
safepoint). The threads that stopped first wait for the last one, so this is pause time too. High
when a thread runs long without reaching a safepoint poll (e.g. a long loop in JIT-compiled code, a
large array copy), or is not on a CPU when the safepoint is requested (CPU limits, more threads
than cores).

### GC metrics

**Metric:** `jvm_gc_pauses_total{gc}`, **Source:** `sun.gc.collector.N.invocations`, **Type:**
counter\
**Usage:**

- `rate(jvm_gc_pauses_total[1m])`: pauses per second, per collector
- `increase(jvm_gc_pauses_total{gc=~".*full.*"}[1h]) > 0`: a full GC happened in the last hour

**Description:** Pauses per collector. ZGC and G1's concurrent cycle have several pauses per
collection; for young and full collectors one pause is one collection. Each pause is one safepoint.
`gc` is `sun.gc.collector.N.name`:

- `ZGC minor collection pauses`: 3 per minor (young) collection
- `ZGC major collection pauses`: about 5 per major (young and old) collection; `System.gc()` runs
  one. hsperfdata has no count of ZGC collections (cycles) or of their concurrent time: divide the
  pauses
- `G1 young collection pauses`: young pauses, concurrent start and mixed ones included
- `G1 full collection pauses`: full collections, `System.gc()` included (unless
  `-XX:+ExplicitGCInvokesConcurrent`); the JDK calls it itself when direct memory runs out
- `G1 concurrent cycle pauses`: remark and cleanup, 2 per concurrent cycle
- `Parallel young collection pauses`, `Parallel full collection pauses`, `Serial young collection
  pauses`, `Serial full collection pauses`: one per collection

Shenandoah reports everything under `Shenandoah full`, with counts that do not match its
collections; it is not covered by the tests.

---

**Metric:** `jvm_gc_pause_nanoseconds_total{gc}`, **Source:** `sun.gc.collector.N.time`, **Type:**
counter\
**Usage:**

- `rate(jvm_gc_pause_nanoseconds_total[1m]) / 1e9`: share of time in GC pauses, per collector
- `rate(jvm_gc_pause_nanoseconds_total[5m]) / rate(jvm_gc_pauses_total[5m]) / 1e6`: average
  pause, ms, per collector

**Description:** Total time of the pauses counted in `jvm_gc_pauses_total`. Concurrent work (almost
all of ZGC's work, G1's marking) runs beside the application and is not included. ZGC's
pauses take microseconds: measured, 15,820 minor pauses took 46 ms in total. A total
only: no maximum or distribution of single pauses, so one long pause shows as a spike of the rate,
not as its length; GC logs or JFR have each pause. Shenandoah: includes concurrent work, see above.

### Heap metrics

**Metric:** `jvm_memory_heap_used_bytes`, **Source:** sum of `sun.gc.generation.N.space.M.used`,
**Type:** gauge\
**Usage:**

- `jvm_memory_heap_used_bytes / jvm_memory_heap_max_bytes`: heap occupancy
- `min_over_time(jvm_memory_heap_used_bytes[15m])`: the low point, close to the live data after
  GC; rising over hours suggests a leak

**Description:** Heap occupied by objects, live ones and garbage not yet collected. When it changes
depends on the collector: ZGC, Parallel and Serial update it at GC only, so between GCs it holds the
value of the last GC; with ZGC that is close to the live data plus what was allocated during the
cycle. G1 follows allocation between GCs.

---

**Metric:** `jvm_memory_heap_committed_bytes`, **Source:** sum of `sun.gc.generation.N.capacity`,
**Type:** gauge\
**Usage:**

- `jvm_memory_heap_committed_bytes / jvm_memory_heap_max_bytes`: how far the heap has grown
- `jvm_resident_memory_shared_bytes - jvm_memory_heap_committed_bytes`: mapped files, lower bound
  (ZGC; exact with `-XX:+AlwaysPreTouch`)
- `jvm_resident_memory_anon_bytes - jvm_memory_heap_committed_bytes`: non-heap memory, lower bound
  (G1, Parallel, Serial)

**Description:** Heap memory taken from the OS. Constant when `-Xms` equals `-Xmx`, otherwise the GC
grows and shrinks it (ZGC gives unused memory back after `-XX:ZUncommitDelay`, 300 s). Resident only
once touched, unless the JVM runs with `-XX:+AlwaysPreTouch`.
Parallel and Serial: both survivor spaces are included, while their `MemoryPoolMXBean`s show one.

---

**Metric:** `jvm_memory_heap_max_bytes`, **Source:** `sun.gc.generation.N.maxCapacity` (ZGC, G1,
Shenandoah: the value every generation has, the whole heap; Parallel, Serial: their sum), **Type:**
gauge\
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

**Metric:** `jvm_memory_metaspace_committed_bytes`, **Source:** `sun.gc.metaspace.capacity`,
**Type:** gauge\
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
Pages swapped out are not included. Only pages the process touched count, and a page counts once per
mapping: an Aeron log buffer mapped by the media driver and its client counts in both processes,
and twice in one process when the driver is embedded. Not the same as a container's memory usage:
the cgroup also charges page cache and `/dev/shm` files, and charges a page once.

---

**Metric:** `jvm_resident_memory_anon_bytes`, **Source:** `statm` resident − shared, **Type:**
gauge\
**Usage:**

- `jvm_resident_memory_anon_bytes`: non-heap memory (ZGC)
- `jvm_resident_memory_anon_bytes - jvm_memory_metaspace_committed_bytes`: non-heap memory besides
  metaspace (ZGC)
- `jvm_resident_memory_anon_bytes - jvm_memory_heap_committed_bytes`: non-heap memory, lower bound
  (G1, Parallel, Serial)
- `jvm_resident_memory_anon_bytes - jvm_memory_heap_used_bytes`: non-heap memory, upper bound (G1,
  Parallel, Serial)
-
`jvm_resident_memory_anon_bytes - jvm_memory_heap_committed_bytes - jvm_memory_metaspace_committed_bytes`:
non-heap memory besides metaspace, lower bound (G1, Parallel, Serial)

**Description:** Resident memory not backed by a file: direct buffers and other native
allocations, metaspace, JIT code cache, GC data structures, thread stacks, and the heap of every
collector but ZGC. With ZGC the heap is shared memory, so this is exactly the JVM's non-heap memory.
With the others, anonymous minus heap is all non-heap memory, not only direct buffers, and it is a
range because heap committed but never touched is not resident, unless the JVM runs with
`-XX:+AlwaysPreTouch`.

---

**Metric:** `jvm_resident_memory_shared_bytes`, **Source:** `statm` shared, **Type:** gauge\
**Usage:**

- `jvm_resident_memory_shared_bytes - jvm_memory_heap_committed_bytes`: mapped files and
  libraries (ZGC with `-XX:+AlwaysPreTouch`; without it, a lower bound)
- `jvm_resident_memory_shared_bytes`: mapped files and libraries (G1, Parallel, Serial)

**Description:** Resident memory backed by a file or shared memory: the ZGC heap, mapped files (e.g.
Aeron log buffers in `/dev/shm`), the JVM's libraries, jars and CDS archive. ZGC's heap
counts once it is touched: measured with a pre-touched 1 GiB heap, shared was the heap plus about 20 MiB.

### Dashboards and alerts

Queries for panels and alerts, by what they watch. Each one works per JVM (per series of labels the
scrape adds); aggregate with `sum by`, `max by` as the dashboard needs. GC pause and safepoint
counters reset when the JVM restarts; `rate` and `increase` handle that. ZGC first; the groups after
it show ZGC's form of a query first where collectors differ.

#### ZGC

**Collections per second**

```promql
rate(jvm_gc_pauses_total{gc="ZGC minor collection pauses"}[5m]) / 3
rate(jvm_gc_pauses_total{gc="ZGC major collection pauses"}[5m]) / 5
```

hsperfdata counts ZGC's pauses, not its collections: 3 pauses per minor (young) collection, about 5
per major (young and old). Minor collections follow the allocation rate; a step up after a release
is new allocation on the hot path. Majors come as old data grows, on `System.gc()`, and when the
heap runs short.

**Heap after the last collection**

```promql
jvm_memory_heap_used_bytes / jvm_memory_heap_max_bytes
```

ZGC updates heap used at the end of each collection only, so the value is the live data plus what
was allocated while the collection ran: no `min_over_time` needed to see the live data. What is left
up to the limit is what the application allocates into while ZGC marks and relocates concurrently.

**Allocation stalls** (alert)

```promql
jvm_memory_heap_used_bytes / jvm_memory_heap_max_bytes > 0.9
rate(jvm_gc_pauses_total{gc="ZGC minor collection pauses"}[5m]) / 3
```

When the application allocates faster than ZGC frees memory, the allocating threads wait for the
collection: an allocation stall. It is not a pause and not a safepoint, and hsperfdata does not
count it: measured, 71,081 stalls in the GC log in 10 s, and no hsperfdata entry but the GC counters
changed. What shows is its cause: the heap stays near the limit after each collection, and
collections run back to back (measured: 527 minor collections per second while stalling, against 17
per second allocating as fast, with headroom). Each stall itself is in the GC log
(`Allocation Stall`) and in JFR (`jdk.ZAllocationStall`).

**Stop time**

```promql
rate(jvm_gc_pause_nanoseconds_total{gc=~"ZGC.*"}[5m]) / rate(jvm_gc_pauses_total{gc=~"ZGC.*"}[5m]) / 1e3
(rate(jvm_safepoint_time_nanoseconds_total[5m]) + rate(jvm_safepoint_sync_time_nanoseconds_total[5m])) / 1e9
```

ZGC's pauses take microseconds on average (first line, µs; measured 3 µs), so GC pause time
understates how long the application is stopped. Each pause is one safepoint, and the safepoint time
and the time to reach it are the stop the application sees (second line, share of time): measured
under heavy allocation, 49 ms of GC pause time against 426 ms at safepoints and 740 ms reaching
them.

**Memory** (stacked)

```promql
jvm_memory_heap_committed_bytes
jvm_resident_memory_anon_bytes
jvm_resident_memory_shared_bytes - jvm_memory_heap_committed_bytes
```

Heap, non-heap and mapped files. ZGC keeps the heap in shared memory, so anonymous memory is exactly
the non-heap memory. With `-Xms` equal to `-Xmx` and `-XX:+AlwaysPreTouch` the heap is resident and
fixed from the start (measured, a 1 GiB heap: shared was the heap plus about 20 MiB) and the third line is
exact; without pre-touch it is a lower bound. Without `-Xms` equal to `-Xmx` the heap grows under
load, and ZGC gives unused memory back after `-XX:ZUncommitDelay` (300 s).

#### Memory: where the process memory goes

**Heap limit in effect**

```promql
jvm_memory_heap_max_bytes
```

The heap limit the JVM runs with. Without `-Xmx` it is a quarter of the container's memory limit
(`-XX:MaxRAMPercentage=25`): a 1 GiB container gets a 256 MiB heap and the rest is left to non-heap
memory. A heap flag that did not reach the JVM shows here first. The direct buffer limit
(`-XX:MaxDirectMemorySize`) defaults to about the same value.

**Process memory against the container limit**

```promql
(jvm_resident_memory_anon_bytes + jvm_memory_heap_committed_bytes) / on(pod, container) container_spec_memory_limit_bytes
jvm_resident_memory_anon_bytes / on(pod, container) container_spec_memory_limit_bytes
```

Memory the kernel cannot take back (without swap): with ZGC, non-heap plus the heap, which is shared
memory but not backed by a file (first line, upper bound without pre-touch); with G1, Parallel and
Serial, anonymous memory (second line). It must stay well below the limit, with room left for the
`/dev/shm` files (the join labels depend on the scrape setup; `container_spec_memory_limit_bytes` is
cAdvisor's). The whole resident memory does not compare with the limit: a page mapped twice in the
process (e.g. an Aeron log buffer, by an embedded driver and its client) counts twice in it and once
in the container, and pages of files on disk can be reclaimed. For the out-of-memory alert itself,
use the container's own working set.

**Memory breakdown** (stacked; ZGC: see Memory under ZGC)

```promql
jvm_memory_heap_committed_bytes
jvm_memory_metaspace_committed_bytes
jvm_resident_memory_anon_bytes - jvm_memory_heap_committed_bytes - jvm_memory_metaspace_committed_bytes
jvm_resident_memory_shared_bytes
```

Heap, metaspace, other native memory (direct buffers, thread stacks, JIT code cache, GC structures,
native libraries' allocations), and mapped files. The third line is a lower bound: heap committed
but never touched is not resident, unless the JVM runs with `-XX:+AlwaysPreTouch`. G1, Parallel and
Serial.

**Native memory growth**

```promql
deriv(jvm_resident_memory_anon_bytes[6h])
max_over_time(jvm_resident_memory_anon_bytes[1d])
deriv((jvm_resident_memory_anon_bytes - jvm_memory_heap_committed_bytes)[6h:5m])
max_over_time((jvm_resident_memory_anon_bytes - jvm_memory_heap_committed_bytes)[1d:5m])
```

The first two lines for ZGC, the last two for G1, Parallel and Serial.

Buffers allocated once at start (rings, preallocated message buffers) make a step at start and then
a flat line: a positive slope hours after start is native memory allocated per request or per
connection and not freed. Pooled network buffers grow with connections and in-flight data and are
kept for reuse, so they follow the load peak: the daily peak is the figure to size the container by.
At the direct buffer limit allocations fail, or the JDK runs a full GC first (see full GCs below).

#### Off-heap: Aeron and mapped files

**Aeron log buffers**

```promql
jvm_resident_memory_shared_bytes - jvm_memory_heap_committed_bytes
jvm_resident_memory_shared_bytes
deriv(jvm_resident_memory_shared_bytes[1h])
```

The first line for ZGC, whose heap is in shared memory too (with a pre-touched heap; without, a
lower bound); the second for G1, Parallel and Serial. With the media driver embedded, its files are
in `/dev/shm` and mapped by this process. `cnc.dat` (46 MiB with Aeron 1.53 defaults) is resident
from the start. Each publication and each image is a log buffer of three terms (16 MiB each by
default, 64 MiB for IPC), mapped by the driver and by the client: in this metric it counts twice,
so a stream with 16 MiB terms adds 96 MiB once written through, while `/dev/shm` holds 48 MiB; a
default IPC stream adds 384 MiB, with 192 MiB in `/dev/shm`. Without
pre-touch (`aeron.pre.touch.mapped.memory`) a new stream raises the line one term at a time until
its log wraps, then it is flat. A client of an external driver counts its logs once, and the driver
process once more.

**Aeron streams not released** (alert)

```promql
deriv(min_over_time((jvm_resident_memory_shared_bytes - jvm_memory_heap_committed_bytes)[1h:5m])[1d:1h]) > 0
deriv(min_over_time(jvm_resident_memory_shared_bytes[1h])[1d:1h]) > 0
```

ZGC first, then G1, Parallel and Serial. A closed publication or image is unmapped after its linger
(publication linger 5 s, image liveness 10 s by default), and the line comes back to where it was
before the stream. A low point that rises day after day is streams that are opened and never closed
(a publication per session, a subscription left open), each holding its log in `/dev/shm`.

**Embedded database files**

```promql
jvm_resident_memory_shared_bytes - jvm_memory_heap_committed_bytes
jvm_resident_memory_anon_bytes
```

Mapped files and non-heap memory, ZGC (G1, Parallel, Serial: `jvm_resident_memory_shared_bytes` and
`jvm_resident_memory_anon_bytes - jvm_memory_heap_committed_bytes`). An embedded database that maps
its table files (e.g. a time-series database with column files per partition): a query maps the
partitions it reads, and they stay resident while the database keeps its readers open. Measured:
+593 MiB after one query over 12 million rows in 60 partitions, given back only when the engine
closed. These pages are files on disk, the kernel can drop them under memory pressure: high here is
not a risk the way `/dev/shm` is. The database's own allocations (write buffers, query memory) are
anonymous native memory, outside the direct buffer limit.

#### Heap: data held in memory

**Live data**

```promql
jvm_memory_heap_used_bytes / jvm_memory_heap_max_bytes
min_over_time(jvm_memory_heap_used_bytes[1h]) / jvm_memory_heap_max_bytes
```

With ZGC heap used is the value at the end of the last collection, close to the live data as it is
(first line). With G1, which follows allocation between GCs, the low point of heap used is close to
the data that survives GC (second line; it works for ZGC too). Flat for a service whose heap comes
and goes with requests; rising, then flat, for a service that holds data in memory (caches,
histories, per-key state) until its bounds are reached. Near 1, ZGC stalls allocations (see
Allocation stalls), G1 ends in back-to-back full GCs, and both in `OutOfMemoryError`.

**Live data reaching the limit** (alert)

```promql
predict_linear(min_over_time(jvm_memory_heap_used_bytes[1h])[1d:1h], 7 * 86400) > jvm_memory_heap_max_bytes
```

The trend of the low point crosses the heap limit within a week: in-memory data whose bounds do not
fit the heap, or a leak.

**Heap fixed for the data**

```promql
jvm_memory_heap_committed_bytes / jvm_memory_heap_max_bytes
jvm_resident_memory_shared_bytes - jvm_memory_heap_max_bytes
jvm_resident_memory_anon_bytes - jvm_memory_heap_max_bytes
```

A JVM that holds its data in memory runs with the heap fixed: `-Xms` equal to `-Xmx`, and
`-XX:+AlwaysPreTouch`. Then the ratio is 1 from the start and the whole heap is resident. With ZGC
the second line is then exactly the mapped files (non-heap is anonymous memory anyway); with G1,
Parallel and Serial the third line is exactly the non-heap memory instead of a range. A ratio below
1 is a heap that grows under load: each step is GC work and new resident memory at the worst moment.

**Live data above the marking threshold** (G1)

```promql
rate(jvm_gc_pauses_total{gc="G1 concurrent cycle pauses"}[1h])
min_over_time(jvm_memory_heap_used_bytes[1h]) / jvm_memory_heap_max_bytes
```

G1 starts a concurrent cycle when the heap fills to a threshold (45% at first, then adapted). With
live data above it, cycles run back to back and the mixed collections after them free little; the
next step is full GCs. Frequent cycles while live data stays low point to humongous objects: arrays
of half a region or more (a 256 MiB heap has 1 MiB regions, so from 512 KiB), e.g. buffers that grow
by doubling, which count as occupied at once.

**Young pauses and live data** (G1)

```promql
rate(jvm_gc_pause_nanoseconds_total{gc="G1 young collection pauses"}[1h]) / rate(jvm_gc_pauses_total{gc="G1 young collection pauses"}[1h]) / 1e6
min_over_time(jvm_memory_heap_used_bytes[1h])
```

A young pause copies what survives and scans references from old data into young: with a large
in-memory data set that keeps changing (entries replaced, maps updated), the average young pause
grows with the data. Pauses rising with live data mean the data set is too mutable for its size;
flat pauses with growing data are fine.

**Loading at start**

```promql
rate(jvm_gc_pauses_total[5m])
jvm_memory_heap_used_bytes / jvm_memory_heap_max_bytes
```

A service that first loads its data (a snapshot, a log replayed from its last position) allocates at
full speed until it is live: GC rate and heap use are high in the first minutes, then fall to their
steady level, with the live data at its new low point. Staying high means the load does not catch
up, or the heap is too small for it.

#### GC

**GC overhead**

```promql
sum without (gc) (rate(jvm_gc_pause_nanoseconds_total[5m])) / 1e9
```

Share of time the application was stopped by GC pauses (0.01 = 1%). Concurrent GC work is not in it.
With ZGC it is near 0 even under stalls: see Stop time and Allocation stalls under ZGC.

**GC frequency, against last week**

```promql
rate(jvm_gc_pauses_total[1h])
rate(jvm_gc_pauses_total[1h]) / rate(jvm_gc_pauses_total[1h] offset 1w)
```

Pauses per second, per collector. ZGC's minor and G1's young pauses follow the allocation rate: a
service that keeps its hot path free of allocation (object pools, preallocated buffers) has few, and
a step up after a release is new allocation on that path. The ratio compares with the same hour last
week.

**Average pause**

```promql
rate(jvm_gc_pause_nanoseconds_total[5m]) / rate(jvm_gc_pauses_total[5m]) / 1e6
```

Milliseconds, per collector. Only the average: a single long pause raises it in its interval, the GC
log or JFR has the pause itself.

**Full GCs** (alert; G1, Parallel, Serial)

```promql
increase(jvm_gc_pauses_total{gc=~".*full.*"}[1h]) > 0
```

ZGC has no full GC: its major collection is concurrent, and what stops a G1 JVM for a full GC makes
ZGC run majors and, when the heap is short, stall allocations. A full GC stops everything and marks
and compacts all live data: on a heap holding gigabytes of it, the pause takes seconds. Causes: the
heap is full of live data, a call to `System.gc()`, the direct buffer limit is reached (the JDK runs
a full GC before failing the allocation), a humongous allocation that does not fit.

**Collector in use**

```promql
count by (gc) (jvm_gc_pauses_total)
```

The `gc` label names the collector. Without a collector flag the JVM picks Serial, not G1, when it
sees fewer than 2 CPUs or less than 1792 MB of memory, which in a container means its limits, unless
the JVM runs with `-XX:+AlwaysActAsServerClassMachine`.

#### Stopped time

**Share of time stopped**

```promql
(rate(jvm_safepoint_time_nanoseconds_total[5m]) + rate(jvm_safepoint_sync_time_nanoseconds_total[5m])) / 1e9
```

Every stop of all Java threads (GC pauses and other safepoints), with the time it took them to stop.
For a latency-sensitive service this is latency added to every message in flight at the time.

**Time to safepoint**

```promql
rate(jvm_safepoint_sync_time_nanoseconds_total[5m]) / rate(jvm_safepoints_total[5m]) / 1e6
```

Average milliseconds from requesting a stop until the last thread stopped; well under the pause
itself on a healthy JVM. Grows when a Java thread is not on a CPU: more busy threads than cores
(threads spinning on dedicated cores, a thread layout made for more cores than the container has),
CPU throttling, a busy neighbour on a shared core.

**Stops that are not GC**

```promql
rate(jvm_safepoints_total[5m]) - sum without (gc) (rate(jvm_gc_pauses_total[5m]))
```

Safepoints for something else than GC: thread and heap dumps (over JMX, `jcmd`, `jstack`), class
redefinition by agents. Near 0 when nobody inspects the JVM: each GC pause is one safepoint
(measured, ZGC: 16,810 safepoints for 16,809 pauses; G1: equal).

#### Threads

**Thread count**

```promql
jvm_threads_live
jvm_threads_live - jvm_threads_daemon
```

Live Java threads, and those of them that keep the JVM running. Threads started once (event loops,
dedicated worker threads, schedulers) make a flat line after start.

**Thread leak** (alert)

```promql
jvm_threads_live - min_over_time(jvm_threads_live[1d]) > 50
```

Threads above the daily base. Elastic pools (workers created for blocking work and ended when idle)
add a burst and remove it; a level that keeps rising is threads that never end. The threshold
depends on the pools' maximum.

**Pool saturation**

```promql
jvm_threads_peak - min_over_time(jvm_threads_live[1d])
```

How many threads above the base the JVM has had at once. Close to the maximum of an elastic pool,
its blocking work used all of it, and further tasks waited in its queue.

**Thread churn**

```promql
rate(jvm_threads_started_total[1h])
```

Threads started per second. Elastic pools end idle workers and start new ones under the next burst,
so some churn is normal; a steady rate with a flat thread count is a thread per task instead of a
pool.

#### Metaspace

**Class metadata growth**

```promql
delta(jvm_memory_metaspace_used_bytes[1d])
```

Class loading happens at start; afterwards metaspace stays flat. Growth is classes generated at run
time and not cached (proxies, generated serializers, scripts) and never unloaded. Without
`-XX:MaxMetaspaceSize` it grows until the container runs out of memory.

### Requirements and tests

The target JVM needs nothing added, and must not run with `-XX:-UsePerfData` or
`-XX:+PerfDisableSharedMem`. `hsperfdata` is a file the JVM keeps mapped and writes to: keep `/tmp`
on tmpfs (in a container, a memory-backed volume): on a disk-backed `/tmp` the kernel's writeback
of that file is a known cause of JVM stalls (not measured here), and `-XX:+PerfDisableSharedMem` is
the usual way around it.

The reader process (the one running the agent) needs
`--add-opens java.base/jdk.internal.misc=ALL-UNNAMED` on JDK 25: Agrona reaches
`jdk.internal.misc.Unsafe`, and fails at start with `IllegalAccessError` without it.

The counter names are JDK internals and may change in any JDK release.
`JvmMetricsReaderAgentJdkTest` checks every metric against an oracle that shares no code with the
agent, on the JDK the build runs on, so CI must run on the JDK the target services run on. Its
javadoc lists the oracle and the test of each metric.

### Example

In `metrics-examples` (Linux): start `io.scalecube.metrics.jvm.JvmMetricsSourceRunner` with
`-Xmx64m`, a JVM holding a direct buffer and a mapped `/dev/shm` file; it prints its pid. Then the
reader prints its JVM metrics every 3 seconds:

```
java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp "app.jar:lib/*" \
  io.scalecube.metrics.jvm.JvmMetricsReaderRunner <pid>
```
