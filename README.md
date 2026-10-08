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
`hsperfdata` file (`/tmp/hsperfdata_<user>/<pid>`), which every HotSpot JVM writes by default. The
observed JVM is not touched: no attach, no code injected, no safepoints, no threads. The agent reads
the one file it is given; finding that file (e.g. through `/proc/<pid>/root`) is up to the caller.

Every read interval the file is mapped read-only, parsed and unmapped. Values go to a
`CountersHandler` as `CounterDescriptor`s (metric name as label, `gc` tag for GC metrics), so
`CountersPrometheusAdapter` exposes them as is. A missing, not yet initialised, or unsupported file
yields an empty list, so the handler never keeps values of a JVM that has gone.

| Metric                                      | From `hsperfdata`                                       |
|---------------------------------------------|---------------------------------------------------------|
| `jvm_safepoints_total`                      | `sun.rt.safepoints`                                     |
| `jvm_safepoint_time_nanoseconds_total`      | `sun.rt.safepointTime`                                  |
| `jvm_safepoint_sync_time_nanoseconds_total` | `sun.rt.safepointSyncTime`                              |
| `jvm_gc_collections_total{gc}`              | `sun.gc.collector.N.invocations`, `gc` = `.name`        |
| `jvm_gc_pause_nanoseconds_total{gc}`        | `sun.gc.collector.N.time`                               |
| `jvm_memory_heap_used_bytes`                | sum of `sun.gc.generation.N.space.M.used`               |
| `jvm_memory_heap_committed_bytes`           | sum of `sun.gc.generation.N.capacity`                   |
| `jvm_memory_heap_max_bytes`                 | `sun.gc.generation.N.maxCapacity`: equal → it, else sum |
| `jvm_threads_live`                          | `java.threads.live`                                     |
| `jvm_threads_daemon`                        | `java.threads.daemon`                                   |
| `jvm_threads_peak`                          | `java.threads.livePeak`                                 |
| `jvm_threads_started_total`                 | `java.threads.started`                                  |

Times are converted from ticks with `sun.os.hrt.frequency`. Shenandoah's collector time includes
concurrent cycles, for the other collectors it is pause time. The JVM updates values without a lock,
so one read may mix values from just before and just after a GC: each value is right on its own,
don't alert on a relation between two values of the same read.

The counter names are JDK internals and may change in any JDK release.
`JvmMetricsReaderAgentJdkTest` checks every metric against a child JVM per collector (G1, ZGC,
Parallel) on the JDK the build runs on, so CI must run on the JDK the observed services run on.
The observed JVM must not run with `-XX:-UsePerfData` or `-XX:+PerfDisableSharedMem`.
