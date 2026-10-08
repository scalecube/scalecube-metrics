package io.scalecube.metrics.jvm;

import static org.agrona.IoUtil.mapExistingFile;

import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import io.scalecube.metrics.KeyFlyweight;
import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.agrona.BufferUtil;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent that periodically reads JVM metrics (safepoints, GC, heap, threads) from HotSpot {@code
 * hsperfdata} file of another JVM (see {@link HsPerfData}), and invokes {@link CountersHandler}
 * with them. The observed JVM is not touched: no attach, no code, no safepoints, no threads.
 *
 * <p>Every read interval the file is mapped read-only, parsed and unmapped, nothing is kept between
 * reads. Missing, not yet initialized, or unsupported file results in empty list of counters, so
 * the handler never keeps values of a JVM that has gone. Each value is reported as {@link
 * CounterDescriptor} with metric name as label, and {@code gc} tag in the key for GC metrics. Times
 * are in nanoseconds.
 *
 * <p>The JVM updates the values without a lock, so one read can mix values from just before and
 * just after a GC. Each value is correct on its own, don't relate two values from the same read.
 */
public class JvmMetricsReaderAgent implements Agent {

  private static final Logger LOGGER = LoggerFactory.getLogger(JvmMetricsReaderAgent.class);

  public static final String SAFEPOINTS = "jvm_safepoints_total";
  public static final String SAFEPOINT_TIME = "jvm_safepoint_time_nanoseconds_total";
  public static final String SAFEPOINT_SYNC_TIME = "jvm_safepoint_sync_time_nanoseconds_total";
  public static final String GC_COLLECTIONS = "jvm_gc_collections_total";
  public static final String GC_PAUSE = "jvm_gc_pause_nanoseconds_total";
  public static final String HEAP_USED = "jvm_memory_heap_used_bytes";
  public static final String HEAP_COMMITTED = "jvm_memory_heap_committed_bytes";
  public static final String HEAP_MAX = "jvm_memory_heap_max_bytes";
  public static final String THREADS_LIVE = "jvm_threads_live";
  public static final String THREADS_DAEMON = "jvm_threads_daemon";
  public static final String THREADS_PEAK = "jvm_threads_peak";
  public static final String THREADS_STARTED = "jvm_threads_started_total";
  public static final String GC_TAG = "gc";

  private static final long NANOS_PER_SECOND = 1_000_000_000L;
  private static final Pattern COLLECTOR_INVOCATIONS =
      Pattern.compile("sun\\.gc\\.collector\\.(\\d+)\\.invocations");
  private static final Pattern GENERATION_CAPACITY =
      Pattern.compile("sun\\.gc\\.generation\\.\\d+\\.capacity");
  private static final Pattern GENERATION_MAX_CAPACITY =
      Pattern.compile("sun\\.gc\\.generation\\.\\d+\\.maxCapacity");
  private static final Pattern SPACE_USED =
      Pattern.compile("sun\\.gc\\.generation\\.\\d+\\.space\\.\\d+\\.used");

  private final String roleName;
  private final File hsperfdataFile;
  private final boolean warnIfNotExists;
  private final EpochClock epochClock;
  private final long readInterval;
  private final CountersHandler countersHandler;

  private long nextReadTime;

  /**
   * Constructor.
   *
   * @param roleName roleName
   * @param hsperfdataFile {@code hsperfdata} file of the observed JVM
   * @param warnIfNotExists whether to log warning if {@code hsperfdata} file does not exist
   * @param epochClock epochClock
   * @param readInterval interval at which to read {@code hsperfdata} file
   * @param countersHandler callback handler to process JVM metrics
   */
  public JvmMetricsReaderAgent(
      String roleName,
      File hsperfdataFile,
      boolean warnIfNotExists,
      EpochClock epochClock,
      Duration readInterval,
      CountersHandler countersHandler) {
    this.roleName = roleName;
    this.hsperfdataFile = hsperfdataFile;
    this.warnIfNotExists = warnIfNotExists;
    this.epochClock = epochClock;
    this.readInterval = readInterval.toMillis();
    this.countersHandler = countersHandler;
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  public int doWork() {
    final var now = epochClock.time();
    if (now < nextReadTime) {
      return 0;
    }
    nextReadTime = now + readInterval;

    countersHandler.accept(now, readCounters());
    return 1;
  }

  private List<CounterDescriptor> readCounters() {
    if (!hsperfdataFile.exists()) {
      if (warnIfNotExists) {
        LOGGER.warn("[{}] {} not exists", roleName, hsperfdataFile);
      }
      return List.of();
    }

    final MappedByteBuffer byteBuffer;
    try {
      byteBuffer = mapExistingFile(hsperfdataFile, MapMode.READ_ONLY, "hsperfdata");
    } catch (Exception e) {
      // the JVM exited between exists() and map
      LOGGER.debug("[{}] cannot map {}: {}", roleName, hsperfdataFile, e.toString());
      return List.of();
    }

    try {
      final var hsPerfData = HsPerfData.parse(new UnsafeBuffer(byteBuffer));
      return hsPerfData != null ? toCounters(hsPerfData) : List.of();
    } catch (IllegalArgumentException e) {
      LOGGER.warn("[{}] {} unsupported: {}", roleName, hsperfdataFile, e.getMessage());
      return List.of();
    } finally {
      BufferUtil.free(byteBuffer);
    }
  }

  static List<CounterDescriptor> toCounters(HsPerfData hsPerfData) {
    final var longs = hsPerfData.longs();
    final var frequency = longs.get("sun.os.hrt.frequency");
    final var counters = new ArrayList<CounterDescriptor>();

    add(counters, SAFEPOINTS, null, longs.get("sun.rt.safepoints"));
    add(counters, SAFEPOINT_TIME, null, toNanos(longs.get("sun.rt.safepointTime"), frequency));
    add(
        counters,
        SAFEPOINT_SYNC_TIME,
        null,
        toNanos(longs.get("sun.rt.safepointSyncTime"), frequency));

    addCollectors(counters, hsPerfData, frequency);

    add(counters, HEAP_USED, null, sum(longs, SPACE_USED));
    add(counters, HEAP_COMMITTED, null, sum(longs, GENERATION_CAPACITY));
    add(counters, HEAP_MAX, null, heapMax(longs));

    add(counters, THREADS_LIVE, null, longs.get("java.threads.live"));
    add(counters, THREADS_DAEMON, null, longs.get("java.threads.daemon"));
    add(counters, THREADS_PEAK, null, longs.get("java.threads.livePeak"));
    add(counters, THREADS_STARTED, null, longs.get("java.threads.started"));

    return counters;
  }

  // Collector indices are not contiguous (ZGC has 0 and 2), so take the ones that exist
  private static void addCollectors(
      List<CounterDescriptor> counters, HsPerfData hsPerfData, Long frequency) {
    final var collectors = new TreeMap<Integer, String>();
    for (var name : hsPerfData.longs().keySet()) {
      final var matcher = COLLECTOR_INVOCATIONS.matcher(name);
      if (matcher.matches()) {
        collectors.put(Integer.parseInt(matcher.group(1)), "sun.gc.collector." + matcher.group(1));
      }
    }

    final var longs = hsPerfData.longs();
    collectors.forEach(
        (index, prefix) -> {
          final var gc = hsPerfData.strings().get(prefix + ".name");
          if (gc != null) {
            add(counters, GC_COLLECTIONS, gc, longs.get(prefix + ".invocations"));
            add(counters, GC_PAUSE, gc, toNanos(longs.get(prefix + ".time"), frequency));
          }
        });
  }

  private static Long sum(Map<String, Long> longs, Pattern pattern) {
    Long sum = null;
    for (var entry : longs.entrySet()) {
      if (pattern.matcher(entry.getKey()).matches()) {
        sum = (sum != null ? sum : 0L) + entry.getValue();
      }
    }
    return sum;
  }

  // G1 and ZGC report whole heap as maxCapacity of every generation, Parallel and Serial split it
  private static Long heapMax(Map<String, Long> longs) {
    Long first = null;
    long sum = 0;
    var allEqual = true;
    for (var entry : longs.entrySet()) {
      final var value = entry.getValue();
      if (value > 0 && GENERATION_MAX_CAPACITY.matcher(entry.getKey()).matches()) {
        if (first == null) {
          first = value;
        } else if (value.longValue() != first) {
          allEqual = false;
        }
        sum += value;
      }
    }
    if (first == null) {
      return null;
    }
    return allEqual ? first : sum;
  }

  private static Long toNanos(Long ticks, Long frequency) {
    if (ticks == null || frequency == null || frequency <= 0) {
      return null;
    }
    return ticks / frequency * NANOS_PER_SECOND + ticks % frequency * NANOS_PER_SECOND / frequency;
  }

  private static void add(List<CounterDescriptor> counters, String name, String gc, Long value) {
    if (value == null) {
      return;
    }
    final var keyFlyweight = new KeyFlyweight().wrap(new ExpandableArrayBuffer(), 0);
    if (gc != null) {
      keyFlyweight.tagsCount(1).stringValue(GC_TAG, gc);
    } else {
      keyFlyweight.tagsCount(0);
    }
    counters.add(new CounterDescriptor(counters.size(), 0, value, keyFlyweight.buffer(), name));
  }
}
