package io.scalecube.metrics.jvm;

import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.GC_COLLECTIONS;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.GC_PAUSE;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.HEAP_COMMITTED;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.HEAP_MAX;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.HEAP_USED;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.METASPACE_COMMITTED;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.METASPACE_USED;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.RESIDENT;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.RESIDENT_ANON;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.RESIDENT_SHARED;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.SAFEPOINTS;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.SAFEPOINT_SYNC_TIME;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.SAFEPOINT_TIME;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.THREADS_DAEMON;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.THREADS_LIVE;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.THREADS_PEAK;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.THREADS_STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigInteger;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Evidence for every metric of {@link JvmMetricsReaderAgent}, against a real target JVM ({@link
 * TargetJvm}) per collector (G1, ZGC, Parallel, Serial), with and without {@code
 * -XX:+AlwaysPreTouch}. hsperfdata counter names are JDK internals and may change in any JDK
 * release, so this runs on the JDK the build runs on, and CI must use the JDK the services run on.
 *
 * <p>Each metric is checked against an oracle that shares no code with the agent:
 *
 * <ul>
 *   <li>all hsperfdata entries: the JDK's own reader ({@code sun.jvmstat}, behind {@code jstat}) on
 *       the same bytes, equal ({@link #readsEveryEntryAsJdkReaderDoes()});
 *   <li>all metrics from hsperfdata: the documented mapping over the JDK reader's entries, equal
 *       ({@link #derivesEveryMetricFromCountersAsDocumented()});
 *   <li>safepoints: {@code HotspotRuntimeMBean}, count equal, times within 1 ms ({@link
 *       #safepointsMatchJvmOwnCounters()});
 *   <li>GC collections and pauses: {@code GarbageCollectorMXBean}s after every collector has run,
 *       count equal, time within 1 ms per pause ({@link
 *       #gcCollectionsAndPausesMatchJvmOwnCollectors()});
 *   <li>threads: {@code ThreadMXBean} after starting threads, equal ({@link
 *       #threadsMatchThreadMxBean()});
 *   <li>heap committed: heap {@code MemoryPoolMXBean}s, equal ({@link
 *       #heapCommittedMatchesJvmOwnValue()});
 *   <li>heap max: {@code -Xmx}, equal ({@link #heapMaxIsXmx()});
 *   <li>heap used: 64 MB made live, then garbage, followed at GC ({@link
 *       #heapUsedFollowsLiveData()});
 *   <li>metaspace: classes loaded, as measured by the target JVM, within 1 MB at GC ({@link
 *       #metaspaceFollowsClassLoading()});
 *   <li>resident: {@code /proc/<pid>/status} {@code VmRSS}, {@code RssAnon}, {@code RssFile +
 *       RssShmem}, equal ({@link #residentMemoryMatchesKernelAccounting()}). The agent reads {@code
 *       statm}, so it is not checked against its own input: {@code status} is a separate file over
 *       the same kernel page counters, in kB, with anonymous and shared broken out, so it checks
 *       parsing, page size, and anonymous = resident - shared;
 *   <li>anonymous and shared: a direct buffer, a mapped {@code /dev/shm} file and the pre-touched
 *       heap show up where documented ({@link #directBufferIsResidentAnonymousMemory()}, {@link
 *       #mappedFileIsResidentSharedMemory()}, {@link #preTouchedHeapIsResident()}).
 * </ul>
 *
 * <p>Live values change between two reads, so a comparison holds when it holds for reads both
 * before and after the oracle; it is retried until then, and fails if it never does within {@link
 * #AWAIT_TIMEOUT}. No assertion relies on timing.
 *
 * <p>Shenandoah is not covered: its collectors ({@code partial}, {@code full}) do not map one to
 * one to its {@code GarbageCollectorMXBean}s.
 */
@ParameterizedClass(name = "{0} {1}")
@CsvSource({
  "-XX:+UseG1GC, -XX:-AlwaysPreTouch",
  "-XX:+UseG1GC, -XX:+AlwaysPreTouch",
  "-XX:+UseZGC, -XX:-AlwaysPreTouch",
  "-XX:+UseZGC, -XX:+AlwaysPreTouch",
  "-XX:+UseParallelGC, -XX:-AlwaysPreTouch",
  "-XX:+UseParallelGC, -XX:+AlwaysPreTouch",
  "-XX:+UseSerialGC, -XX:-AlwaysPreTouch",
  "-XX:+UseSerialGC, -XX:+AlwaysPreTouch"
})
@TestInstance(Lifecycle.PER_CLASS)
class JvmMetricsReaderAgentJdkTest {

  // Xms = Xmx: heap is never uncommitted, so resident memory only grows by what a test does
  private static final long HEAP = 256L * 1024 * 1024;
  private static final long MB = 1024 * 1024;
  private static final long NANOS_PER_MILLI = 1_000_000L;
  private static final Duration READ_INTERVAL = Duration.ofSeconds(1);
  private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);
  private static final Set<String> METRICS_FROM_HSPERFDATA =
      Set.of(
          SAFEPOINTS,
          SAFEPOINT_TIME,
          SAFEPOINT_SYNC_TIME,
          HEAP_USED,
          HEAP_COMMITTED,
          HEAP_MAX,
          THREADS_LIVE,
          THREADS_DAEMON,
          THREADS_PEAK,
          THREADS_STARTED,
          METASPACE_USED,
          METASPACE_COMMITTED);
  private static final Set<String> METRICS_FROM_STATM =
      Set.of(RESIDENT, RESIDENT_ANON, RESIDENT_SHARED);

  // not an entry: the JDK reader makes it from the file prologue's modification time stamp
  private static final String JDK_READER_PROLOGUE_TIMESTAMP = "sun.perfdata.timestamp";

  // hsperfdata collector name -> GarbageCollectorMXBean of the same pauses
  private static final Map<String, String> COLLECTOR_MX_BEANS =
      Map.of(
          "G1 young collection pauses", "G1 Young Generation",
          "G1 full collection pauses", "G1 Old Generation",
          "G1 concurrent cycle pauses", "G1 Concurrent GC",
          "ZGC minor collection pauses", "ZGC Minor Pauses",
          "ZGC major collection pauses", "ZGC Major Pauses",
          "Parallel young collection pauses", "PS Scavenge",
          "Parallel full collection pauses", "PS MarkSweep",
          "Serial young collection pauses", "Copy",
          "Serial full collection pauses", "MarkSweepCompact");

  private static final Pattern COLLECTOR_NAME =
      Pattern.compile("sun\\.gc\\.collector\\.\\d+\\.name");
  private static final Pattern SPACE_USED =
      Pattern.compile("sun\\.gc\\.generation\\.\\d+\\.space\\.\\d+\\.used");
  private static final Pattern GENERATION_CAPACITY =
      Pattern.compile("sun\\.gc\\.generation\\.\\d+\\.capacity");

  @Parameter(0)
  private String gcFlag;

  @Parameter(1)
  private String preTouchFlag;

  @TempDir private Path tempDir;

  private final CachedEpochClock epochClock = new CachedEpochClock();
  private Process process;
  private BufferedReader in;
  private PrintStream out;
  private JvmMetricsReaderAgent agent;
  private List<CounterDescriptor> lastRead;

  @BeforeParameterizedClassInvocation
  void startTargetJvm() throws Exception {
    process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                gcFlag,
                preTouchFlag,
                "-Xms" + HEAP,
                "-Xmx" + HEAP,
                "--add-exports",
                "jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED",
                "--add-exports",
                "java.management/sun.management=ALL-UNNAMED",
                "-cp",
                Path.of(TargetJvm.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                    .toString(),
                TargetJvm.class.getName())
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start();
    in = new BufferedReader(new InputStreamReader(process.getInputStream()));
    out = new PrintStream(process.getOutputStream(), true);
    assertEquals("ready", in.readLine());

    agent =
        newAgent(
            new File(
                "/tmp/hsperfdata_" + System.getProperty("user.name"),
                String.valueOf(process.pid())),
            new File("/proc/" + process.pid() + "/statm"));
    await("all metrics", values -> !values.isEmpty());
  }

  @AfterParameterizedClassInvocation
  void stopTargetJvm() throws Exception {
    if (process != null) {
      out.close(); // target JVM exits on end of input, and removes its hsperfdata file
      if (!process.waitFor(10, TimeUnit.SECONDS)) {
        process.destroyForcibly().waitFor();
      }
    }
  }

  // Parser: every entry the JDK's own reader sees, with the same value, from the same bytes
  @Test
  void readsEveryEntryAsJdkReaderDoes() throws Exception {
    final var snapshot = snapshot();
    final var jdk = jdkEntries(snapshot);
    final var ours = parse(snapshot);

    final var expectedLongs = new HashMap<String, Long>();
    final var expectedStrings = new HashMap<String, String>();
    jdk.forEach((name, value) -> {
      if (name.equals(JDK_READER_PROLOGUE_TIMESTAMP)) {
        return;
      }
      if (value instanceof Long longValue) {
        expectedLongs.put(name, longValue);
      } else {
        expectedStrings.put(name, (String) value);
      }
    });
    assertTrue(expectedLongs.size() > 100, "jdk reader longs: " + expectedLongs.size());
    assertTrue(expectedStrings.size() > 10, "jdk reader strings: " + expectedStrings.size());
    assertEquals(Map.of(), mismatches(expectedLongs, ours.longs()), "longs: jdk reader vs ours");
    assertEquals(
        Map.of(), mismatches(expectedStrings, ours.strings()), "strings: jdk reader vs ours");
  }

  // name -> [jdk reader value, our value], for entries that differ or exist on one side only
  private static <T> Map<String, List<T>> mismatches(Map<String, T> jdk, Map<String, T> ours) {
    final var mismatches = new TreeMap<String, List<T>>();
    final var names = new HashSet<>(jdk.keySet());
    names.addAll(ours.keySet());
    for (var name : names) {
      final var jdkValue = jdk.get(name);
      final var ourValue = ours.get(name);
      if (!Objects.equals(jdkValue, ourValue)) {
        mismatches.put(name, Arrays.asList(jdkValue, ourValue));
      }
    }
    return mismatches;
  }

  // Mapping: every metric is the documented function of the counters the JDK's own reader sees
  @Test
  void derivesEveryMetricFromCountersAsDocumented() throws Exception {
    final var snapshot = snapshot();
    final var jdk = jdkEntries(snapshot);
    final var frequency = (Long) jdk.get("sun.os.hrt.frequency");

    final var expected = new LinkedHashMap<String, Long>();
    expected.put(SAFEPOINTS, (Long) jdk.get("sun.rt.safepoints"));
    expected.put(SAFEPOINT_TIME, nanos((Long) jdk.get("sun.rt.safepointTime"), frequency));
    expected.put(SAFEPOINT_SYNC_TIME, nanos((Long) jdk.get("sun.rt.safepointSyncTime"), frequency));
    jdk.forEach((name, value) -> {
      if (COLLECTOR_NAME.matcher(name).matches()) {
        final var prefix = name.substring(0, name.length() - ".name".length());
        final var gc = "{gc=" + value + "}";
        expected.put(GC_COLLECTIONS + gc, (Long) jdk.get(prefix + ".invocations"));
        expected.put(GC_PAUSE + gc, nanos((Long) jdk.get(prefix + ".time"), frequency));
      }
    });
    expected.put(HEAP_USED, sum(jdk, SPACE_USED));
    expected.put(HEAP_COMMITTED, sum(jdk, GENERATION_CAPACITY));
    expected.put(HEAP_MAX, HEAP);
    expected.put(THREADS_LIVE, (Long) jdk.get("java.threads.live"));
    expected.put(THREADS_DAEMON, (Long) jdk.get("java.threads.daemon"));
    expected.put(THREADS_PEAK, (Long) jdk.get("java.threads.livePeak"));
    expected.put(THREADS_STARTED, (Long) jdk.get("java.threads.started"));
    expected.put(METASPACE_USED, (Long) jdk.get("sun.gc.metaspace.used"));
    expected.put(METASPACE_COMMITTED, (Long) jdk.get("sun.gc.metaspace.capacity"));

    expected.forEach((metric, value) -> assertNotNull(value, "no counter for " + metric));
    assertTrue(expected.keySet().containsAll(METRICS_FROM_HSPERFDATA));
    assertTrue(expected.keySet().stream().anyMatch(name -> name.startsWith(GC_COLLECTIONS)));

    final var snapshotAgent = newAgent(snapshot.toFile(), null);
    assertEquals(expected, read(snapshotAgent));
  }

  @Test
  void safepointsMatchJvmOwnCounters() throws Exception {
    call("gc");
    await(
        "safepoints",
        this::stats,
        (values, stats) ->
            values.get(SAFEPOINTS) > 0
                && values.get(SAFEPOINTS).equals(stats.get("safepoints"))
                // the JVM converts to millis in floating point: allow 1 ms of rounding
                && withinMillis(values.get(SAFEPOINT_TIME), stats.get("safepointTimeMs"), 1)
                && withinMillis(
                    values.get(SAFEPOINT_SYNC_TIME), stats.get("safepointSyncTimeMs"), 1));
  }

  // Counts are equal. Times come from two timers around the same pause: the hsperfdata one
  // encloses the MXBean one by a few microseconds per pause, measured 1-4 us; 1 ms per pause is
  // allowed, a wrong unit is off by 1000x
  @Test
  void gcCollectionsAndPausesMatchJvmOwnCollectors() throws Exception {
    call("churn");
    final var collectors =
        await(
            "gc",
            this::stats,
            (values, stats) ->
                collectors(values).stream()
                    .allMatch(gc -> {
                      final var mxBean = COLLECTOR_MX_BEANS.get(gc);
                      if (mxBean == null) {
                        return false;
                      }
                      final var count = values.get(GC_COLLECTIONS + "{gc=" + gc + "}");
                      final var pause = values.get(GC_PAUSE + "{gc=" + gc + "}");
                      final var mxCount = stats.get("gc." + mxBean + ".count");
                      final var mxTimeMs = stats.get("gc." + mxBean + ".timeMs");
                      return count > 0
                          && count.equals(mxCount)
                          && pause >= (mxTimeMs - 1) * NANOS_PER_MILLI
                          && pause <= (mxTimeMs + 1 + count) * NANOS_PER_MILLI;
                    }));
    final var names = collectors(collectors);
    assertTrue(COLLECTOR_MX_BEANS.keySet().containsAll(names), "unknown collector: " + names);
    assertEquals(gcFlag.equals("-XX:+UseG1GC") ? 3 : 2, names.size(), names.toString());
  }

  @Test
  void threadsMatchThreadMxBean() throws Exception {
    call("threads 10 4");
    await(
        "threads",
        this::stats,
        (values, stats) ->
            values.get(THREADS_LIVE) >= 10
                && values.get(THREADS_LIVE).equals(stats.get("threadsLive"))
                && values.get(THREADS_DAEMON).equals(stats.get("threadsDaemon"))
                && values.get(THREADS_PEAK).equals(stats.get("threadsPeak"))
                && values.get(THREADS_STARTED).equals(stats.get("threadsStarted")));
  }

  @Test
  void heapCommittedMatchesJvmOwnValue() throws Exception {
    await(
        "heap committed",
        this::stats,
        (values, stats) -> values.get(HEAP_COMMITTED).equals(stats.get("heapCommitted")));
  }

  @Test
  void heapMaxIsXmx() throws Exception {
    await("heap max", values -> values.get(HEAP_MAX) == HEAP);
  }

  // hsperfdata heap used is refreshed at GC (G1: also per eden region), so it is checked at GC
  @Test
  void heapUsedFollowsLiveData() throws Exception {
    final var liveMb = 64;
    call("retain " + liveMb);
    call("gc");
    final var withLive =
        await("heap used with live data", values -> values.get(HEAP_USED) >= liveMb * MB)
            .get(HEAP_USED);
    call("release");
    call("gc");
    await(
        "heap used after release",
        values ->
            values.get(HEAP_USED) <= withLive - liveMb * MB * 3 / 4
                && values.get(HEAP_USED) <= values.get(HEAP_COMMITTED));
  }

  // hsperfdata metaspace is refreshed at GC only. The target JVM measures the growth itself
  // (MemoryPoolMXBean); other metaspace use during the test (JIT profiles) is far below 1 MB
  @Test
  void metaspaceFollowsClassLoading() throws Exception {
    call("gc");
    final var before = read(agent).get(METASPACE_USED);
    final var loaded = Long.parseLong(call("classes 2000"));
    assertTrue(loaded >= 4 * MB, "classes took " + loaded);
    call("gc");
    await(
        "metaspace",
        values ->
            Math.abs(values.get(METASPACE_USED) - before - loaded) <= MB
                && values.get(METASPACE_COMMITTED) >= values.get(METASPACE_USED));
  }

  @Test
  void residentMemoryMatchesKernelAccounting() throws Exception {
    final var status = new File("/proc/" + process.pid() + "/status");
    await(
        "resident memory",
        () -> procStatus(status),
        (values, kernel) ->
            values.get(RESIDENT).equals(kernel.get("VmRSS"))
                && values.get(RESIDENT_ANON).equals(kernel.get("RssAnon"))
                && values
                    .get(RESIDENT_SHARED)
                    .equals(kernel.get("RssFile") + kernel.get("RssShmem"))
                && values.get(RESIDENT) == values.get(RESIDENT_ANON) + values.get(RESIDENT_SHARED));
  }

  @Test
  void directBufferIsResidentAnonymousMemory() throws Exception {
    final var before = read(agent).get(RESIDENT_ANON);
    call("direct");
    await(
        "direct buffer in anon",
        values -> values.get(RESIDENT_ANON) - before >= TargetJvm.OFF_HEAP_SIZE * 3L / 4);
  }

  @Test
  void mappedFileIsResidentSharedMemory() throws Exception {
    final var before = read(agent).get(RESIDENT_SHARED);
    call("mmap");
    await(
        "mapped file in shared",
        values -> values.get(RESIDENT_SHARED) - before >= TargetJvm.OFF_HEAP_SIZE * 3L / 4);
  }

  // G1/Parallel/Serial keep the heap in anonymous memory, ZGC in shared memory
  @Test
  void preTouchedHeapIsResident() throws Exception {
    assumeTrue(preTouchFlag.equals("-XX:+AlwaysPreTouch"), "heap is touched only with pre-touch");
    final var heapIn = gcFlag.equals("-XX:+UseZGC") ? RESIDENT_SHARED : RESIDENT_ANON;
    await("pre-touched heap in " + heapIn, values -> values.get(heapIn) >= HEAP);
  }

  private String call(String command) throws IOException {
    out.println(command);
    final var reply = in.readLine();
    assertNotNull(reply, "target JVM exited on " + command);
    assertFalse(reply.startsWith("unknown command"), reply);
    return reply;
  }

  private Map<String, Long> stats() {
    try {
      final var stats = new HashMap<String, Long>();
      for (var pair : call("stats").split("\t")) {
        final var separator = pair.lastIndexOf('=');
        stats.put(pair.substring(0, separator), Long.parseLong(pair.substring(separator + 1)));
      }
      return stats;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Path snapshot() throws IOException {
    final var snapshot = tempDir.resolve("hsperfdata-" + System.nanoTime());
    call("snapshot " + snapshot);
    return snapshot;
  }

  // entries of the snapshot as the JDK's own reader saw them, see TargetJvm
  private static Map<String, Object> jdkEntries(Path snapshot) throws IOException {
    final var properties = new Properties();
    try (var input = new FileInputStream(snapshot + ".jdk")) {
      properties.load(input);
    }
    final var entries = new HashMap<String, Object>();
    for (var name : properties.stringPropertyNames()) {
      final var value = properties.getProperty(name);
      entries.put(
          name, value.charAt(0) == 'J' ? Long.valueOf(value.substring(1)) : value.substring(1));
    }
    return entries;
  }

  private static HsPerfData parse(Path file) {
    final var buffer = IoUtil.mapExistingFile(file.toFile(), MapMode.READ_ONLY, "hsperfdata");
    try {
      return HsPerfData.parse(new UnsafeBuffer(buffer));
    } finally {
      BufferUtil.free(buffer);
    }
  }

  // ticks to nanos, exact
  private static Long nanos(Long ticks, Long frequency) {
    return BigInteger.valueOf(ticks)
        .multiply(BigInteger.valueOf(1_000_000_000L))
        .divide(BigInteger.valueOf(frequency))
        .longValueExact();
  }

  private static Long sum(Map<String, Object> entries, Pattern pattern) {
    return entries.entrySet().stream()
        .filter(entry -> pattern.matcher(entry.getKey()).matches())
        .mapToLong(entry -> (Long) entry.getValue())
        .sum();
  }

  private static boolean withinMillis(long nanos, long millis, long delta) {
    return Math.abs(nanos / NANOS_PER_MILLI - millis) <= delta;
  }

  private static Set<String> collectors(Map<String, Long> values) {
    return values.keySet().stream()
        .filter(name -> name.startsWith(GC_COLLECTIONS + "{gc="))
        .map(name -> name.substring((GC_COLLECTIONS + "{gc=").length(), name.length() - 1))
        .collect(Collectors.toSet());
  }

  // /proc/<pid>/status memory lines, kB -> bytes
  private static Map<String, Long> procStatus(File status) {
    try {
      final var values = new HashMap<String, Long>();
      for (var line : Files.readAllLines(status.toPath())) {
        if (line.endsWith(" kB")) {
          final var separator = line.indexOf(':');
          final var kb = line.substring(separator + 1, line.length() - " kB".length()).trim();
          values.put(line.substring(0, separator), Long.parseLong(kb) * 1024);
        }
      }
      return values;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, Long> await(String what, Predicate<Map<String, Long>> ok)
      throws InterruptedException {
    return await(what, Map::<String, Long>of, (values, ignored) -> ok.test(values));
  }

  // Holds when it holds for agent reads both before and after the oracle, retried until then
  private Map<String, Long> await(
      String what,
      Supplier<Map<String, Long>> oracle,
      BiPredicate<Map<String, Long>, Map<String, Long>> matches)
      throws InterruptedException {
    final var deadline = System.nanoTime() + AWAIT_TIMEOUT.toNanos();
    Map<String, Long> before;
    Map<String, Long> expected;
    Map<String, Long> after;
    do {
      before = read(agent);
      expected = oracle.get();
      after = read(agent);
      if (hasAllMetrics(before)
          && hasAllMetrics(after)
          && matches.test(before, expected)
          && matches.test(after, expected)) {
        return after;
      }
      Thread.sleep(100);
    } while (System.nanoTime() < deadline);
    fail(what + " never matched within " + AWAIT_TIMEOUT + ": " + after + " vs " + expected);
    return null;
  }

  private static boolean hasAllMetrics(Map<String, Long> values) {
    return values.keySet().containsAll(METRICS_FROM_HSPERFDATA)
        && values.keySet().containsAll(METRICS_FROM_STATM);
  }

  private JvmMetricsReaderAgent newAgent(File hsperfdata, File statm) {
    final var newAgent =
        new JvmMetricsReaderAgent(
            "JvmMetricsReaderAgent",
            hsperfdata,
            statm,
            true,
            epochClock,
            READ_INTERVAL,
            new CountersHandler() {
              @Override
              public void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
                lastRead = counterDescriptors;
              }
            });
    newAgent.onStart();
    return newAgent;
  }

  private Map<String, Long> read(JvmMetricsReaderAgent agent) {
    epochClock.advance(READ_INTERVAL.toMillis());
    agent.doWork();
    return JvmMetricsReaderAgentTest.toMap(lastRead);
  }
}
