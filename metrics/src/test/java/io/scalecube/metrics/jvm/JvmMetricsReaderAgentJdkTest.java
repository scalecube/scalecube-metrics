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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Guard against JDK changes: hsperfdata counter names are JDK internals and may change in any JDK
 * release. Runs on the JDK the build runs on, so CI must use the JDK the services run on. Starts a
 * child JVM per collector, with and without heap pre-touch, reads its hsperfdata and statm files and
 * compares with the child's own values; off-heap allocations and mapped files must show up.
 */
class JvmMetricsReaderAgentJdkTest {

  private static final long MAX_HEAP = 256L * 1024 * 1024;
  private static final long OFF_HEAP_MIN_GROWTH = ChildJvm.OFF_HEAP_SIZE * 7L / 8;
  private static final Duration READ_INTERVAL = Duration.ofSeconds(1);
  private static final Set<String> SCALAR_METRICS =
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
          METASPACE_COMMITTED,
          RESIDENT,
          RESIDENT_ANON,
          RESIDENT_SHARED);

  private final CachedEpochClock epochClock = new CachedEpochClock();
  private List<CounterDescriptor> lastRead;

  @ParameterizedTest
  @CsvSource({
    "-XX:+UseG1GC, -XX:-AlwaysPreTouch",
    "-XX:+UseG1GC, -XX:+AlwaysPreTouch",
    "-XX:+UseZGC, -XX:-AlwaysPreTouch",
    "-XX:+UseZGC, -XX:+AlwaysPreTouch",
    "-XX:+UseParallelGC, -XX:-AlwaysPreTouch",
    "-XX:+UseParallelGC, -XX:+AlwaysPreTouch"
  })
  void readsAllMetricsOfRunningJvm(String gcFlag, String preTouchFlag) throws Exception {
    final var process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                gcFlag,
                preTouchFlag,
                "-Xmx" + MAX_HEAP,
                "-cp",
                Path.of(ChildJvm.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                    .toString(),
                ChildJvm.class.getName())
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start();
    try {
      final var in = new BufferedReader(new InputStreamReader(process.getInputStream()));
      final var out = new PrintStream(process.getOutputStream(), true);
      assertEquals("ready", in.readLine());

      final var file =
          new File(
              "/tmp/hsperfdata_" + System.getProperty("user.name"), String.valueOf(process.pid()));
      final var statm = new File("/proc/" + process.pid() + "/statm");
      final var agent =
          new JvmMetricsReaderAgent(
              "JvmMetricsReaderAgent",
              file,
              statm,
              true,
              epochClock,
              READ_INTERVAL,
              countersHandler());
      agent.onStart();

      final var before = readUntilPresent(agent);
      assertAllMetricsPresent(before);
      assertEquals(
          before.get(RESIDENT), before.get(RESIDENT_ANON) + before.get(RESIDENT_SHARED), "rss");
      if (preTouchFlag.equals("-XX:+AlwaysPreTouch")) {
        // whole committed heap is resident: in anon, except ZGC, whose heap is shared memory
        final var heapIn = gcFlag.equals("-XX:+UseZGC") ? RESIDENT_SHARED : RESIDENT_ANON;
        assertTrue(before.get(heapIn) >= before.get(HEAP_COMMITTED), heapIn + ": " + before);
      }

      // before any GC: G1 shrinks and uncommits the heap after a full GC, which moves anon
      out.println("direct");
      assertEquals("ok", in.readLine());
      final var afterDirect = read(agent);
      assertTrue(
          afterDirect.get(RESIDENT_ANON) - before.get(RESIDENT_ANON) >= OFF_HEAP_MIN_GROWTH,
          "direct buffer not in anon: " + before + " -> " + afterDirect);

      out.println("mmap");
      assertEquals("ok", in.readLine());
      final var afterMmap = read(agent);
      assertTrue(
          afterMmap.get(RESIDENT_SHARED) - afterDirect.get(RESIDENT_SHARED) >= OFF_HEAP_MIN_GROWTH,
          "mapped file not in shared: " + afterDirect + " -> " + afterMmap);

      out.println("gc");
      assertEquals("ok", in.readLine());
      out.println("stats");
      final var stats =
          Arrays.stream(in.readLine().split(" ")).mapToLong(Long::parseLong).toArray();
      final var after = read(agent);
      assertAllMetricsPresent(after);

      assertTrue(sum(after, GC_COLLECTIONS) > sum(before, GC_COLLECTIONS), "gc collections");
      assertTrue(sum(after, GC_PAUSE) > sum(before, GC_PAUSE), "gc pause");
      assertTrue(after.get(SAFEPOINTS) > before.get(SAFEPOINTS), "safepoints");
      assertTrue(after.get(SAFEPOINT_TIME) > before.get(SAFEPOINT_TIME), "safepoint time");

      assertEquals(MAX_HEAP, after.get(HEAP_MAX));
      assertWithinRatio(stats[0], after.get(HEAP_COMMITTED), 0.2, "heap committed");
      assertTrue(after.get(HEAP_USED) > 0, "heap used");
      assertTrue(after.get(HEAP_USED) <= MAX_HEAP, "heap used");

      assertWithin(stats[2], after.get(THREADS_LIVE), 2, "threads live");
      assertWithin(stats[3], after.get(THREADS_DAEMON), 2, "threads daemon");
      assertWithin(stats[4], after.get(THREADS_PEAK), 2, "threads peak");
      assertWithin(stats[5], after.get(THREADS_STARTED), 2, "threads started");

      assertTrue(after.get(METASPACE_USED) > 0, "metaspace used");
      assertTrue(after.get(METASPACE_COMMITTED) >= after.get(METASPACE_USED), "metaspace");
    } finally {
      process.destroyForcibly().waitFor();
    }
  }

  private CountersHandler countersHandler() {
    return new CountersHandler() {
      @Override
      public void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
        lastRead = counterDescriptors;
      }
    };
  }

  private Map<String, Long> read(JvmMetricsReaderAgent agent) {
    epochClock.advance(READ_INTERVAL.toMillis());
    agent.doWork();
    return JvmMetricsReaderAgentTest.toMap(lastRead);
  }

  private Map<String, Long> readUntilPresent(JvmMetricsReaderAgent agent) throws Exception {
    final var deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    var values = read(agent);
    while (values.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(50);
      values = read(agent);
    }
    return values;
  }

  private static void assertAllMetricsPresent(Map<String, Long> values) {
    assertTrue(values.keySet().containsAll(SCALAR_METRICS), "missing: " + values.keySet());

    final var collections = collectors(values, GC_COLLECTIONS);
    assertFalse(collections.isEmpty(), "no collectors: " + values.keySet());
    assertEquals(collections, collectors(values, GC_PAUSE));
    assertEquals(SCALAR_METRICS.size() + 2 * collections.size(), values.size());
  }

  private static Set<String> collectors(Map<String, Long> values, String metric) {
    return values.keySet().stream()
        .filter(name -> name.startsWith(metric + "{"))
        .map(name -> name.substring(metric.length()))
        .collect(Collectors.toSet());
  }

  private static long sum(Map<String, Long> values, String metric) {
    return values.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(metric + "{"))
        .mapToLong(Map.Entry::getValue)
        .sum();
  }

  private static void assertWithinRatio(long expected, long actual, double ratio, String message) {
    final var delta = (long) (expected * ratio);
    assertTrue(
        Math.abs(expected - actual) <= delta, message + ": expected " + expected + ", " + actual);
  }

  private static void assertWithin(long expected, long actual, long delta, String message) {
    assertTrue(
        Math.abs(expected - actual) <= delta, message + ": expected " + expected + ", " + actual);
  }
}
