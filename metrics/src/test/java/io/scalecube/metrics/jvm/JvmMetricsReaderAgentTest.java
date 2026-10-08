package io.scalecube.metrics.jvm;

import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.GC_COLLECTIONS;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.GC_PAUSE;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.GC_TAG;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.HEAP_COMMITTED;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.HEAP_MAX;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.HEAP_USED;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.SAFEPOINTS;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.SAFEPOINT_SYNC_TIME;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.SAFEPOINT_TIME;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.THREADS_DAEMON;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.THREADS_LIVE;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.THREADS_PEAK;
import static io.scalecube.metrics.jvm.JvmMetricsReaderAgent.THREADS_STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import io.scalecube.metrics.KeyCodec;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JvmMetricsReaderAgentTest {

  private static final Duration READ_INTERVAL = Duration.ofSeconds(3);

  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final List<List<CounterDescriptor>> reads = new ArrayList<>();
  private final CountersHandler countersHandler =
      new CountersHandler() {
        @Override
        public void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
          reads.add(counterDescriptors);
        }
      };

  @TempDir private File tempDir;

  @Test
  void mapsAllMetrics() {
    final var hsPerfData =
        HsPerfData.parse(
            g1().longEntry("sun.rt.safepoints", 7)
                .longEntry("sun.rt.safepointTime", 1_500)
                .longEntry("sun.rt.safepointSyncTime", 3)
                .longEntry("java.threads.live", 20)
                .longEntry("java.threads.daemon", 15)
                .longEntry("java.threads.livePeak", 22)
                .longEntry("java.threads.started", 30)
                .build());

    final var expected = new LinkedHashMap<String, Long>();
    expected.put(SAFEPOINTS, 7L);
    expected.put(SAFEPOINT_TIME, 1_500_000L);
    expected.put(SAFEPOINT_SYNC_TIME, 3_000L);
    expected.put(GC_COLLECTIONS + "{gc=G1 young collection pauses}", 10L);
    expected.put(GC_PAUSE + "{gc=G1 young collection pauses}", 50_000L);
    expected.put(GC_COLLECTIONS + "{gc=G1 full collection pauses}", 1L);
    expected.put(GC_PAUSE + "{gc=G1 full collection pauses}", 200_000L);
    expected.put(HEAP_USED, 100L + 10L + 300L);
    expected.put(HEAP_COMMITTED, 400L + 600L);
    expected.put(HEAP_MAX, 1024L);
    expected.put(THREADS_LIVE, 20L);
    expected.put(THREADS_DAEMON, 15L);
    expected.put(THREADS_PEAK, 22L);
    expected.put(THREADS_STARTED, 30L);

    assertEquals(expected, toMap(JvmMetricsReaderAgent.toCounters(hsPerfData)));
  }

  @Test
  void takesCollectorsThatExist() {
    final var hsPerfData =
        HsPerfData.parse(
            new HsPerfDataBuilder()
                .longEntry("sun.os.hrt.frequency", 1_000_000_000)
                .stringEntry("sun.gc.collector.2.name", "ZGC Major Pauses")
                .longEntry("sun.gc.collector.2.invocations", 2)
                .longEntry("sun.gc.collector.2.time", 20)
                .stringEntry("sun.gc.collector.0.name", "ZGC Minor Pauses")
                .longEntry("sun.gc.collector.0.invocations", 1)
                .longEntry("sun.gc.collector.0.time", 10)
                .build());

    final var expected = new LinkedHashMap<String, Long>();
    expected.put(GC_COLLECTIONS + "{gc=ZGC Minor Pauses}", 1L);
    expected.put(GC_PAUSE + "{gc=ZGC Minor Pauses}", 10L);
    expected.put(GC_COLLECTIONS + "{gc=ZGC Major Pauses}", 2L);
    expected.put(GC_PAUSE + "{gc=ZGC Major Pauses}", 20L);

    assertEquals(expected, toMap(JvmMetricsReaderAgent.toCounters(hsPerfData)));
  }

  @Test
  void sumsHeapMaxWhenGenerationsSplitIt() {
    final var hsPerfData =
        HsPerfData.parse(
            new HsPerfDataBuilder()
                .longEntry("sun.gc.generation.0.maxCapacity", 341)
                .longEntry("sun.gc.generation.1.maxCapacity", 683)
                .longEntry("sun.gc.generation.2.maxCapacity", 0)
                .build());

    assertEquals(Map.of(HEAP_MAX, 1024L), toMap(JvmMetricsReaderAgent.toCounters(hsPerfData)));
  }

  @Test
  void skipsMissingCounters() {
    final var hsPerfData = HsPerfData.parse(new HsPerfDataBuilder().build());
    assertEquals(List.of(), JvmMetricsReaderAgent.toCounters(hsPerfData));
  }

  @Test
  void reportsNothingWhileFileIsMissing() throws IOException {
    final var file = new File(tempDir, "1");
    final var agent = newAgent(file);

    agent.doWork();
    Files.write(file.toPath(), g1().buildBytes());
    advanceAndWork(agent);
    Files.delete(file.toPath());
    advanceAndWork(agent);
    Files.write(file.toPath(), g1().buildBytes());
    advanceAndWork(agent);

    assertEquals(4, reads.size());
    assertEquals(List.of(), reads.get(0));
    assertEquals(HEAP_MAX, reads.get(1).get(reads.get(1).size() - 1).label());
    assertEquals(List.of(), reads.get(2));
    assertEquals(reads.get(1).size(), reads.get(3).size());
  }

  @Test
  void reportsNothingForUnsupportedOrNotReadyFile() throws IOException {
    final var file = new File(tempDir, "1");
    final var agent = newAgent(file);

    Files.write(file.toPath(), g1().magic(0xcafebabe).buildBytes());
    agent.doWork();
    Files.write(file.toPath(), g1().accessible(false).buildBytes());
    advanceAndWork(agent);
    Files.write(file.toPath(), new byte[0]);
    advanceAndWork(agent);

    assertEquals(List.of(List.of(), List.of(), List.of()), reads);
  }

  @Test
  void readsOncePerInterval() throws IOException {
    final var file = new File(tempDir, "1");
    Files.write(file.toPath(), g1().buildBytes());
    final var agent = newAgent(file);

    assertEquals(1, agent.doWork());
    epochClock.advance(READ_INTERVAL.toMillis() - 1);
    assertEquals(0, agent.doWork());
    epochClock.advance(1);
    assertEquals(1, agent.doWork());

    assertEquals(2, reads.size());
  }

  private JvmMetricsReaderAgent newAgent(File file) {
    final var agent =
        new JvmMetricsReaderAgent(
            "JvmMetricsReaderAgent", file, false, epochClock, READ_INTERVAL, countersHandler);
    agent.onStart();
    return agent;
  }

  private void advanceAndWork(JvmMetricsReaderAgent agent) {
    epochClock.advance(READ_INTERVAL.toMillis());
    agent.doWork();
  }

  private static HsPerfDataBuilder g1() {
    return new HsPerfDataBuilder()
        .longEntry("sun.os.hrt.frequency", 1_000_000)
        .stringEntry("sun.gc.collector.0.name", "G1 young collection pauses")
        .longEntry("sun.gc.collector.0.invocations", 10)
        .longEntry("sun.gc.collector.0.time", 50)
        .stringEntry("sun.gc.collector.1.name", "G1 full collection pauses")
        .longEntry("sun.gc.collector.1.invocations", 1)
        .longEntry("sun.gc.collector.1.time", 200)
        .longEntry("sun.gc.generation.0.capacity", 400)
        .longEntry("sun.gc.generation.0.maxCapacity", 1024)
        .longEntry("sun.gc.generation.0.space.0.used", 100)
        .longEntry("sun.gc.generation.0.space.1.used", 10)
        .longEntry("sun.gc.generation.1.capacity", 600)
        .longEntry("sun.gc.generation.1.maxCapacity", 1024)
        .longEntry("sun.gc.generation.1.space.0.used", 300);
  }

  // metric name, with gc tag in braces if any -> value; keeps order of counters
  static Map<String, Long> toMap(List<CounterDescriptor> counters) {
    final var keyCodec = new KeyCodec();
    final var map = new LinkedHashMap<String, Long>();
    for (var counter : counters) {
      final var gc = keyCodec.decodeKey(counter.keyBuffer(), 0).stringValue(GC_TAG);
      final var name = gc != null ? counter.label() + "{gc=" + gc + "}" : counter.label();
      assertTrue(map.put(name, counter.value()) == null, "duplicate: " + name);
    }
    return map;
  }
}
