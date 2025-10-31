package io.scalecube.metrics;

import static io.scalecube.metrics.MetricsRecorder.Context.DEFAULT_METRICS_DIR_NAME;
import static io.scalecube.metrics.MetricsRecorder.Context.DIR_DELETE_ON_SHUTDOWN_PROP_NAME;
import static io.scalecube.metrics.MetricsRecorder.Context.IDLE_STRATEGY_PROP_NAME;
import static io.scalecube.metrics.MetricsRecorder.Context.METRICS_BUFFER_LENGTH_PROP_NAME;
import static io.scalecube.metrics.MetricsRecorder.Context.METRICS_DIRECTORY_NAME_PROP_NAME;
import static io.scalecube.metrics.MetricsRecorder.Context.METRICS_FILE;
import static org.agrona.IoUtil.delete;
import static org.agrona.IoUtil.mapExistingFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.metrics.MetricsRecorder.Context;
import io.scalecube.metrics.MetricsRecorder.LayoutDescriptor;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricsRecorderTest {

  private static final long OLD_START_TIMESTAMP = 10042;
  private static final long OLD_PID = 100500;
  private static final int OLD_BUFFER_LENGTH = 8 * 1024 * 1024;
  private static final long START_TIMESTAMP = ManagementFactory.getRuntimeMXBean().getStartTime();
  private static final long PID = ManagementFactory.getRuntimeMXBean().getPid();

  private static final AtomicInteger INT_COUNTER = new AtomicInteger(1);
  private static final AtomicLong LONG_COUNTER = new AtomicLong(1000L);
  private static final AtomicInteger PATH_COUNTER = new AtomicInteger(1);
  private static final Random RANDOM = new Random(12345L);

  private final List<AutoCloseable> resources = new ArrayList<>();

  @BeforeEach
  void beforeEach() {
    CloseHelper.quietCloseAll(resources);
    resources.clear();
    delete(new File(DEFAULT_METRICS_DIR_NAME), true);
  }

  @Test
  void testPopulateFromProperties() {
    // given
    final var metricsDirectoryName = nextPath();
    final var dirDeleteOnShutdown = nextBoolean();
    final var metricsBufferLength = nextInt();
    final var idleStrategy = "org.agrona.concurrent.SleepingIdleStrategy";

    Properties props = new Properties();
    props.setProperty(METRICS_DIRECTORY_NAME_PROP_NAME, metricsDirectoryName);
    props.setProperty(DIR_DELETE_ON_SHUTDOWN_PROP_NAME, String.valueOf(dirDeleteOnShutdown));
    props.setProperty(METRICS_BUFFER_LENGTH_PROP_NAME, String.valueOf(metricsBufferLength));
    props.setProperty(IDLE_STRATEGY_PROP_NAME, idleStrategy);

    // when
    Context context = new Context(props);

    // then
    assertEquals(metricsDirectoryName, context.metricsDirectoryName());
    assertEquals(dirDeleteOnShutdown, context.dirDeleteOnShutdown());
    assertEquals(metricsBufferLength, context.metricsBufferLength());
    assertEquals(idleStrategy, context.idleStrategy().getClass().getName());
  }

  @Test
  void testStartManyFromScratch() {
    final var foo = addResource(MetricsRecorder.launch(new Context().dirDeleteOnShutdown(true)));
    final var bar = addResource(MetricsRecorder.launch(new Context().dirDeleteOnShutdown(true)));
    final var baz = addResource(MetricsRecorder.launch(new Context().dirDeleteOnShutdown(true)));

    final var fileList =
        List.of(
            new File(foo.context().metricsDir(), METRICS_FILE),
            new File(bar.context().metricsDir(), METRICS_FILE),
            new File(baz.context().metricsDir(), METRICS_FILE));

    for (var f1 : fileList) {
      for (var f2 : fileList) {
        if (f1 != f2) {
          assertTrue(f1.exists());
          assertTrue(f2.exists());
          assertEquals(f1, f2);
          final var b1 = new UnsafeBuffer(mapExistingFile(f1, f1.getName()));
          final var b2 = new UnsafeBuffer(mapExistingFile(f2, f2.getName()));
          assertEquals(b1, b2);
        }
      }
    }

    assertMetricsHeader(START_TIMESTAMP, PID);
  }

  @Test
  void testStartManyAfterRestart() {
    var foo = addResource(MetricsRecorder.launch(new Context().dirDeleteOnShutdown(false)));
    var bar = addResource(MetricsRecorder.launch(new Context().dirDeleteOnShutdown(false)));
    var baz = addResource(MetricsRecorder.launch(new Context().dirDeleteOnShutdown(false)));

    // Stop

    CloseHelper.quietCloseAll(resources);
    resources.clear();

    // Restart

    updateMetricsHeader(OLD_START_TIMESTAMP, OLD_PID, OLD_BUFFER_LENGTH);
    foo = addResource(MetricsRecorder.launch(new Context().dirDeleteOnShutdown(true)));
    bar = addResource(MetricsRecorder.launch(new Context().dirDeleteOnShutdown(true)));
    baz = addResource(MetricsRecorder.launch(new Context().dirDeleteOnShutdown(true)));

    final var fileList =
        List.of(
            new File(foo.context().metricsDir(), METRICS_FILE),
            new File(bar.context().metricsDir(), METRICS_FILE),
            new File(baz.context().metricsDir(), METRICS_FILE));

    for (var f1 : fileList) {
      for (var f2 : fileList) {
        if (f1 != f2) {
          assertTrue(f1.exists());
          assertTrue(f2.exists());
          assertEquals(f1, f2);
          final var b1 = new UnsafeBuffer(mapExistingFile(f1, f1.getName()));
          final var b2 = new UnsafeBuffer(mapExistingFile(f2, f2.getName()));
          assertEquals(b1, b2);
        }
      }
    }

    assertMetricsHeader(START_TIMESTAMP, PID);
  }

  private static void updateMetricsHeader(long startTimestamp, long pid, int bufferLength) {
    final var file = new File(DEFAULT_METRICS_DIR_NAME, METRICS_FILE);
    final var mappedByteBuffer = mapExistingFile(file, METRICS_FILE);
    try {
      final var headerBuffer = LayoutDescriptor.createHeaderBuffer(mappedByteBuffer);
      LayoutDescriptor.fillHeaderBuffer(headerBuffer, startTimestamp, pid, bufferLength);
    } finally {
      BufferUtil.free(mappedByteBuffer);
    }
  }

  private static void assertMetricsHeader(long startTimestamp, long pid) {
    final var file = new File(DEFAULT_METRICS_DIR_NAME, METRICS_FILE);
    final var mappedByteBuffer = mapExistingFile(file, METRICS_FILE);
    try {
      final var headerBuffer = LayoutDescriptor.createHeaderBuffer(mappedByteBuffer);
      assertEquals(startTimestamp, LayoutDescriptor.startTimestamp(headerBuffer), "startTimestamp");
      assertEquals(pid, LayoutDescriptor.pid(headerBuffer), "pid");
    } finally {
      BufferUtil.free(mappedByteBuffer);
    }
  }

  private <T extends AutoCloseable> T addResource(T resource) {
    resources.add(resource);
    return resource;
  }

  private static int nextInt() {
    return INT_COUNTER.getAndIncrement();
  }

  private static long nextLong() {
    return LONG_COUNTER.getAndAdd(100L);
  }

  private static String nextPath() {
    return Paths.get(SystemUtil.tmpDirName() + "test-path-" + PATH_COUNTER.getAndIncrement())
        .toString();
  }

  private static boolean nextBoolean() {
    return RANDOM.nextBoolean();
  }
}
