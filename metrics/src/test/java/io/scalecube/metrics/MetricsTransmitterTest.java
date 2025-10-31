package io.scalecube.metrics;

import static io.scalecube.metrics.MetricsTransmitter.Context.BROADCAST_BUFFER_LENGTH_PROP_NAME;
import static io.scalecube.metrics.MetricsTransmitter.Context.HEARTBEAT_TIMEOUT_PROP_NAME;
import static io.scalecube.metrics.MetricsTransmitter.Context.IDLE_STRATEGY_PROP_NAME;
import static io.scalecube.metrics.MetricsTransmitter.Context.METRICS_DIRECTORY_NAME_PROP_NAME;
import static io.scalecube.metrics.MetricsTransmitter.Context.RETRY_INTERVAL_PROP_NAME;
import static io.scalecube.metrics.MetricsTransmitter.Context.WARN_IF_METRICS_NOT_EXISTS_PROP_NAME;
import static org.agrona.IoUtil.delete;
import static org.agrona.IoUtil.mapExistingFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.metrics.CountersRegistry.Context;
import io.scalecube.metrics.MetricsRecorder.LayoutDescriptor;
import io.scalecube.metrics.MetricsTransmitterAgent.State;
import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricsTransmitterTest {

  private static final Duration RETRY_INTERVAL = Duration.ofSeconds(3);
  private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(1);
  private static final long OLD_START_TIMESTAMP = 10042;
  private static final long OLD_PID = 100500;
  private static final int OLD_BUFFER_LENGTH = 8 * 1024 * 1024;

  private static final AtomicInteger INT_COUNTER = new AtomicInteger(1);
  private static final AtomicLong LONG_COUNTER = new AtomicLong(1000L);
  private static final AtomicInteger PATH_COUNTER = new AtomicInteger(1);
  private static final Random RANDOM = new Random(12345L);

  private final CachedEpochClock epochClock = new CachedEpochClock();
  private MetricsTransmitter metricsTransmitter;
  private MetricsTransmitterAgent agent;

  @BeforeEach
  void beforeEach() {
    delete(new File(METRICS_DIRECTORY_NAME_PROP_NAME), true);
    metricsTransmitter =
        MetricsTransmitter.launch(
            new MetricsTransmitter.Context()
                .useAgentInvoker(true)
                .epochClock(epochClock)
                .retryInterval(RETRY_INTERVAL)
                .heartbeatTimeout(HEARTBEAT_TIMEOUT));
    agent = (MetricsTransmitterAgent) metricsTransmitter.agentInvoker().agent();
  }

  @AfterEach
  void afterEach() {
    CloseHelper.quietCloseAll(metricsTransmitter);
  }

  @Test
  void testPopulateFromProperties() {
    // given
    final var metricsDirectoryName = nextPath();
    final var warnIfMetricsNotExists = nextBoolean();
    final var retryInterval = nextLong();
    final var heartbeatTimeout = nextLong();
    final var broadcastBufferLength = nextInt();
    final var idleStrategy = "org.agrona.concurrent.SleepingIdleStrategy";

    Properties props = new Properties();
    props.setProperty(METRICS_DIRECTORY_NAME_PROP_NAME, metricsDirectoryName);
    props.setProperty(WARN_IF_METRICS_NOT_EXISTS_PROP_NAME, String.valueOf(warnIfMetricsNotExists));
    props.setProperty(RETRY_INTERVAL_PROP_NAME, String.valueOf(retryInterval));
    props.setProperty(HEARTBEAT_TIMEOUT_PROP_NAME, String.valueOf(heartbeatTimeout));
    props.setProperty(BROADCAST_BUFFER_LENGTH_PROP_NAME, String.valueOf(broadcastBufferLength));
    props.setProperty(IDLE_STRATEGY_PROP_NAME, idleStrategy);

    // when
    MetricsTransmitter.Context context = new MetricsTransmitter.Context(props);

    // then
    assertEquals(metricsDirectoryName, context.metricsDirectoryName());
    assertEquals(warnIfMetricsNotExists, context.warnIfMetricsNotExists());
    assertEquals(retryInterval, context.retryInterval().toNanos());
    assertEquals(heartbeatTimeout, context.heartbeatTimeout().toNanos());
    assertEquals(broadcastBufferLength, context.broadcastBufferLength());
    assertEquals(idleStrategy, context.idleStrategy().getClass().getName());
  }

  @Test
  void testWorkWithMetrics() {
    try (final var metricsRecorder = MetricsRecorder.launch()) {
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
      epochClock.advance(HEARTBEAT_TIMEOUT.toMillis() + 1);
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
    }
  }

  @Test
  void testStartWithoutMetrics() {
    agent.doWork();
    assertEquals(State.CLEANUP, agent.state());
  }

  @Test
  void testWorkWhenMetricsShutdown() {
    try (final var metricsRecorder =
        MetricsRecorder.launch(new MetricsRecorder.Context().dirDeleteOnShutdown(true))) {
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
    }
    epochClock.advance(HEARTBEAT_TIMEOUT.toMillis() + 1);
    agent.doWork();
    assertEquals(State.CLEANUP, agent.state());
  }

  @Test
  void testWorkWhenMetricsRestarted() {
    try (final var metricsRecorder = MetricsRecorder.launch()) {
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
    }
    try (final var countersRegistry = CountersRegistry.create(new Context())) {
      updateMetricsHeader(OLD_START_TIMESTAMP, OLD_PID, OLD_BUFFER_LENGTH);
      epochClock.advance(HEARTBEAT_TIMEOUT.toMillis() + 1);
      agent.doWork();
      assertEquals(State.CLEANUP, agent.state());
    }
  }

  private static void updateMetricsHeader(long startTimestamp, long pid, int bufferLength) {
    final var metricsDirName = MetricsRecorder.Context.DEFAULT_METRICS_DIR_NAME;
    final var metricsFile = MetricsRecorder.Context.METRICS_FILE;
    final var file = new File(metricsDirName, metricsFile);
    final var mappedByteBuffer = mapExistingFile(file, metricsFile);
    try {
      final var headerBuffer = LayoutDescriptor.createHeaderBuffer(mappedByteBuffer);
      LayoutDescriptor.fillHeaderBuffer(headerBuffer, startTimestamp, pid, bufferLength);
    } finally {
      BufferUtil.free(mappedByteBuffer);
    }
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
