package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import io.scalecube.metrics.MetricsReaderAgent.State;
import io.scalecube.metrics.MetricsRecorder.Context;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetricsReaderAgentTest {

  private static final Duration RETRY_INTERVAL = Duration.ofSeconds(3);

  private final CachedEpochClock epochClock = new CachedEpochClock();

  @Test
  void testTruncatedMetricsFileIsReportedInsteadOfCrashing(@TempDir File dir) throws IOException {
    metricsFileOfLength(dir, 8);

    final var handler = mock(MetricsHandler.class);
    final var agent = newAgent(dir, handler);

    assertDoesNotThrow(agent::doWork, "a truncated file must not blow up initialisation");
    assertEquals(State.CLEANUP, agent.state(), "a bad file routes to cleanup");
    agent.doWork();
    assertEquals(State.INIT, agent.state(), "and the agent goes back to retrying");
    verifyNoInteractions(handler);
  }

  @Test
  void testMetricsFileShorterThanItsHeaderDeclaresIsReportedInsteadOfCrashing(@TempDir File dir)
      throws IOException {
    final var sourceDir = Files.createTempDirectory("metrics-reader-agent").toFile();
    final var recorder =
        MetricsRecorder.launch(
            new Context().metricsDirectoryName(sourceDir.getPath()).useAgentInvoker(true));
    try {
      copyTruncated(sourceDir, dir, MetricsRecorder.LayoutDescriptor.HEADER_LENGTH + 1024);
    } finally {
      CloseHelper.quietClose(recorder);
    }

    final var handler = mock(MetricsHandler.class);
    final var agent = newAgent(dir, handler);

    assertDoesNotThrow(agent::doWork, "a file shorter than its header declares must not be read");
    assertEquals(State.CLEANUP, agent.state(), "a bad file routes to cleanup");
    agent.doWork();
    assertEquals(State.INIT, agent.state(), "and the agent goes back to retrying");
    verifyNoInteractions(handler);
  }

  @Test
  void testLappedReaderSkipsLostMessagesAndKeepsRunning(@TempDir File dir) {
    final var recorder =
        MetricsRecorder.launch(
            new Context().metricsDirectoryName(dir.getPath()).useAgentInvoker(true));
    try {
      final var transmitter = new MetricsTransmitter(dir);
      final var values = new ArrayList<Long>();
      final var agent =
          newAgent(
              dir,
              new MetricsHandler() {
                @Override
                public void onTps(
                    long timestamp, DirectBuffer keyBuffer, int keyOffset, int length, long value) {
                  values.add(value);
                }
              });
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());

      transmitter.transmitTps(1);
      drain(agent);
      assertEquals(List.of(1L), values);

      // way more than fits into the buffer, reader is lapped
      final var count = 2 * transmitter.capacity() / 32;
      for (long i = 2; i <= count; i++) {
        transmitter.transmitTps(i);
      }

      values.clear();
      assertDoesNotThrow(() -> drain(agent), "lapped reader must not fail");
      assertEquals(State.RUNNING, agent.state(), "lapped reader keeps running");
      final var first = values.get(0);
      assertTrue(first > 2, "lost messages are skipped, first: " + first);
      assertEquals(count, values.get(values.size() - 1), "and reading continues up to the latest");
    } finally {
      CloseHelper.quietClose(recorder);
    }
  }

  @Test
  void testReadersKeepNothingMappedBetweenReads(@TempDir File dir) {
    final var recorder =
        MetricsRecorder.launch(
            new Context().metricsDirectoryName(dir.getPath()).useAgentInvoker(true));
    final var agents = new ArrayList<MetricsReaderAgent>();
    try {
      final var transmitter = new MetricsTransmitter(dir);
      final var directBefore = memoryUsed("direct");
      final var mappedBefore = memoryUsed("mapped");
      for (int i = 0; i < 16; i++) {
        final var agent = newAgent(dir, mock(MetricsHandler.class));
        agent.doWork();
        assertEquals(State.RUNNING, agent.state());
        agents.add(agent);
      }
      transmitter.transmitTps(1);
      agents.forEach(this::drain);

      final var directUsed = memoryUsed("direct") - directBefore;
      assertTrue(
          directUsed < 1024 * 1024,
          "16 readers must not reserve direct memory, used: " + directUsed);
      final var mappedUsed = memoryUsed("mapped") - mappedBefore;
      assertTrue(
          mappedUsed <= 0, "16 readers must not keep metrics file mapped, used: " + mappedUsed);
    } finally {
      agents.forEach(MetricsReaderAgent::onClose);
      CloseHelper.quietClose(recorder);
    }
  }

  private static long memoryUsed(String bufferPool) {
    return ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
        .filter(pool -> bufferPool.equals(pool.getName()))
        .mapToLong(BufferPoolMXBean::getMemoryUsed)
        .sum();
  }

  private void drain(MetricsReaderAgent agent) {
    epochClock.advance(RETRY_INTERVAL.toMillis());
    agent.doWork();
  }

  /** Writes tps messages straight into metrics file of running {@link MetricsRecorder}. */
  private static class MetricsTransmitter {

    private final MetricsEncoder encoder = new MetricsEncoder();
    private final UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[64]);
    private final int keyLength;
    private final BroadcastTransmitter transmitter;

    MetricsTransmitter(File dir) {
      final var mappedBuffer = IoUtil.mapExistingFile(new File(dir, Context.METRICS_FILE), "test");
      final var header = MetricsRecorder.LayoutDescriptor.createHeaderBuffer(mappedBuffer);
      transmitter =
          new BroadcastTransmitter(
              new UnsafeBuffer(
                  mappedBuffer,
                  MetricsRecorder.LayoutDescriptor.HEADER_LENGTH,
                  MetricsRecorder.LayoutDescriptor.metricsBufferLength(header)));
      keyLength =
          new KeyFlyweight().wrap(keyBuffer, 0).tagsCount(1).stringValue("name", "tps").length();
    }

    int capacity() {
      return transmitter.capacity();
    }

    void transmitTps(long value) {
      final var length = encoder.encodeTps(0, new UnsafeBuffer(keyBuffer, 0, keyLength), value);
      transmitter.transmit(1, encoder.buffer(), 0, length);
    }
  }

  private MetricsReaderAgent newAgent(File dir, MetricsHandler handler) {
    final var agent =
        new MetricsReaderAgent(
            "MetricsReaderAgent", dir, false, epochClock, RETRY_INTERVAL, handler);
    agent.onStart();
    return agent;
  }

  private static void metricsFileOfLength(File dir, int length) throws IOException {
    try (var file = new RandomAccessFile(new File(dir, Context.METRICS_FILE), "rw")) {
      file.setLength(length);
    }
  }

  private static void copyTruncated(File srcDir, File dstDir, int length) throws IOException {
    final var target = new File(dstDir, Context.METRICS_FILE);
    Files.copy(
        new File(srcDir, Context.METRICS_FILE).toPath(),
        target.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    try (var file = new RandomAccessFile(target, "rw")) {
      file.setLength(length);
    }
  }
}
