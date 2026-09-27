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
import java.util.stream.LongStream;
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
    assertEquals(State.READ_METRICS, agent.state(), "and the agent goes back to retrying");
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
    assertEquals(State.READ_METRICS, agent.state(), "and the agent goes back to retrying");
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
      assertEquals(State.READ_METRICS, agent.state());

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
      assertEquals(State.READ_METRICS, agent.state(), "lapped reader keeps running");
      final var first = values.get(0);
      assertTrue(first > 2, "lost messages are skipped, first: " + first);
      assertEquals(count, values.get(values.size() - 1), "and reading continues up to the latest");
    } finally {
      CloseHelper.quietClose(recorder);
    }
  }

  @Test
  void testPositionIsKeptAcrossReads(@TempDir File dir) {
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
      agent.doWork(); // attach

      long value = 0;
      for (int read = 0; read < 5; read++) {
        for (int i = 0; i < 3; i++) {
          transmitter.transmitTps(++value);
        }
        drain(agent); // each read maps and unmaps the file
      }

      assertEquals(
          LongStream.rangeClosed(1, value).boxed().toList(), values, "all messages, in order");
    } finally {
      CloseHelper.quietClose(recorder);
    }
  }

  @Test
  void testWriterRestartBetweenReads(@TempDir File dir) {
    var recorder = launchRecorder(dir);
    try {
      var transmitter = new MetricsTransmitter(dir);
      final var values = new ArrayList<Long>();
      final var agent = newAgent(dir, tpsHandler(values));
      agent.doWork(); // attach

      // old writer is well ahead, so its position is meaningless for the new file
      for (long i = 1; i <= 100; i++) {
        transmitter.transmitTps(i);
      }
      drain(agent);
      assertEquals(100, values.size());

      // restart within one read interval: reader never sees the file missing
      CloseHelper.quietClose(recorder);
      recorder = launchRecorder(dir);
      transmitter = new MetricsTransmitter(dir);
      transmitter.pid(ProcessHandle.current().pid() + 1);
      values.clear();

      transmitter.transmitTps(1001);
      drain(agent);
      assertEquals(State.READ_METRICS, agent.state());
      assertEquals(List.of(), values, "re-attaches from the latest message of the new writer");

      transmitter.transmitTps(1002);
      transmitter.transmitTps(1003);
      drain(agent);
      assertEquals(List.of(1002L, 1003L), values, "then reads the new writer");
    } finally {
      CloseHelper.quietClose(recorder);
    }
  }

  @Test
  void testWriterGoneThenBack(@TempDir File dir) {
    var recorder = launchRecorder(dir);
    try {
      var transmitter = new MetricsTransmitter(dir);
      final var values = new ArrayList<Long>();
      final var agent = newAgent(dir, tpsHandler(values));
      agent.doWork(); // attach

      transmitter.transmitTps(1);
      drain(agent);
      assertEquals(List.of(1L), values);

      // writer gone
      CloseHelper.quietClose(recorder);
      IoUtil.delete(dir, false);
      drain(agent);
      assertEquals(State.CLEANUP, agent.state(), "missing file routes to cleanup");
      agent.doWork();
      assertEquals(State.READ_METRICS, agent.state(), "and back to reading");

      // writer back
      recorder = launchRecorder(dir);
      transmitter = new MetricsTransmitter(dir);
      values.clear();
      drain(agent); // attach

      transmitter.transmitTps(2);
      transmitter.transmitTps(3);
      drain(agent);
      assertEquals(List.of(2L, 3L), values);
    } finally {
      CloseHelper.quietClose(recorder);
    }
  }

  private static MetricsRecorder launchRecorder(File dir) {
    return MetricsRecorder.launch(
        new Context().metricsDirectoryName(dir.getPath()).useAgentInvoker(true));
  }

  private static MetricsHandler tpsHandler(List<Long> values) {
    return new MetricsHandler() {
      @Override
      public void onTps(
          long timestamp, DirectBuffer keyBuffer, int keyOffset, int length, long value) {
        values.add(value);
      }
    };
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
        assertEquals(State.READ_METRICS, agent.state());
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

  private static class MetricsTransmitter {

    private final MetricsEncoder encoder = new MetricsEncoder();
    private final UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[64]);
    private final int keyLength;
    private final UnsafeBuffer header;
    private final BroadcastTransmitter transmitter;

    MetricsTransmitter(File dir) {
      final var mappedBuffer = IoUtil.mapExistingFile(new File(dir, Context.METRICS_FILE), "test");
      header = MetricsRecorder.LayoutDescriptor.createHeaderBuffer(mappedBuffer);
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

    void pid(long pid) {
      MetricsRecorder.LayoutDescriptor.fillHeaderBuffer(
          header,
          MetricsRecorder.LayoutDescriptor.startTimestamp(header),
          pid,
          MetricsRecorder.LayoutDescriptor.metricsBufferLength(header));
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
