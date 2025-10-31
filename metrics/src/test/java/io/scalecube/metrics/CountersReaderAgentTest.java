package io.scalecube.metrics;

import static io.scalecube.metrics.CountersRegistry.Context.COUNTERS_FILE;
import static io.scalecube.metrics.CountersRegistry.Context.DEFAULT_COUNTERS_DIR_NAME;
import static org.agrona.IoUtil.delete;
import static org.agrona.IoUtil.mapExistingFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.scalecube.metrics.CountersReaderAgent.State;
import io.scalecube.metrics.CountersRegistry.Context;
import io.scalecube.metrics.CountersRegistry.LayoutDescriptor;
import java.io.File;
import java.time.Duration;
import org.agrona.BufferUtil;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CountersReaderAgentTest {

  private static final Duration READ_INTERVAL = Duration.ofSeconds(3);
  private static final long OLD_START_TIMESTAMP = 10042;
  private static final long OLD_PID = 100500;
  private static final int OLD_BUFFER_LENGTH = 8 * 1024 * 1024;

  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final CountersHandler countersHandler = mock(CountersHandler.class);
  private CountersReaderAgent agent;

  @BeforeEach
  void beforeEach() {
    delete(new File(DEFAULT_COUNTERS_DIR_NAME), true);
    agent =
        new CountersReaderAgent(
            "CountersReaderAgent",
            new File(DEFAULT_COUNTERS_DIR_NAME),
            true,
            epochClock,
            READ_INTERVAL,
            countersHandler);
    agent.onStart();
  }

  @AfterEach
  void afterEach() {
    if (agent != null) {
      agent.onClose();
    }
  }

  @Test
  void testWorkWithCounters() {
    try (final var countersRegistry = CountersRegistry.create()) {
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
      epochClock.advance(READ_INTERVAL.toMillis() + 1);
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
      verify(countersHandler).accept(anyLong(), anyList());
    }
  }

  @Test
  void testStartWithoutCounters() {
    agent.doWork();
    assertEquals(State.CLEANUP, agent.state());
  }

  @Test
  void testWorkWhenCountersShutdown() {
    try (final var countersRegistry =
        CountersRegistry.create(new Context().dirDeleteOnShutdown(true))) {
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
    }
    epochClock.advance(READ_INTERVAL.toMillis() + 1);
    agent.doWork();
    assertEquals(State.CLEANUP, agent.state());
  }

  @Test
  void testWorkWhenCountersRestarted() {
    try (final var countersRegistry = CountersRegistry.create(new Context())) {
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
    }
    try (final var countersRegistry = CountersRegistry.create(new Context())) {
      updateCountersHeader(OLD_START_TIMESTAMP, OLD_PID, OLD_BUFFER_LENGTH);
      epochClock.advance(READ_INTERVAL.toMillis() + 1);
      agent.doWork();
      assertEquals(State.CLEANUP, agent.state());
    }
  }

  private static void updateCountersHeader(long startTimestamp, long pid, int bufferLength) {
    final var file = new File(DEFAULT_COUNTERS_DIR_NAME, COUNTERS_FILE);
    final var mappedByteBuffer = mapExistingFile(file, COUNTERS_FILE);
    try {
      final var headerBuffer = LayoutDescriptor.createHeaderBuffer(mappedByteBuffer);
      LayoutDescriptor.fillHeaderBuffer(headerBuffer, startTimestamp, pid, bufferLength);
    } finally {
      BufferUtil.free(mappedByteBuffer);
    }
  }
}
