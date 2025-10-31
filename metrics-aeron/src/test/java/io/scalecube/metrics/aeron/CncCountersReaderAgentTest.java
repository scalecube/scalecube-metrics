package io.scalecube.metrics.aeron;

import static io.aeron.CommonContext.AERON_DIR_PROP_DEFAULT;
import static org.agrona.IoUtil.delete;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.scalecube.metrics.CountersHandler;
import io.scalecube.metrics.aeron.CncCountersReaderAgent.State;
import java.io.File;
import java.time.Duration;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CncCountersReaderAgentTest {

  private static final Duration READ_INTERVAL = Duration.ofSeconds(3);
  private static final Duration DRIVER_TIMEOUT = Duration.ofMillis(500);

  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final CountersHandler countersHandler = mock(CountersHandler.class);
  private CncCountersReaderAgent agent;

  @BeforeEach
  void beforeEach() {
    delete(new File(AERON_DIR_PROP_DEFAULT), true);
    agent =
        new CncCountersReaderAgent(
            "CncCountersReaderAgent",
            AERON_DIR_PROP_DEFAULT,
            true,
            epochClock,
            READ_INTERVAL,
            DRIVER_TIMEOUT,
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
  void testWorkWithCncCounters() {
    try (final var mediaDriver = MediaDriver.launch()) {
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
      epochClock.advance(READ_INTERVAL.toMillis() + 1);
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
      verify(countersHandler).accept(anyLong(), anyList());
    }
  }

  @Test
  void testStartWithoutCncCounters() {
    agent.doWork();
    assertEquals(State.CLEANUP, agent.state());
  }

  @Test
  void testWorkWhenCncCountersShutdown() throws InterruptedException {
    try (final var mediaDriver = MediaDriver.launch(new Context().dirDeleteOnShutdown(true))) {
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
    }
    epochClock.advance(READ_INTERVAL.toMillis() + 1);
    agent.doWork();
    assertEquals(State.RUNNING, agent.state());

    Thread.sleep(DRIVER_TIMEOUT.toMillis());
    epochClock.advance(READ_INTERVAL.toMillis() + 1);
    agent.doWork();
    assertEquals(State.CLEANUP, agent.state());
  }

  @Test
  void testWorkWhenCncCountersRestarted() throws InterruptedException {
    try (final var mediaDriver = MediaDriver.launch(new Context().dirDeleteOnShutdown(true))) {
      agent.doWork();
      assertEquals(State.RUNNING, agent.state());
    }

    Thread.sleep(DRIVER_TIMEOUT.toMillis());
    epochClock.advance(READ_INTERVAL.toMillis() + 1);
    agent.doWork();
    assertEquals(State.CLEANUP, agent.state());

    try (final var mediaDriver = MediaDriver.launch(new Context().dirDeleteOnShutdown(true))) {
      agent.doWork();
      assertEquals(State.INIT, agent.state());
    }
  }
}
