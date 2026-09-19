package io.scalecube.metrics;

import static io.scalecube.metrics.CounterTags.COUNTER_VISIBILITY;
import static io.scalecube.metrics.CounterTags.WRITE_EPOCH_ID;
import static io.scalecube.metrics.CounterVisibility.PRIVATE;
import static io.scalecube.metrics.CountersRegistry.Context.DEFAULT_COUNTERS_DIR_NAME;
import static org.agrona.IoUtil.delete;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.scalecube.metrics.CountersReaderAgent.State;
import io.scalecube.metrics.CountersRegistry.Context;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CountersReaderAgentTest {

  private static final Duration READ_INTERVAL = Duration.ofSeconds(3);

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
  void testTruncatedCountersFileIsReportedInsteadOfCrashing(@TempDir File dir) throws IOException {
    countersFileOfLength(dir, 8);

    final var handler = mock(CountersHandler.class);
    final var truncatedAgent = newAgent(dir, handler);

    assertDoesNotThrow(truncatedAgent::doWork, "a truncated file must not blow up the read cycle");
    assertEquals(State.CLEANUP, truncatedAgent.state(), "a bad file routes to cleanup");
    truncatedAgent.doWork();
    assertEquals(State.READ_COUNTERS, truncatedAgent.state(), "and the agent recovers");
    verifyNoInteractions(handler);
  }

  @Test
  void testCountersFileShorterThanItsHeaderDeclaresIsReportedInsteadOfCrashing(@TempDir File dir)
      throws IOException {
    try (final var countersRegistry = CountersRegistry.create()) {
      countersRegistry.countersManager().newCounter("foo").set(1);
      copyTruncated(
          new File(DEFAULT_COUNTERS_DIR_NAME),
          dir,
          CountersRegistry.LayoutDescriptor.HEADER_LENGTH + 1024);
    }

    final var handler = mock(CountersHandler.class);
    final var truncatedAgent = newAgent(dir, handler);

    assertDoesNotThrow(
        truncatedAgent::doWork, "a file shorter than its header declares must not be read");
    assertEquals(State.CLEANUP, truncatedAgent.state(), "a bad file routes to cleanup");
    truncatedAgent.doWork();
    assertEquals(State.READ_COUNTERS, truncatedAgent.state(), "and the agent recovers");
    verifyNoInteractions(handler);
  }

  private CountersReaderAgent newAgent(File dir, CountersHandler handler) {
    final var newAgent =
        new CountersReaderAgent(
            "CountersReaderAgent", dir, false, epochClock, Duration.ZERO, handler);
    newAgent.onStart();
    return newAgent;
  }

  private static void countersFileOfLength(File dir, int length) throws IOException {
    try (var file = new RandomAccessFile(new File(dir, Context.COUNTERS_FILE), "rw")) {
      file.setLength(length);
    }
  }

  private static void copyTruncated(File srcDir, File dstDir, int length) throws IOException {
    final var target = new File(dstDir, Context.COUNTERS_FILE);
    Files.copy(
        new File(srcDir, Context.COUNTERS_FILE).toPath(),
        target.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    try (var file = new RandomAccessFile(target, "rw")) {
      file.setLength(length);
    }
  }

  @Test
  void testWorkWithEmptyCounters() {
    try (final var countersRegistry = CountersRegistry.create()) {
      agent.doWork();
      assertEquals(State.READ_COUNTERS, agent.state());
      epochClock.advance(READ_INTERVAL.toMillis() + 1);
      agent.doWork();
      assertEquals(State.READ_COUNTERS, agent.state());
      verify(countersHandler, times(2))
          .accept(anyLong(), assertArg(list -> assertEquals(0, list.size())));
    }
  }

  @Test
  void testWorkWithPlainCounters() {
    try (final var countersRegistry = CountersRegistry.create()) {
      final var name = "foo";
      final var value = 100500;
      final var counter = countersRegistry.countersManager().newCounter(name);
      counter.set(value);

      agent.doWork();
      assertEquals(State.READ_COUNTERS, agent.state());
      epochClock.advance(READ_INTERVAL.toMillis() + 1);
      agent.doWork();
      assertEquals(State.READ_COUNTERS, agent.state());
      verify(countersHandler, times(2))
          .accept(
              anyLong(),
              assertArg(
                  list -> {
                    assertEquals(1, list.size());
                    final var counterDescriptor = list.get(0);
                    assertEquals(name, counterDescriptor.label(), "label");
                    assertEquals(value, counterDescriptor.value(), "value");
                  }));
    }
  }

  @Test
  void testWorkWithWriteEpochCounters() {
    try (final var countersRegistry = CountersRegistry.create()) {
      final var counterAllocator = new CounterAllocator(countersRegistry.countersManager());
      final var writeEpoch =
          counterAllocator.newCounter(
              "writeEpoch",
              flyweight ->
                  flyweight
                      .tagsCount(1)
                      .enumValue(COUNTER_VISIBILITY, PRIVATE, CounterVisibility::value));
      final var foo =
          counterAllocator.newCounter(
              "foo", flyweight -> flyweight.tagsCount(1).intValue(WRITE_EPOCH_ID, writeEpoch.id()));
      final var bar =
          counterAllocator.newCounter(
              "bar", flyweight -> flyweight.tagsCount(1).intValue(WRITE_EPOCH_ID, writeEpoch.id()));

      writeEpoch.increment();
      foo.set(1);
      bar.set(2);
      writeEpoch.increment();

      agent.doWork();
      assertEquals(State.READ_COUNTERS, agent.state());
      epochClock.advance(READ_INTERVAL.toMillis() + 1);
      agent.doWork();
      assertEquals(State.READ_COUNTERS, agent.state());
      verify(countersHandler, times(2))
          .accept(
              anyLong(),
              assertArg(
                  list -> {
                    assertEquals(2, list.size());
                    assertEquals(
                        Set.of(foo.id(), bar.id()),
                        list.stream()
                            .map(CounterDescriptor::counterId)
                            .collect(Collectors.toSet()));
                  }));
    }
  }

  @Test
  void testWorkWithWriteInProgressEpochCounters() {
    try (final var countersRegistry = CountersRegistry.create()) {
      final var counterAllocator = new CounterAllocator(countersRegistry.countersManager());
      final var writeEpoch =
          counterAllocator.newCounter(
              "writeEpoch",
              flyweight ->
                  flyweight
                      .tagsCount(1)
                      .enumValue(COUNTER_VISIBILITY, PRIVATE, CounterVisibility::value));
      final var foo =
          counterAllocator.newCounter(
              "foo", flyweight -> flyweight.tagsCount(1).intValue(WRITE_EPOCH_ID, writeEpoch.id()));
      final var bar =
          counterAllocator.newCounter(
              "bar", flyweight -> flyweight.tagsCount(1).intValue(WRITE_EPOCH_ID, writeEpoch.id()));

      writeEpoch.increment();
      foo.set(1);
      bar.set(2);
      // writeEpoch.increment();

      agent.doWork();
      assertEquals(State.READ_COUNTERS, agent.state());
      epochClock.advance(READ_INTERVAL.toMillis() + 1);
      agent.doWork();
      assertEquals(State.READ_COUNTERS, agent.state());
      verify(countersHandler, times(2))
          .accept(anyLong(), assertArg(list -> assertEquals(0, list.size())));
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
      assertEquals(State.READ_COUNTERS, agent.state());
    }
    epochClock.advance(READ_INTERVAL.toMillis() + 1);
    agent.doWork();
    assertEquals(State.CLEANUP, agent.state());
  }

  @Test
  void testWorkWhenCountersRestarted() {
    try (final var countersRegistry = CountersRegistry.create(new Context())) {
      agent.doWork();
      assertEquals(State.READ_COUNTERS, agent.state());
    }
    try (final var countersRegistry = CountersRegistry.create(new Context())) {
      epochClock.advance(READ_INTERVAL.toMillis() + 1);
      agent.doWork();
      assertEquals(State.READ_COUNTERS, agent.state());
    }
  }
}
