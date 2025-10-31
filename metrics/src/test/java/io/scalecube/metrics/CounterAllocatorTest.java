package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.metrics.CountersRegistry.Context;
import java.util.ArrayList;
import java.util.List;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class CounterAllocatorTest {

  private static final int TYPE_ID = 100;

  private CountersRegistry countersRegistry;

  @AfterEach
  void tearDown() {
    CloseHelper.quietCloseAll(countersRegistry);
  }

  @ValueSource(ints = {2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024})
  @ParameterizedTest
  void testAllocator(int countersValuesBufferLength) {
    countersRegistry =
        CountersRegistry.create(
            new Context().countersValuesBufferLength(countersValuesBufferLength));
    final var countersManager = countersRegistry.countersManager();
    final var counterAllocator = new CounterAllocator(countersManager);
    final var maxCounters = countersValuesBufferLength / CountersReader.COUNTER_LENGTH;

    for (int times = 0; times < 3; times++) {
      List<AtomicCounter> counters = new ArrayList<>(maxCounters);
      for (int i = 0; i < maxCounters; i++) {
        final var counter = counterAllocator.newCounter(TYPE_ID, "name-" + times + "-" + i, null);
        counter.set(times * i);
        counters.add(counter);
      }
      for (int i = 0; i < counters.size(); i++) {
        final var counter = counters.get(i);
        final var descriptor = CounterDescriptor.getCounter(countersManager, counter.id());
        assertEquals(TYPE_ID, descriptor.typeId());
        assertEquals((long) times * i, descriptor.value());
      }
      CloseHelper.quietCloseAll(counters);
    }
  }
}
