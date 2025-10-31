package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ConcurrentCountersTest {

  private final CountersRegistry countersRegistry = CountersRegistry.create();
  private final CountersManager countersManager = countersRegistry.countersManager();
  private final ConcurrentCounters concurrentCounters = new ConcurrentCounters(countersManager);

  @AfterEach
  void afterEach() {
    CloseHelper.quietCloseAll(countersRegistry);
  }

  @Test
  void shouldCreateCounterWithName() {
    var counter = concurrentCounters.counter("test-counter", null);
    assertNotNull(counter, "Counter should not be null");

    counter.increment();
    assertEquals(1, counter.get(), "Counter should increment correctly");
  }

  @Test
  void shouldReturnSameCounterForSameNameAndType() {
    var counter1 = concurrentCounters.counter("same-counter", null);
    var counter2 = concurrentCounters.counter("same-counter", null);

    assertSame(counter1, counter2, "Same name and type should return same counter instance");

    counter1.increment();
    assertEquals(1, counter2.get(), "Incrementing one should reflect on the other");
  }

  @Test
  void shouldCreateDistinctCountersForDifferentNames() {
    var counter1 = concurrentCounters.counter("counter-1", null);
    var counter2 = concurrentCounters.counter("counter-2", null);

    assertNotSame(counter1, counter2, "Different names should create distinct counters");
  }

  @Test
  void shouldIncludeKeyAttributes() {
    var counter =
        concurrentCounters.counter("key-counter", key -> key.tagsCount(1).intValue("k", 42));

    // Retrieve it again with same key attributes
    var sameCounter =
        concurrentCounters.counter("key-counter", key -> key.tagsCount(1).intValue("k", 42));

    assertSame(counter, sameCounter, "Same name and key attributes should return same counter");

    // Changing the key attribute should create a new counter
    var differentCounter =
        concurrentCounters.counter("key-counter", key -> key.tagsCount(1).intValue("k", 43));
    assertNotSame(counter, differentCounter, "Different key attributes should create new counter");
  }

  @Test
  void shouldWorkConcurrently() throws InterruptedException {
    final var threads = 10;
    final var iterations = 1000;
    final var counter = concurrentCounters.counter("concurrent-counter", null);

    Thread[] workers = new Thread[threads];
    for (int i = 0; i < threads; i++) {
      workers[i] =
          new Thread(
              () -> {
                for (int j = 0; j < iterations; j++) {
                  counter.increment();
                }
              });
      workers[i].start();
    }

    for (var t : workers) {
      t.join();
    }

    assertEquals(
        threads * iterations,
        counter.get(),
        "Counter should be incremented correctly concurrently");
  }

  @Test
  void shouldCreateDistinctCountersForDifferentTypeIds() {
    final var typeId1 = 1;
    final var typeId2 = 2;
    final var name = "typed-counter";

    final var counter1 = concurrentCounters.counter(typeId1, name, null);
    final var counter2 = concurrentCounters.counter(typeId2, name, null);

    assertNotSame(
        counter1, counter2, "Same name but different typeId should create distinct counters");

    // increment both and verify independent values
    counter1.increment();
    counter2.increment();
    counter2.increment();

    assertEquals(1, counter1.get(), "Counter1 should have its own value");
    assertEquals(2, counter2.get(), "Counter2 should have its own value");
  }

  @Test
  void shouldDistinguishByTypeIdAndKeyAttributes() {
    int typeId = 10;
    String name = "complex-counter";

    var counter1 =
        concurrentCounters.counter(typeId, name, key -> key.tagsCount(1).intValue("g", 100));
    var counter2 =
        concurrentCounters.counter(typeId, name, key -> key.tagsCount(1).intValue("g", 200));

    assertNotSame(
        counter1,
        counter2,
        "Different key attributes with same typeId and name should create distinct counters");
  }
}
