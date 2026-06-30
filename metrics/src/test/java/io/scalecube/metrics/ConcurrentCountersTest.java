package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.AtomicCounter;
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
  void shouldReturnSameCounterAfterInterveningLongerKey() {
    // Re-requesting the same (name, key) must always return the cached counter, even when a
    // counter with a longer key was allocated on the same thread in between. The lookup reuses a
    // shared, growing ThreadLocal buffer; the intervening longer key leaves stale trailing bytes
    // (and may grow capacity), so a key-identity that compares over full buffer capacity would
    // miss the cache and leak a new counter slot with an identical visible key.
    var first = concurrentCounters.counter("agent_state", agentKey("IndicativePriceAgent", 30));

    // Intervening allocation with a strictly longer name and tag value on the same thread.
    concurrentCounters.counter(
        "agent_stream_position_total", agentKey("MarketDataReplayAgentWithLongerRoleName", 30));

    var second = concurrentCounters.counter("agent_state", agentKey("IndicativePriceAgent", 30));

    assertSame(
        first,
        second,
        "Re-requesting the same name and key must return the cached counter, not a new slot");
  }

  @Test
  void shouldNotLeakCountersWhenSameKeyRequestedRepeatedly() {
    // Mirrors ScalecubeBridge.catalogueWriterAgent re-requesting agent_* counters on every
    // onClusterFound: the same logical metric must map to exactly one allocated counter slot.
    final var beforeCount = countersManager.maxCounterId();

    AtomicCounter previous = null;
    for (int event = 0; event < 5; event++) {
      // A different, longer key is touched first on this thread each iteration (as happens in
      // real allocation sequences), then the target counter is (re-)requested.
      concurrentCounters.counter("agent_error_total", agentKey("MarketDataReplayAgent", 30));

      var current = concurrentCounters.counter("agent_state", agentKey("IndicativePriceAgent", 30));

      if (previous != null) {
        assertSame(previous, current, "Every re-request must yield the same counter instance");
      }
      previous = current;
    }

    // Two distinct logical counters (agent_error_total + agent_state) means at most two new slots,
    // regardless of how many times they were requested.
    assertTrue(
        countersManager.maxCounterId() - beforeCount <= 2,
        "Repeated requests for the same keys must not allocate additional counter slots");
  }

  @Test
  void shouldNotInheritStaleKeyForNoKeyCounterOnSameThread() {
    // Same thread, same name: a keyed counter followed by a no-key counter. The no-key request
    // must not reuse the KeyFlyweight state left by the keyed one. Using the same name makes the
    // buffer prefix identical, so if the no-key key length still included the previous key's
    // (stale) attributes the two counters would alias.
    var keyed = concurrentCounters.counter("metric", key -> key.tagsCount(1).intValue("x", 1));
    var noKey = concurrentCounters.counter("metric", null);

    assertNotSame(
        keyed, noKey, "A no-key counter must not collide with a keyed counter of the same name");

    // The no-key counter is still cached on its own identity.
    assertSame(noKey, concurrentCounters.counter("metric", null), "No-key counter must be cached");
  }

  @Test
  void shouldAllocateSingleCounterUnderConcurrentFirstRequests() throws Exception {
    // CountersManager is not thread-safe and "check the cache, then allocate, then store" is a
    // compound action; without serialization, threads racing on the same not-yet-created key each
    // allocate a separate slot. Many threads released together, repeated over many rounds (a new
    // key per round), make that race overwhelmingly likely to surface if it regresses.
    final var threads = 8;
    final var rounds = 50;
    final var pool = Executors.newFixedThreadPool(threads);
    try {
      for (int round = 0; round < rounds; round++) {
        final int r = round;
        final var barrier = new CyclicBarrier(threads);
        final var futures = new ArrayList<Future<AtomicCounter>>();
        final var before = allocatedCounterCount();

        for (int i = 0; i < threads; i++) {
          futures.add(
              pool.submit(
                  () -> {
                    barrier.await();
                    return concurrentCounters.counter(
                        "contended-" + r, key -> key.tagsCount(1).intValue("k", r));
                  }));
        }

        final var first = futures.get(0).get();
        for (var future : futures) {
          assertSame(
              first, future.get(), "Concurrent requests for the same key must return one counter");
        }
        assertEquals(
            before + 1,
            allocatedCounterCount(),
            "Concurrent first-time allocation must create exactly one counter slot");
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private int allocatedCounterCount() {
    final int[] count = {0};
    countersManager.forEach((counterId, typeId, keyBuffer, label) -> count[0]++);
    return count[0];
  }

  private static Consumer<KeyFlyweight> agentKey(String roleName, int clusterId) {
    return key ->
        key.tagsCount(2).stringValue("roleName", roleName).intValue("clusterId", clusterId);
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
