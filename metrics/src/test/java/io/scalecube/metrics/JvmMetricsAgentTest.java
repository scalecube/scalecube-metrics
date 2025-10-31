package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.Test;

public class JvmMetricsAgentTest {

  public JvmMetricsAgentTest() {}

  @Test
  public void testAgent() {
    final var clock = new CachedEpochClock();

    final var reg = CountersRegistry.create();

    JvmMetricsAgent cut =
        new JvmMetricsAgent(new CounterAllocator(reg.countersManager()), clock, 1000L, true);
    cut.onStart();
    assertEquals(JvmMetricsAgent.State.INIT, cut.state());
    int ret = cut.doWork();
    assertEquals(JvmMetricsAgent.State.RUNNING, cut.state());
    assertEquals(1, ret);
    ret = cut.doWork();
    assertEquals(JvmMetricsAgent.State.RUNNING, cut.state());
    assertEquals(0, ret);
    ret = cut.doWork();
    assertEquals(JvmMetricsAgent.State.RUNNING, cut.state());
    assertEquals(0, ret);

    clock.advance(1001);
    ret = cut.doWork();
    assertEquals(1, ret);

    reg.countersManager()
        .forEach(
            (long value, int counterId, String label) -> {
              final var descriptor = CounterDescriptor.getCounter(reg.countersManager(), counterId);
              final var key = new KeyCodec().decodeKey(descriptor.keyBuffer(), 0);
              System.out.println(
                  String.format(
                      "label %s, id:%d, tags %s, value: %d", label, counterId, key.tags(), value));
            });
  }
}
