package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.metrics.MetricsTransmitter.Context;
import java.util.function.Consumer;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TpsMetricTest {

  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final MetricsHandlerImpl metricsHandler = new MetricsHandlerImpl();
  private MetricsRecorder metricsRecorder;
  private MetricsTransmitter metricsTransmitter;
  private MetricsReader metricsReader;

  @BeforeEach
  void beforeEach() {
    metricsRecorder =
        MetricsRecorder.launch(
            new MetricsRecorder.Context().useAgentInvoker(true).epochClock(epochClock));
    metricsRecorder.agentInvoker().invoke(); // init delays

    metricsTransmitter =
        MetricsTransmitter.launch(new Context().useAgentInvoker(true).epochClock(epochClock));
    metricsTransmitter.agentInvoker().invoke(); // kick-off

    metricsReader = new MetricsReader(metricsTransmitter.context().broadcastBuffer());
  }

  @AfterEach
  void afterEach() {
    CloseHelper.quietCloseAll(metricsReader, metricsTransmitter, metricsRecorder);
  }

  @Nested
  class Single {

    @Test
    void testRecord() {
      final var name = "foo";
      final var tps =
          metricsRecorder.newTps(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name));
      metricsRecorder.agentInvoker().invoke(); // on-board

      tps.record();
      tps.record();
      tps.record();
      tps.record();

      advanceClock();
      metricsRecorder.agentInvoker().invoke();
      metricsTransmitter.agentInvoker().invoke();

      metricsReader.read(metricsHandler.reset());
      metricsHandler.assertHasRead();
      metricsHandler.assertName(name);
      metricsHandler.assertValue(4);
    }

    @Test
    void testRecordWithTags() {
      final var name = "foo";
      final var tps =
          metricsRecorder.newTps(
              keyFlyweight ->
                  keyFlyweight
                      .tagsCount(3)
                      .stringValue("name", name)
                      .stringValue("kind", "k")
                      .intValue("mp_id", 100500));
      metricsRecorder.agentInvoker().invoke(); // on-board

      tps.record();
      tps.record();
      tps.record();
      tps.record();

      advanceClock();
      metricsRecorder.agentInvoker().invoke();
      metricsTransmitter.agentInvoker().invoke();

      metricsReader.read(metricsHandler.reset());
      metricsHandler.assertHasRead();
      metricsHandler.assertName(name);
      metricsHandler.assertValue(4);
      metricsHandler.assertKey(
          key -> {
            Assertions.assertEquals(name, key.stringValue("name"), "name");
            Assertions.assertEquals("k", key.stringValue("kind"), "kind");
            Assertions.assertEquals(100500, key.intValue("mp_id"), "mpId");
          });
    }

    @Test
    void testRepeatedSemantic() {
      final var name = "foo";
      final var tps =
          metricsRecorder.newTps(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name));
      metricsRecorder.agentInvoker().invoke(); // on-board

      for (int i = 1; i <= 4; i++) {
        tps.record();
        tps.record();
        tps.record();
        tps.record();

        advanceClock();
        metricsRecorder.agentInvoker().invoke();
        metricsTransmitter.agentInvoker().invoke();

        metricsReader.read(metricsHandler.reset());
        metricsHandler.assertHasRead();
        metricsHandler.assertName(name);
        metricsHandler.assertValue(4);
      }
    }
  }

  @Nested
  class Aggregation {

    @Test
    void testRecord() {
      final var name = "foo";
      final var foo1 =
          metricsRecorder.newTps(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name));
      final var foo2 =
          metricsRecorder.newTps(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name));
      metricsRecorder.agentInvoker().invoke(); // on-board
      metricsRecorder.agentInvoker().invoke(); // on-board

      // foo1

      foo1.record();
      foo1.record();
      foo1.record();
      foo1.record();

      // foo2

      foo2.record();
      foo2.record();
      foo2.record();
      foo2.record();

      // publish

      advanceClock();
      metricsRecorder.agentInvoker().invoke();
      metricsTransmitter.agentInvoker().invoke();

      metricsReader.read(metricsHandler.reset());
      metricsHandler.assertHasRead();
      metricsHandler.assertName(name);
      metricsHandler.assertValue(8);
    }

    @Test
    void testRecordWithTags() {
      final var name = "foo";
      final var foo1 =
          metricsRecorder.newTps(
              keyFlyweight ->
                  keyFlyweight
                      .tagsCount(3)
                      .stringValue("name", name)
                      .stringValue("kind", "k")
                      .intValue("mp_id", 100500));
      final var foo2 =
          metricsRecorder.newTps(
              keyFlyweight ->
                  keyFlyweight
                      .tagsCount(3)
                      .stringValue("name", name)
                      .stringValue("kind", "k")
                      .intValue("mp_id", 100500));
      metricsRecorder.agentInvoker().invoke(); // on-board
      metricsRecorder.agentInvoker().invoke(); // on-board

      // foo1

      foo1.record();
      foo1.record();
      foo1.record();
      foo1.record();

      // foo2

      foo2.record();
      foo2.record();
      foo2.record();
      foo2.record();

      // publish

      advanceClock();
      metricsRecorder.agentInvoker().invoke();
      metricsTransmitter.agentInvoker().invoke();

      metricsReader.read(metricsHandler.reset());
      metricsHandler.assertHasRead();
      metricsHandler.assertName(name);
      metricsHandler.assertValue(8);
      metricsHandler.assertKey(
          key -> {
            Assertions.assertEquals(name, key.stringValue("name"), "name");
            Assertions.assertEquals("k", key.stringValue("kind"), "kind");
            Assertions.assertEquals(100500, key.intValue("mp_id"), "mpId");
          });
    }

    @Test
    void testRepeatedSemantic() {
      final var name = "foo";
      final var foo1 =
          metricsRecorder.newTps(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name));
      final var foo2 =
          metricsRecorder.newTps(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name));
      metricsRecorder.agentInvoker().invoke(); // on-board
      metricsRecorder.agentInvoker().invoke(); // on-board

      for (int i = 1; i <= 4; i++) {
        // foo1

        foo1.record();
        foo1.record();
        foo1.record();
        foo1.record();

        // foo2

        foo2.record();
        foo2.record();
        foo2.record();
        foo2.record();

        // publish

        advanceClock();
        metricsRecorder.agentInvoker().invoke();
        metricsTransmitter.agentInvoker().invoke();

        metricsReader.read(metricsHandler.reset());
        metricsHandler.assertHasRead();
        metricsHandler.assertName(name);
        metricsHandler.assertValue(8);
      }
    }
  }

  private void advanceClock() {
    epochClock.advance(1000);
  }

  private static class MetricsHandlerImpl implements MetricsHandler {

    long timestamp;
    Key key;
    long value = -1;

    @Override
    public void onTps(
        long timestamp, DirectBuffer keyBuffer, int keyOffset, int keyLength, long value) {
      this.timestamp = timestamp;
      this.key = new KeyCodec().decodeKey(keyBuffer, keyOffset);
      this.value = value;
    }

    MetricsHandlerImpl reset() {
      timestamp = 0;
      key = null;
      value = -1;
      return this;
    }

    void assertHasRead() {
      assertTrue(timestamp > 0);
    }

    void assertName(String name) {
      assertNotNull(key, "key");
      assertEquals(name, key.stringValue("name"), "name");
    }

    void assertKey(Consumer<Key> consumer) {
      consumer.accept(key);
    }

    void assertValue(long value) {
      assertEquals(value, this.value, "value");
    }
  }
}
