package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.metrics.MetricsTransmitter.Context;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.HdrHistogram.Histogram;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class HistogramMetricTest {

  private static final long HIGHEST_TRACKABLE_VALUE = TimeUnit.SECONDS.toNanos(1);
  private static final double CONVERSION_FACTOR = 1e-3;
  private static final long RESOLUTION = TimeUnit.SECONDS.toMillis(1);

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
      final var histogram =
          metricsRecorder.newHistogram(
              keyFlyweight ->
                  keyFlyweight
                      .tagsCount(3)
                      .stringValue("name", name)
                      .stringValue("kind", "k")
                      .stringValue("type", "t"),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION);
      metricsRecorder.agentInvoker().invoke(); // on-board

      histogram.record(100);
      histogram.record(200);
      histogram.record(300);
      histogram.record(200);

      advanceClock(RESOLUTION);
      metricsRecorder.agentInvoker().invoke();
      metricsTransmitter.agentInvoker().invoke();

      metricsReader.read(metricsHandler.reset());
      metricsHandler.assertHasRead();
      metricsHandler.assertName(name);
      assertEquals(4, metricsHandler.accumulated.getTotalCount(), "accumulated.totalCount");
      assertEquals(4, metricsHandler.distinct.getTotalCount(), "distinct.totalCount");
      metricsHandler.assertKey(
          key -> {
            Assertions.assertEquals(name, key.stringValue("name"), "name");
            Assertions.assertEquals("k", key.stringValue("kind"), "kind");
            Assertions.assertEquals("t", key.stringValue("type"), "type");
          });
    }

    @Test
    void testRepeatedSemantic() {
      final var name = "foo";
      final var histogram =
          metricsRecorder.newHistogram(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION);
      metricsRecorder.agentInvoker().invoke(); // on-board

      for (int i = 1; i <= 4; i++) {
        histogram.record(100);
        histogram.record(200);
        histogram.record(300);
        histogram.record(200);

        advanceClock(RESOLUTION);
        metricsRecorder.agentInvoker().invoke();
        metricsTransmitter.agentInvoker().invoke();

        metricsReader.read(metricsHandler.reset());
        metricsHandler.assertHasRead();
        metricsHandler.assertName(name);
        assertEquals(i * 4, metricsHandler.accumulated.getTotalCount(), "accumulated.totalCount");
        assertEquals(4, metricsHandler.distinct.getTotalCount(), "distinct.totalCount");
      }
    }
  }

  @Nested
  class Aggregation {

    @Test
    void testRecord() {
      final var name = "foo";
      final var foo1 =
          metricsRecorder.newHistogram(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION);
      final var foo2 =
          metricsRecorder.newHistogram(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION);
      metricsRecorder.agentInvoker().invoke(); // on-board
      metricsRecorder.agentInvoker().invoke(); // on-board

      // foo1

      foo1.record(100);
      foo1.record(200);
      foo1.record(300);
      foo1.record(200);

      // foo2

      foo2.record(100);
      foo2.record(200);
      foo2.record(300);
      foo2.record(200);

      // publish

      advanceClock(RESOLUTION);
      metricsRecorder.agentInvoker().invoke();
      metricsTransmitter.agentInvoker().invoke();

      metricsReader.read(metricsHandler.reset());
      metricsHandler.assertHasRead();
      metricsHandler.assertName(name);
      assertEquals(8, metricsHandler.accumulated.getTotalCount(), "accumulated.totalCount");
      assertEquals(8, metricsHandler.distinct.getTotalCount(), "distinct.totalCount");
    }

    @Test
    void testRecordWithTags() {
      final var name = "foo";
      final var foo1 =
          metricsRecorder.newHistogram(
              keyFlyweight ->
                  keyFlyweight
                      .tagsCount(3)
                      .stringValue("name", name)
                      .stringValue("kind", "k")
                      .stringValue("type", "t"),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION);
      final var foo2 =
          metricsRecorder.newHistogram(
              keyFlyweight ->
                  keyFlyweight
                      .tagsCount(3)
                      .stringValue("name", name)
                      .stringValue("kind", "k")
                      .stringValue("type", "t"),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION);
      metricsRecorder.agentInvoker().invoke(); // on-board
      metricsRecorder.agentInvoker().invoke(); // on-board

      // foo1

      foo1.record(100);
      foo1.record(200);
      foo1.record(300);
      foo1.record(200);

      // foo2

      foo2.record(100);
      foo2.record(200);
      foo2.record(300);
      foo2.record(200);

      // publish

      advanceClock(RESOLUTION);
      metricsRecorder.agentInvoker().invoke();
      metricsTransmitter.agentInvoker().invoke();

      metricsReader.read(metricsHandler.reset());
      metricsHandler.assertHasRead();
      metricsHandler.assertName(name);
      assertEquals(8, metricsHandler.accumulated.getTotalCount(), "accumulated.totalCount");
      assertEquals(8, metricsHandler.distinct.getTotalCount(), "distinct.totalCount");
      metricsHandler.assertKey(
          key -> {
            Assertions.assertEquals(name, key.stringValue("name"), "name");
            Assertions.assertEquals("k", key.stringValue("kind"), "kind");
            Assertions.assertEquals("t", key.stringValue("type"), "type");
          });
    }

    @Test
    void testRepeatedSemantic() {
      final var name = "foo";
      final var foo1 =
          metricsRecorder.newHistogram(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION);
      final var foo2 =
          metricsRecorder.newHistogram(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", name),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION);
      metricsRecorder.agentInvoker().invoke(); // on-board
      metricsRecorder.agentInvoker().invoke(); // on-board

      for (int i = 1; i <= 4; i++) {
        // foo1

        foo1.record(100);
        foo1.record(200);
        foo1.record(300);
        foo1.record(200);

        // foo2

        foo2.record(100);
        foo2.record(200);
        foo2.record(300);
        foo2.record(200);

        // publish

        advanceClock(RESOLUTION);
        metricsRecorder.agentInvoker().invoke();
        metricsTransmitter.agentInvoker().invoke();

        metricsReader.read(metricsHandler.reset());
        metricsHandler.assertHasRead();
        metricsHandler.assertName(name);
        assertEquals(i * 8, metricsHandler.accumulated.getTotalCount(), "accumulated.totalCount");
        assertEquals(8, metricsHandler.distinct.getTotalCount(), "distinct.totalCount");
      }
    }
  }

  private void advanceClock(long step) {
    epochClock.advance(step);
  }

  private static class MetricsHandlerImpl implements MetricsHandler {

    long timestamp;
    Key key;
    Histogram accumulated;
    Histogram distinct;
    long highestTrackableValue;
    double conversionFactor;

    @Override
    public void onHistogram(
        long timestamp,
        DirectBuffer keyBuffer,
        int keyOffset,
        int keyLength,
        Histogram accumulated,
        Histogram distinct,
        long highestTrackableValue,
        double conversionFactor) {
      this.timestamp = timestamp;
      this.key = new KeyCodec().decodeKey(keyBuffer, keyOffset);
      this.accumulated = accumulated;
      this.distinct = distinct;
      this.highestTrackableValue = highestTrackableValue;
      this.conversionFactor = conversionFactor;
    }

    MetricsHandlerImpl reset() {
      timestamp = 0;
      key = null;
      accumulated = null;
      distinct = null;
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
  }
}
