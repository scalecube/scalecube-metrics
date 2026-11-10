package io.scalecube.metrics.aeron;

import static io.aeron.AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID;
import static io.aeron.AeronCounters.DRIVER_RECEIVER_POS_TYPE_ID;
import static io.aeron.AeronCounters.DRIVER_SUBSCRIBER_POSITION_TYPE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import io.scalecube.metrics.Key;
import io.scalecube.metrics.KeyCodec;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.agrona.CloseHelper;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DriverCountersAdapterTest {

  private static final int SELECTED_STREAM_ID = 1001;
  private static final int OTHER_STREAM_ID = 1002;
  private static final String SELECTED_CHANNEL = "aeron:udp?endpoint=localhost:24325";
  private static final String OTHER_CHANNEL = "aeron:udp?endpoint=localhost:24326";
  private static final Set<Integer> STREAM_POSITION_TYPE_IDS =
      Set.of(
          DRIVER_PUBLISHER_POS_TYPE_ID,
          DRIVER_RECEIVER_POS_TYPE_ID,
          DRIVER_SUBSCRIBER_POSITION_TYPE_ID);

  @TempDir Path tempDir;

  private MediaDriver mediaDriver;
  private Aeron aeron;
  private Publication publication;
  private Subscription subscription;
  private String aeronDirectoryName;
  private final List<CounterDescriptor> received = new ArrayList<>();

  @BeforeEach
  void setUp() {
    aeronDirectoryName = tempDir.resolve("aeron").toString();
    mediaDriver =
        MediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
    aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));

    publication = aeron.addExclusivePublication(SELECTED_CHANNEL, SELECTED_STREAM_ID);
    subscription = aeron.addSubscription(SELECTED_CHANNEL, SELECTED_STREAM_ID);
    final var otherPublication = aeron.addExclusivePublication(OTHER_CHANNEL, OTHER_STREAM_ID);
    final var otherSubscription = aeron.addSubscription(OTHER_CHANNEL, OTHER_STREAM_ID);
    awaitConnected(publication, subscription);
    awaitConnected(otherPublication, otherSubscription);

    final var message = new UnsafeBuffer(new byte[64]);
    while (publication.offer(message) < 0) {
      Thread.onSpinWait();
    }
    while (subscription.poll((buffer, offset, length, header) -> {}, 10) == 0) {
      Thread.onSpinWait();
    }
  }

  @AfterEach
  void tearDown() {
    CloseHelper.quietCloseAll(aeron, mediaDriver);
  }

  @Test
  void readsStreamPositionsOfSelectedStreamsOnly() {
    final var streamIds = new IntHashSet();
    streamIds.add(SELECTED_STREAM_ID);
    final var cut = newAgent(streamIds);

    final var counters = readCounters(cut);

    final var positions =
        counters.stream().filter(c -> STREAM_POSITION_TYPE_IDS.contains(c.typeId())).toList();
    assertEquals(3, positions.size(), () -> "one pub-pos, rcv-pos and sub-pos: " + positions);
    positions.forEach(
        c -> assertEquals(SELECTED_STREAM_ID, key(c).intValue("streamId"), c::toString));

    final var subscriberPosition = byType(positions, DRIVER_SUBSCRIBER_POSITION_TYPE_ID);
    final var key = key(subscriberPosition);
    assertEquals("driver_subscriber_position", key.stringValue("name"));
    assertEquals(subscription.registrationId(), key.longValue("registrationId"));
    assertEquals(publication.sessionId(), key.intValue("sessionId"));
    assertEquals(SELECTED_CHANNEL, key.stringValue("channel"));
    assertEquals(subscription.imageAtIndex(0).position(), subscriberPosition.value());

    assertEquals(
        "driver_publisher_position",
        key(byType(positions, DRIVER_PUBLISHER_POS_TYPE_ID)).stringValue("name"));
    assertEquals(
        "driver_receiver_position",
        key(byType(positions, DRIVER_RECEIVER_POS_TYPE_ID)).stringValue("name"));
  }

  @Test
  void readsNoStreamPositionsByDefault() {
    final var cut =
        new CncCountersReaderAgent(
            "test",
            aeronDirectoryName,
            true,
            SystemEpochClock.INSTANCE,
            Duration.ZERO,
            countersHandler());

    final var counters = readCounters(cut);

    assertTrue(
        counters.stream().noneMatch(c -> STREAM_POSITION_TYPE_IDS.contains(c.typeId())),
        counters::toString);
  }

  private CncCountersReaderAgent newAgent(IntHashSet streamIds) {
    return new CncCountersReaderAgent(
        "test",
        aeronDirectoryName,
        true,
        SystemEpochClock.INSTANCE,
        Duration.ZERO,
        streamIds,
        countersHandler());
  }

  private CountersHandler countersHandler() {
    return new CountersHandler() {
      @Override
      public void accept(long timestamp, List<CounterDescriptor> descriptors) {
        received.addAll(descriptors);
      }
    };
  }

  private List<CounterDescriptor> readCounters(CncCountersReaderAgent agent) {
    agent.onStart();
    agent.doWork();
    agent.onClose();
    return received;
  }

  private static CounterDescriptor byType(List<CounterDescriptor> counters, int typeId) {
    return counters.stream().filter(c -> c.typeId() == typeId).findFirst().orElseThrow();
  }

  private static Key key(CounterDescriptor counter) {
    return new KeyCodec().decodeKey(counter.keyBuffer(), 0);
  }

  private static void awaitConnected(Publication publication, Subscription subscription) {
    while (!publication.isConnected() || !subscription.isConnected()) {
      Thread.onSpinWait();
    }
  }
}
