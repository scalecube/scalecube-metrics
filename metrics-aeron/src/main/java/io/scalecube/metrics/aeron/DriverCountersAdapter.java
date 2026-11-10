package io.scalecube.metrics.aeron;

import static io.aeron.AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID;
import static io.aeron.AeronCounters.DRIVER_RECEIVER_POS_TYPE_ID;
import static io.aeron.AeronCounters.DRIVER_SUBSCRIBER_POSITION_TYPE_ID;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

import io.scalecube.metrics.KeyFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;

/**
 * Converts the media driver's stream position counters of selected streams:
 *
 * <ul>
 *   <li>{@code pub-pos} → {@code driver_publisher_position}
 *   <li>{@code rcv-pos} → {@code driver_receiver_position}
 *   <li>{@code sub-pos} → {@code driver_subscriber_position}
 * </ul>
 *
 * <p>Tags: {@code registrationId}, {@code sessionId}, {@code streamId}, {@code channel}, read from
 * the driver's {@code StreamCounter} key.
 */
public class DriverCountersAdapter {

  // io.aeron.driver.status.StreamCounter key layout
  private static final int REGISTRATION_ID_OFFSET = 0;
  private static final int SESSION_ID_OFFSET = REGISTRATION_ID_OFFSET + SIZE_OF_LONG;
  private static final int STREAM_ID_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
  private static final int CHANNEL_OFFSET = STREAM_ID_OFFSET + SIZE_OF_INT;

  private final IntHashSet streamIds;
  private final KeyFlyweight keyFlyweight = new KeyFlyweight();

  private DriverCountersAdapter(IntHashSet streamIds) {
    this.streamIds = streamIds;
  }

  /**
   * Registers the converters, unless {@code streamIds} is empty.
   *
   * @param map converters by counter type id
   * @param streamIds streams to convert the counters of
   */
  public static void populate(Int2ObjectHashMap<KeyConverter> map, IntHashSet streamIds) {
    if (streamIds.isEmpty()) {
      return;
    }
    final var adapter = new DriverCountersAdapter(streamIds);
    map.put(
        DRIVER_PUBLISHER_POS_TYPE_ID,
        (keyBuffer, label) -> adapter.convert("driver_publisher_position", keyBuffer));
    map.put(
        DRIVER_RECEIVER_POS_TYPE_ID,
        (keyBuffer, label) -> adapter.convert("driver_receiver_position", keyBuffer));
    map.put(
        DRIVER_SUBSCRIBER_POSITION_TYPE_ID,
        (keyBuffer, label) -> adapter.convert("driver_subscriber_position", keyBuffer));
  }

  /**
   * Converts a stream counter key.
   *
   * @return the key, or {@code null} when the counter's stream is not selected
   */
  private DirectBuffer convert(String name, DirectBuffer keyBuffer) {
    final var streamId = keyBuffer.getInt(STREAM_ID_OFFSET);
    if (!streamIds.contains(streamId)) {
      return null;
    }
    return keyFlyweight
        .wrap(new ExpandableArrayBuffer(), 0)
        .tagsCount(5)
        .stringValue("name", name)
        .longValue("registrationId", keyBuffer.getLong(REGISTRATION_ID_OFFSET))
        .intValue("sessionId", keyBuffer.getInt(SESSION_ID_OFFSET))
        .intValue("streamId", streamId)
        .stringValue("channel", keyBuffer.getStringAscii(CHANNEL_OFFSET))
        .buffer();
  }
}
