package io.scalecube.metrics.aeron;

import static io.aeron.AeronCounters.DRIVER_PUBLISHER_POS_TYPE_ID;
import static io.aeron.AeronCounters.DRIVER_RECEIVER_POS_TYPE_ID;
import static io.aeron.AeronCounters.DRIVER_SUBSCRIBER_POSITION_TYPE_ID;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

import io.scalecube.metrics.KeyFlyweight;
import java.util.function.IntPredicate;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;

/**
 * Converts the media driver's stream position counters of selected streams (default none):
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

  /** Selects no stream: no converters are registered. */
  public static final IntPredicate NO_STREAMS = streamId -> false;

  /** Selects every stream. */
  public static final IntPredicate ALL_STREAMS = streamId -> true;

  private final IntPredicate streams;
  private final KeyFlyweight keyFlyweight = new KeyFlyweight();

  private DriverCountersAdapter(IntPredicate streams) {
    this.streams = streams;
  }

  /**
   * Selects the given streams.
   *
   * @param streamIds stream ids; none selects no stream
   * @return the selection
   */
  public static IntPredicate streams(int... streamIds) {
    if (streamIds.length == 0) {
      return NO_STREAMS;
    }
    final var set = new IntHashSet();
    for (var streamId : streamIds) {
      set.add(streamId);
    }
    return set::contains;
  }

  /**
   * Registers the converters, unless {@code streams} is {@link #NO_STREAMS}.
   *
   * @param map converters by counter type id
   * @param streams selects the streams to convert the counters of, e.g. {@link #ALL_STREAMS},
   *     {@link #streams(int...)}
   */
  public static void populate(Int2ObjectHashMap<KeyConverter> map, IntPredicate streams) {
    if (streams == NO_STREAMS) {
      return;
    }
    final var adapter = new DriverCountersAdapter(streams);
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
    if (!streams.test(streamId)) {
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
