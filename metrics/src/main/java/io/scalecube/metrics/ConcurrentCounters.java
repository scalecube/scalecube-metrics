package io.scalecube.metrics;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.status.CountersReader.DEFAULT_TYPE_ID;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

/**
 * Manages {@link AtomicCounter} instances with unique keys composed of {@code typeId}, {@code
 * name}, and optional attributes. Can be used safely from multiple threads.
 */
public class ConcurrentCounters {

  private final CountersManager countersManager;

  private final ThreadLocal<Encoder> encoderHolder = ThreadLocal.withInitial(Encoder::new);

  // NOTE: entries are never evicted. If a counter is freed via the CountersManager this cache
  // still holds, and will hand back, the stale AtomicCounter, and the map grows unbounded for the
  // lifetime of the process. Add a removal path tied to the counter lifecycle.
  private final Map<DirectBuffer, AtomicCounter> counters = new ConcurrentHashMap<>();

  public ConcurrentCounters(CountersManager countersManager) {
    this.countersManager = countersManager;
  }

  /**
   * Creates new, or returns existing, {@link AtomicCounter} instance.
   *
   * @param name name
   * @param consumer consumer (optional)
   * @return {@link AtomicCounter} instance
   */
  public AtomicCounter counter(String name, Consumer<KeyFlyweight> consumer) {
    return counter(DEFAULT_TYPE_ID, name, consumer);
  }

  /**
   * Creates new, or returns existing, {@link AtomicCounter} instance.
   *
   * @param typeId typeId
   * @param name name
   * @param consumer consumer (optional)
   * @return {@link AtomicCounter} instance
   */
  public AtomicCounter counter(int typeId, String name, Consumer<KeyFlyweight> consumer) {
    final var encoder = encoderHolder.get();
    final var buffer = encoder.buffer;

    buffer.putInt(0, typeId);
    final int nameLength = buffer.putStringWithoutLengthAscii(SIZE_OF_INT, name);
    final int keyOffset = SIZE_OF_INT + nameLength;

    final var keyFlyweight = encoder.keyFlyweight.wrap(buffer, keyOffset);
    if (consumer != null) {
      consumer.accept(keyFlyweight);
    }
    final int keyTagsLength = keyFlyweight.length();

    // The cache key is the exact bytes [0, keyLength): typeId + name + encoded attributes. Agrona
    // buffer equals/hashCode compare over the whole capacity, so the lookup view and the stored
    // copy must both be sized exactly to keyLength; otherwise stale trailing bytes (or a differing
    // capacity) miss the cache and leak a duplicate counter slot for an already-registered key.
    final int keyLength = keyOffset + keyTagsLength;
    final var keyView = encoder.keyView;
    keyView.wrap(buffer, 0, keyLength);

    final var counter = counters.get(keyView);
    if (counter != null) {
      return counter;
    }

    final var key = new UnsafeBuffer(new byte[keyLength]);
    key.putBytes(0, buffer, 0, keyLength);
    return counters.computeIfAbsent(
        key,
        // The encoded attributes at [keyOffset, +keyTagsLength) are the counter key; the name at
        // [SIZE_OF_INT, +nameLength) is the label. Both already live in buffer, so pass those
        // regions directly instead of re-copying.
        k ->
            countersManager.newCounter(
                typeId, buffer, keyOffset, keyTagsLength, buffer, SIZE_OF_INT, nameLength));
  }

  /** Per-thread scratch state for encoding and looking up counter keys. */
  private static final class Encoder {
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    final KeyFlyweight keyFlyweight = new KeyFlyweight();
    final UnsafeBuffer keyView = new UnsafeBuffer();
  }
}
