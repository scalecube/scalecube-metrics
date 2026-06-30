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

  private final ThreadLocal<ExpandableArrayBuffer> bufferHolder =
      ThreadLocal.withInitial(ExpandableArrayBuffer::new);
  private final ThreadLocal<ExpandableArrayBuffer> nameBufferHolder =
      ThreadLocal.withInitial(ExpandableArrayBuffer::new);
  private final ThreadLocal<KeyFlyweight> keyFlyweightHolder =
      ThreadLocal.withInitial(KeyFlyweight::new);
  private final ThreadLocal<UnsafeBuffer> keyViewHolder =
      ThreadLocal.withInitial(UnsafeBuffer::new);
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
    int offset = 0;
    final var buffer = bufferHolder.get();

    buffer.putInt(offset, typeId);
    offset += SIZE_OF_INT;
    offset += buffer.putStringWithoutLengthAscii(offset, name);

    final var keyFlyweight = keyFlyweightHolder.get();
    if (consumer != null) {
      consumer.accept(keyFlyweight.wrap(buffer, offset));
    }

    // The cache key is the exact key bytes [0, keyLength) written into the (reused, over-sized)
    // ThreadLocal buffer. Agrona buffer equals/hashCode compare over the whole capacity, so the
    // lookup and the stored key must both be sized exactly to keyLength; otherwise stale trailing
    // bytes (or a differing capacity) cause the lookup to miss and a duplicate counter slot to be
    // allocated for an already-registered key.
    final var keyLength = offset + keyFlyweight.length();
    final var keyView = keyViewHolder.get();
    keyView.wrap(buffer, 0, keyLength);

    var counter = counters.get(keyView);
    if (counter != null) {
      return counter;
    }

    final var nameBuffer = nameBufferHolder.get();
    final var nameLength = nameBuffer.putStringWithoutLengthAscii(0, name);

    counter =
        countersManager.newCounter(
            typeId,
            keyFlyweight.buffer(),
            keyFlyweight.offset(),
            keyFlyweight.length(),
            nameBuffer,
            0,
            nameLength);

    final var keyBuffer = new UnsafeBuffer(new byte[keyLength]);
    keyBuffer.putBytes(0, buffer, 0, keyLength);
    counters.put(keyBuffer, counter);

    return counter;
  }
}
