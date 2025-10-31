package io.scalecube.metrics;

import static org.agrona.concurrent.status.CountersReader.DEFAULT_TYPE_ID;

import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

/**
 * Allocates {@link AtomicCounter} objects with custom metadata and name using {@link
 * CountersManager}.
 */
public class CounterAllocator {

  private final CountersManager countersManager;

  private final ExpandableArrayBuffer nameBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final KeyFlyweight keyFlyweight = new KeyFlyweight();

  public CounterAllocator(CountersManager countersManager) {
    this.countersManager = countersManager;
  }

  /**
   * Allocates new {@link AtomicCounter} by calling {@link CountersManager#newCounter(int,
   * DirectBuffer, int, int, DirectBuffer, int, int)}.
   *
   * @param name name
   * @param consumer consumer for {@link KeyFlyweight} (optional)
   * @return newly allocated {@link AtomicCounter}
   */
  public AtomicCounter newCounter(String name, Consumer<KeyFlyweight> consumer) {
    return newCounter(DEFAULT_TYPE_ID, name, consumer);
  }

  /**
   * Allocates new {@link AtomicCounter} by calling {@link CountersManager#newCounter(int,
   * DirectBuffer, int, int, DirectBuffer, int, int)}.
   *
   * @param typeId typeId
   * @param name name
   * @param consumer consumer for {@link KeyFlyweight} (optional)
   * @return newly allocated {@link AtomicCounter}
   */
  public AtomicCounter newCounter(int typeId, String name, Consumer<KeyFlyweight> consumer) {
    final var nameLength = nameBuffer.putStringWithoutLengthAscii(0, name);
    if (consumer != null) {
      consumer.accept(keyFlyweight.wrap(keyBuffer, 0));
    }
    return countersManager.newCounter(
        typeId,
        keyFlyweight.buffer(),
        keyFlyweight.offset(),
        keyFlyweight.length(),
        nameBuffer,
        0,
        nameLength);
  }
}
