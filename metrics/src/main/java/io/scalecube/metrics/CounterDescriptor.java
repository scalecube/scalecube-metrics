package io.scalecube.metrics;

import static org.agrona.concurrent.status.CountersReader.KEY_OFFSET;
import static org.agrona.concurrent.status.CountersReader.MAX_KEY_LENGTH;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

/**
 * Descriptor for counter managed by {@link CountersReader}. This record provides metadata and
 * runtime information about counter, including its id, type, current value, associated attributes
 * (key buffer), and human-readable label.
 */
public record CounterDescriptor(
    int counterId, int typeId, long value, DirectBuffer keyBuffer, String label) {

  /**
   * Retrieves {@code CounterDescriptor} by counter id.
   *
   * @param countersReader countersReader
   * @param counterId counterId
   * @return {@code CounterDescriptor} instance
   */
  public static CounterDescriptor getCounter(CountersReader countersReader, int counterId) {
    final var metaDataBuffer = countersReader.metaDataBuffer();
    final var metaDataOffset = CountersReader.metaDataOffset(counterId);

    final int keyOffset = metaDataOffset + KEY_OFFSET;
    byte[] keyBytes = new byte[MAX_KEY_LENGTH];
    metaDataBuffer.getBytes(keyOffset, keyBytes);
    final var keyBuffer = new UnsafeBuffer(keyBytes);

    final int typeId = countersReader.getCounterTypeId(counterId);
    final var value = countersReader.getCounterValue(counterId);
    final var label = countersReader.getCounterLabel(counterId);

    return new CounterDescriptor(counterId, typeId, value, keyBuffer, label);
  }

  /**
   * Finds first counter by predicate.
   *
   * @param countersReader countersReader
   * @param predicate predicate
   * @return {@code CounterDescriptor}, or null
   */
  public static CounterDescriptor findFirstCounter(
      CountersReader countersReader, Predicate<CounterDescriptor> predicate) {
    final var counters = findAllCounters(countersReader, predicate);
    return counters.isEmpty() ? null : counters.get(0);
  }

  /**
   * Finds last counter by predicate.
   *
   * @param countersReader countersReader
   * @param predicate predicate
   * @return {@code CounterDescriptor}, or null
   */
  public static CounterDescriptor findLastCounter(
      CountersReader countersReader, Predicate<CounterDescriptor> predicate) {
    final var counters = findAllCounters(countersReader, predicate);
    return counters.isEmpty() ? null : counters.get(counters.size() - 1);
  }

  /**
   * Finds all counters by predicate.
   *
   * @param countersReader countersReader
   * @param predicate predicate
   * @return list of {@code CounterDescriptor} objects
   */
  public static List<CounterDescriptor> findAllCounters(
      CountersReader countersReader, Predicate<CounterDescriptor> predicate) {
    final var list = new ArrayList<CounterDescriptor>();
    countersReader.forEach(
        (value, counterId, label) -> {
          final var descriptor = CounterDescriptor.getCounter(countersReader, counterId);
          if (predicate.test(descriptor)) {
            list.add(descriptor);
          }
        });
    return list;
  }

  public static Predicate<CounterDescriptor> byName(String name) {
    return descriptor -> name.equals(descriptor.label());
  }

  public static Predicate<CounterDescriptor> byValue(long value) {
    return descriptor -> value == descriptor.value();
  }

  public static Predicate<CounterDescriptor> byType(int typeId) {
    return descriptor -> typeId == descriptor.typeId();
  }

  public static Predicate<CounterDescriptor> byTag(String tag, byte value) {
    return descriptor -> {
      final var keyCodec = new KeyCodec();
      final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
      return Objects.equals(value, key.byteValue(tag));
    };
  }

  public static Predicate<CounterDescriptor> byTag(String tag, short value) {
    return descriptor -> {
      final var keyCodec = new KeyCodec();
      final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
      return Objects.equals(value, key.shortValue(tag));
    };
  }

  public static Predicate<CounterDescriptor> byTag(String tag, int value) {
    return descriptor -> {
      final var keyCodec = new KeyCodec();
      final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
      return Objects.equals(value, key.intValue(tag));
    };
  }

  public static Predicate<CounterDescriptor> byTag(String tag, long value) {
    return descriptor -> {
      final var keyCodec = new KeyCodec();
      final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
      return Objects.equals(value, key.longValue(tag));
    };
  }

  public static Predicate<CounterDescriptor> byTag(String tag, double value) {
    return descriptor -> {
      final var keyCodec = new KeyCodec();
      final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
      return Objects.equals(value, key.doubleValue(tag));
    };
  }

  public static Predicate<CounterDescriptor> byTag(String tag, String value) {
    return descriptor -> {
      final var keyCodec = new KeyCodec();
      final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
      return Objects.equals(value, key.stringValue(tag));
    };
  }

  public static <T extends Enum<T>> Predicate<CounterDescriptor> byTag(
      String tag, T value, Function<Byte, T> enumFunc) {
    return descriptor -> {
      final var keyCodec = new KeyCodec();
      final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
      return Objects.equals(value, key.enumValue(tag, enumFunc));
    };
  }
}
