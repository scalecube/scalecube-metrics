package io.scalecube.metrics;

import static io.scalecube.metrics.CounterDescriptor.byType;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

/**
 * Registry for storing typed properties as Agrona counters. Supports setting properties of any
 * primitive type and retrieving them via {@link CountersReader}.
 */
public class PropertiesRegistry {

  public static final int PROPERTY_COUNTER_TYPE_ID = 2;

  private final CountersManager countersManager;

  private final ThreadLocal<CounterAllocator> counterAllocatorHolder;
  private final Map<String, AtomicCounter> counters = new ConcurrentHashMap<>();

  /**
   * Creates new registry backed by given {@link CountersManager}.
   *
   * @param countersManager countersManager
   */
  public PropertiesRegistry(CountersManager countersManager) {
    this.countersManager = countersManager;
    this.counterAllocatorHolder =
        ThreadLocal.withInitial(() -> new CounterAllocator(this.countersManager));
  }

  /**
   * Stores or updates property. If property does not exist, new counter is allocated, otherwise,
   * counter is updated with the new value.
   *
   * @param name property name
   * @param value property value
   */
  public void put(String name, Object value) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(value, "value");
    counters.compute(
        name,
        (k, counter) -> {
          if (counter == null) {
            final var counterAllocator = counterAllocatorHolder.get();
            return counterAllocator.newCounter(
                PROPERTY_COUNTER_TYPE_ID,
                name + "=" + value,
                keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("visibility", "private"));
          } else {
            countersManager.setCounterLabel(counter.id(), name + "=" + value);
            return counter;
          }
        });
  }

  /**
   * Retrieves {@link Byte} property.
   *
   * @param countersReader countersReader
   * @param name property name
   * @return property value, or {@code null} if not found
   */
  public static Byte getByteProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, Byte::parseByte);
  }

  /**
   * Retrieves {@link Short} property.
   *
   * @param countersReader countersReader
   * @param name property name
   * @return property value, or {@code null} if not found
   */
  public static Short getShortProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, Short::parseShort);
  }

  /**
   * Retrieves {@link Integer} property.
   *
   * @param countersReader countersReader
   * @param name property name
   * @return property value, or {@code null} if not found
   */
  public static Integer getIntProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, Integer::parseInt);
  }

  /**
   * Retrieves {@link Long} property.
   *
   * @param countersReader countersReader
   * @param name property name
   * @return property value, or {@code null} if not found
   */
  public static Long getLongProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, Long::parseLong);
  }

  /**
   * Retrieves {@link Double} property.
   *
   * @param countersReader countersReader
   * @param name property name
   * @return property value, or {@code null} if not found
   */
  public static Double getDoubleProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, Double::parseDouble);
  }

  /**
   * Retrieves enum property.
   *
   * @param countersReader countersReader
   * @param name property name
   * @return property value, or {@code null} if not found
   */
  public static <T extends Enum<T>> T getEnumProperty(
      CountersReader countersReader, String name, Function<String, T> enumFunc) {
    return getProperty(countersReader, name, enumFunc);
  }

  /**
   * Retrieves {@link String} property.
   *
   * @param countersReader countersReader
   * @param name property name
   * @return property value, or {@code null} if not found
   */
  public static String getProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, s -> s);
  }

  /**
   * Retrieves property from and converts it using provided function.
   *
   * @param countersReader countersReader
   * @param name property name
   * @param converter function to convert string value
   * @param <T> result type
   * @return converted property value, or {@code null} if not found
   */
  public static <T> T getProperty(
      CountersReader countersReader, String name, Function<String, T> converter) {
    final var counter = CounterDescriptor.findFirstCounter(countersReader, byPropertyName(name));
    return counter != null ? converter.apply(counter.label().split("=")[1]) : null;
  }

  /**
   * Returns predicate that matches counters of property type, and with given property name.
   *
   * @param name property name
   * @return predicate for filtering property counters by property name
   */
  public static Predicate<CounterDescriptor> byPropertyName(String name) {
    return byType(PropertiesRegistry.PROPERTY_COUNTER_TYPE_ID)
        .and(descriptor -> name.equals(descriptor.label().split("=")[0]));
  }
}
