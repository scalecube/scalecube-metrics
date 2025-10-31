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

public class PropertiesRegistry {

  public static final int PROPERTY_COUNTER_TYPE_ID = 2;

  private final CountersManager countersManager;

  private final ThreadLocal<CounterAllocator> counterAllocatorHolder;
  private final Map<String, AtomicCounter> counters = new ConcurrentHashMap<>();

  public PropertiesRegistry(CountersManager countersManager) {
    this.countersManager = countersManager;
    this.counterAllocatorHolder =
        ThreadLocal.withInitial(() -> new CounterAllocator(this.countersManager));
  }

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

  public static Byte getByteProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, Byte::parseByte);
  }

  public static Short getShortProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, Short::parseShort);
  }

  public static Integer getIntProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, Integer::parseInt);
  }

  public static Long getLongProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, Long::parseLong);
  }

  public static Double getDoubleProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, Double::parseDouble);
  }

  public static <T extends Enum<T>> T getEnumProperty(
      CountersReader countersReader, String name, Function<String, T> enumFunc) {
    return getProperty(countersReader, name, enumFunc);
  }

  public static String getProperty(CountersReader countersReader, String name) {
    return getProperty(countersReader, name, s -> s);
  }

  public static <T> T getProperty(
      CountersReader countersReader, String name, Function<String, T> converter) {
    final var counter = CounterDescriptor.findFirstCounter(countersReader, byPropertyName(name));
    return counter != null ? converter.apply(counter.label().split("=")[1]) : null;
  }

  public static Predicate<CounterDescriptor> byPropertyName(String name) {
    return byType(PropertiesRegistry.PROPERTY_COUNTER_TYPE_ID)
        .and(descriptor -> name.equals(descriptor.label().split("=")[0]));
  }
}
