package io.scalecube.metrics;

import java.util.Map;
import java.util.function.Function;

/**
 * Represents key object used in metrics/counters composed of a map of tag-value pairs. Each tag
 * maps to a typed value which can be accessed via helper methods. Supposed to be used on
 * metrics/counters consumption side.
 */
public record Key(Map<String, Object> tags) {

  /**
   * Gets {@code Byte} value for the specified tag.
   *
   * @param tag tag name
   * @return {@code Byte} value, or null
   */
  public Byte byteValue(String tag) {
    return (Byte) tags.get(tag);
  }

  /**
   * Gets {@code Short} value for the specified tag.
   *
   * @param tag tag name
   * @return {@code Short} value, or null
   */
  public Short shortValue(String tag) {
    return (Short) tags.get(tag);
  }

  /**
   * Gets {@code Integer} value for the specified tag.
   *
   * @param tag tag name
   * @return {@code Integer} value, or null
   */
  public Integer intValue(String tag) {
    return (Integer) tags.get(tag);
  }

  /**
   * Gets {@code Long} value for the specified tag.
   *
   * @param tag tag name
   * @return {@code Long} value, or null
   */
  public Long longValue(String tag) {
    return (Long) tags.get(tag);
  }

  /**
   * Gets {@code Double} value for the specified tag.
   *
   * @param tag tag name
   * @return {@code Double} value, or null
   */
  public Double doubleValue(String tag) {
    return (Double) tags.get(tag);
  }

  /**
   * Gets {@code String} value for the specified tag.
   *
   * @param tag tag name
   * @return {@code String} value, or null
   */
  public String stringValue(String tag) {
    return (String) tags.get(tag);
  }

  /**
   * Gets an {@code Enum} value from byte representation for the specified tag.
   *
   * @param tag tag name
   * @param enumFunc function to convert the byte value to enum
   * @return enum value, or null
   */
  public <T extends Enum<T>> T enumValue(String tag, Function<Byte, T> enumFunc) {
    final var byteValue = byteValue(tag);
    return byteValue != null ? enumFunc.apply(byteValue) : null;
  }
}
