package io.scalecube.metrics;

public enum CounterVisibility {
  PUBLIC((byte) 1),
  PRIVATE((byte) 2),
  NULL_VAL(Byte.MIN_VALUE);

  private final byte value;

  CounterVisibility(byte value) {
    this.value = value;
  }

  public int value() {
    return value;
  }

  /**
   * Returns the visibility for the given encoded value, throwing when it is not one of the known
   * values. Use {@link #find(byte)} where an unknown value must be tolerated rather than rejected.
   *
   * @param value encoded value
   * @return matching visibility
   * @throws IllegalArgumentException if the value is not a known visibility
   */
  public static CounterVisibility get(final byte value) {
    final var visibility = find(value);
    if (visibility == null) {
      throw new IllegalArgumentException("Unknown value: " + value);
    }
    return visibility;
  }

  /**
   * Looks up the visibility for the given encoded value, returning null when it is not one of the
   * known values. Unlike {@link #get(byte)} this never throws, so an out-of-process reader can
   * render a counters file written by a newer or a corrupted writer without failing.
   *
   * @param value encoded value
   * @return matching visibility, or null
   */
  public static CounterVisibility find(final byte value) {
    return switch (value) {
      case 1 -> PUBLIC;
      case 2 -> PRIVATE;
      case Byte.MIN_VALUE -> NULL_VAL;
      default -> null;
    };
  }
}
