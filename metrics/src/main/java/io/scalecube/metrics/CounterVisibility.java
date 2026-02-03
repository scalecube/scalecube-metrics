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

  public static CounterVisibility get(final byte value) {
    return switch (value) {
      case 1 -> PUBLIC;
      case 2 -> PRIVATE;
      case Byte.MIN_VALUE -> NULL_VAL;
      default -> throw new IllegalArgumentException("Unknown value: " + value);
    };
  }
}
