package io.scalecube.metrics;

import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;

class HistogramRecorder implements HistogramMetric {

  static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 3;

  private final DirectBuffer keyBuffer;
  private final long highestTrackableValue;
  private final double conversionFactor;
  private final long resolutionMs;

  private volatile Histogram current;
  private volatile Histogram swap;

  HistogramRecorder(
      DirectBuffer keyBuffer,
      long highestTrackableValue,
      double conversionFactor,
      long resolutionMs) {
    this.keyBuffer = keyBuffer;
    this.highestTrackableValue = highestTrackableValue;
    this.conversionFactor = Math.round(conversionFactor * 1000.0) / 1000.0;
    this.resolutionMs = resolutionMs;
    current = new Histogram(highestTrackableValue, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
    swap = new Histogram(highestTrackableValue, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
  }

  @Override
  public DirectBuffer keyBuffer() {
    return keyBuffer;
  }

  @Override
  public void record(long value) {
    final var histogram = current;
    histogram.recordValue(Math.min(value, highestTrackableValue));
  }

  long highestTrackableValue() {
    return highestTrackableValue;
  }

  double conversionFactor() {
    return conversionFactor;
  }

  long resolutionMs() {
    return resolutionMs;
  }

  void swapAndUpdate(HistogramAggregate aggregate) {
    final var value = current;
    current = swap;
    swap = value;
    aggregate.update(value);
    value.reset();
  }
}
