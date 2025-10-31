package io.scalecube.metrics;

import static io.scalecube.metrics.HistogramRecorder.NUMBER_OF_SIGNIFICANT_VALUE_DIGITS;

import io.scalecube.metrics.MetricsRecorder.MetricsPublication;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;

class HistogramAggregate {

  private final DirectBuffer keyBuffer;
  private final long highestTrackableValue;
  private final double conversionFactor;
  private final long resolutionMs;
  private final MetricsEncoder encoder;
  private final MetricsPublication metricsPublication;

  private final Histogram accumulated;
  private final Histogram distinct;

  HistogramAggregate(
      DirectBuffer keyBuffer,
      long highestTrackableValue,
      double conversionFactor,
      long resolutionMs,
      MetricsEncoder encoder,
      MetricsPublication metricsPublication) {
    this.keyBuffer = keyBuffer;
    this.highestTrackableValue = highestTrackableValue;
    this.conversionFactor = conversionFactor;
    this.resolutionMs = resolutionMs;
    this.encoder = encoder;
    this.metricsPublication = metricsPublication;
    accumulated = new Histogram(highestTrackableValue, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
    distinct = new Histogram(highestTrackableValue, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
  }

  long resolutionMs() {
    return resolutionMs;
  }

  void update(Histogram value) {
    accumulated.add(value);
    distinct.add(value);
  }

  void publish(long timestamp) {
    try {
      final var length =
          encoder.encodeHistogram(
              timestamp, keyBuffer, accumulated, distinct, highestTrackableValue, conversionFactor);
      metricsPublication.publish(encoder.buffer(), 0, length);
    } finally {
      distinct.reset();
    }
  }
}
