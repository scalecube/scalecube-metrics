package io.scalecube.metrics;

import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;

/**
 * Callback interface for handling metrics (histograms, tps values). Being used as part of metric
 * processing functionality.
 *
 * @see MetricsReader
 */
public interface MetricsHandler {

  /**
   * Callback for handling histogram metric.
   *
   * @param timestamp timestamp
   * @param keyBuffer keyBuffer
   * @param keyOffset keyOffset
   * @param keyLength keyLength
   * @param accumulated accumulated histogram
   * @param distinct distinct histgoram
   * @param highestTrackableValue highestTrackableValue
   * @param conversionFactor conversionFactor
   */
  default void onHistogram(
      long timestamp,
      DirectBuffer keyBuffer,
      int keyOffset,
      int keyLength,
      Histogram accumulated,
      Histogram distinct,
      long highestTrackableValue,
      double conversionFactor) {
    // no-op
  }

  /**
   * Callback for handling tps metric.
   *
   * @param timestamp timestamp
   * @param keyBuffer keyBuffer
   * @param keyOffset keyOffset
   * @param keyLength keyLength
   * @param value value
   */
  default void onTps(
      long timestamp, DirectBuffer keyBuffer, int keyOffset, int keyLength, long value) {
    // no-op
  }
}
