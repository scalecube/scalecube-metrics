package io.scalecube.metrics.aeron;

import org.agrona.DirectBuffer;

public interface KeyConverter {

  /**
   * Converts an Aeron counter key into a metrics key.
   *
   * @param keyBuffer the counter's key, valid only during this call
   * @param label the counter's label
   * @return the metrics key, or {@code null} to skip the counter
   */
  DirectBuffer convert(DirectBuffer keyBuffer, String label);
}
