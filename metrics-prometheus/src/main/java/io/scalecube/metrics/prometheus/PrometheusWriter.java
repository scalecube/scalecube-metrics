package io.scalecube.metrics.prometheus;

import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Serializer of the state into the Prometheus text exposition format. Implementations of this
 * interface are expected to take their current snapshot of metrics and write them into the provided
 * {@link java.io.OutputStreamWriter}, following Prometheus text format conventions.
 */
public interface PrometheusWriter {

  /**
   * Writes current snapshot of metrics to the Prometheus stream.
   *
   * @param writer Prometheus output stream
   * @throws IOException in case of errors
   */
  void write(OutputStreamWriter writer) throws IOException;
}
