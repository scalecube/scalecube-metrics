package io.scalecube.metrics;

import org.agrona.DirectBuffer;

public interface HistogramMetric {

  DirectBuffer keyBuffer();

  void record(long value);
}
