package io.scalecube.metrics;

import org.agrona.DirectBuffer;

public interface TpsMetric {

  DirectBuffer keyBuffer();

  void record();
}
