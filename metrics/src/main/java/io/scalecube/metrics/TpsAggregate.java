package io.scalecube.metrics;

import io.scalecube.metrics.MetricsRecorder.MetricsPublication;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableLong;

class TpsAggregate {

  private final DirectBuffer keyBuffer;
  private final MetricsEncoder encoder;
  private final MetricsPublication metricsPublication;

  private final MutableLong counter = new MutableLong();

  TpsAggregate(
      DirectBuffer keyBuffer, MetricsEncoder encoder, MetricsPublication metricsPublication) {
    this.keyBuffer = keyBuffer;
    this.encoder = encoder;
    this.metricsPublication = metricsPublication;
  }

  void update(long value) {
    counter.addAndGet(value);
  }

  void publish(long timestamp) {
    try {
      final var value = counter.get();
      final var length = encoder.encodeTps(timestamp, keyBuffer, value);
      metricsPublication.publish(encoder.buffer(), 0, length);
    } finally {
      counter.set(0);
    }
  }
}
