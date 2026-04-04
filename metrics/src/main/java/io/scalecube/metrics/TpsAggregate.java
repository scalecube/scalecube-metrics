package io.scalecube.metrics;

import org.agrona.DirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;

class TpsAggregate {

  private final DirectBuffer keyBuffer;
  private final MetricsEncoder encoder;
  private final BroadcastTransmitter metricsTransmitter;

  private final MutableLong counter = new MutableLong();

  TpsAggregate(
      DirectBuffer keyBuffer, MetricsEncoder encoder, BroadcastTransmitter metricsTransmitter) {
    this.keyBuffer = keyBuffer;
    this.encoder = encoder;
    this.metricsTransmitter = metricsTransmitter;
  }

  void update(long value) {
    counter.addAndGet(value);
  }

  void publish(long timestamp) {
    try {
      final var value = counter.get();
      final var length = encoder.encodeTps(timestamp, keyBuffer, value);
      metricsTransmitter.transmit(1, encoder.buffer(), 0, length);
    } finally {
      counter.set(0);
    }
  }
}
