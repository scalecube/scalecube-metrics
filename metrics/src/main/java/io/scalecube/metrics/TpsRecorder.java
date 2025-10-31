package io.scalecube.metrics;

import org.agrona.DirectBuffer;
import org.agrona.collections.MutableLong;

class TpsRecorder implements TpsMetric {

  private final DirectBuffer keyBuffer;

  private volatile MutableLong current;
  private volatile MutableLong swap;

  TpsRecorder(DirectBuffer keyBuffer) {
    this.keyBuffer = keyBuffer;
    current = new MutableLong();
    swap = new MutableLong();
  }

  @Override
  public DirectBuffer keyBuffer() {
    return keyBuffer;
  }

  @Override
  public void record() {
    current.increment();
  }

  void swapAndUpdate(TpsAggregate aggregate) {
    final var counter = current;
    current = swap;
    swap = counter;
    aggregate.update(counter.get());
    counter.set(0);
  }
}
