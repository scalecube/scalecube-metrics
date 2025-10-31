package io.scalecube.metrics;

import org.agrona.concurrent.EpochClock;

public class Delay {

  private final EpochClock epochClock;
  private final long defaultDelay;

  private long deadline;

  public Delay(EpochClock epochClock, long defaultDelay) {
    this.epochClock = epochClock;
    this.defaultDelay = defaultDelay;
  }

  public void delay() {
    delay(defaultDelay);
  }

  public void delay(long delay) {
    deadline = epochClock.time() + delay;
  }

  public boolean isOverdue() {
    final long now = epochClock.time();
    return now > deadline;
  }

  public boolean isNotOverdue() {
    final long now = epochClock.time();
    return now < deadline;
  }
}
