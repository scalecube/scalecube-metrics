package io.scalecube.metrics;

import io.scalecube.metrics.MetricsRecorder.MetricsPublication;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.agrona.DeadlineTimerWheel;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.EpochClock;

class MetricsRecorderAgent implements Agent {

  private static final int TIMER_POLL_LIMIT = 10;
  private static final int TIMER_TICK_RESOLUTION = 8192;
  private static final int TIMER_TICKS_PER_WHEEL = 128;
  private static final long TPS_RESOLUTION = TimeUnit.SECONDS.toMillis(1);

  private final Queue<Object> queue;
  private final EpochClock epochClock;
  private final CachedEpochClock cachedEpochClock;
  private final MetricsPublication metricsPublication;

  private final MetricsEncoder metricsEncoder = new MetricsEncoder();
  private final Map<DirectBuffer, List<Object>> metrics = new Object2ObjectHashMap<>();
  private final Map<DirectBuffer, Object> aggregates = new Object2ObjectHashMap<>();
  private final Long2ObjectHashMap<DirectBuffer> scheduledTimers = new Long2ObjectHashMap<>();
  private final DeadlineTimerWheel timerWheel;

  MetricsRecorderAgent(
      Queue<Object> queue,
      EpochClock epochClock,
      CachedEpochClock cachedEpochClock,
      MetricsPublication metricsPublication) {
    this.queue = queue;
    this.epochClock = epochClock;
    this.cachedEpochClock = cachedEpochClock;
    this.metricsPublication = metricsPublication;
    timerWheel =
        new DeadlineTimerWheel(
            TimeUnit.MILLISECONDS, 0, TIMER_TICK_RESOLUTION, TIMER_TICKS_PER_WHEEL);
  }

  @Override
  public String roleName() {
    return "MetricsRecorderAgent";
  }

  @Override
  public int doWork() {
    final var time = epochClock.time();
    cachedEpochClock.update(time);

    final var metric = queue.poll();
    if (metric != null) {
      if (metric instanceof HistogramRecorder) {
        add((HistogramRecorder) metric);
      }
      if (metric instanceof TpsRecorder) {
        add((TpsRecorder) metric);
      }
    }

    return timerWheel.poll(time, this::onTimerExpiry, TIMER_POLL_LIMIT);
  }

  private void add(HistogramRecorder metric) {
    final var keyBuffer = metric.keyBuffer();
    final var list = metrics.computeIfAbsent(keyBuffer, k -> new ArrayList<>());
    list.add(metric);

    if (!aggregates.containsKey(keyBuffer)) {
      aggregates.put(
          keyBuffer,
          new HistogramAggregate(
              keyBuffer,
              metric.highestTrackableValue(),
              metric.conversionFactor(),
              metric.resolutionMs(),
              metricsEncoder,
              metricsPublication));
      schedule(keyBuffer, metric.resolutionMs());
    }
  }

  private void add(TpsRecorder metric) {
    final var keyBuffer = metric.keyBuffer();
    final var list = metrics.computeIfAbsent(keyBuffer, k -> new ArrayList<>());
    list.add(metric);

    if (!aggregates.containsKey(keyBuffer)) {
      aggregates.put(keyBuffer, new TpsAggregate(keyBuffer, metricsEncoder, metricsPublication));
      schedule(keyBuffer, TPS_RESOLUTION);
    }
  }

  private boolean onTimerExpiry(TimeUnit timeUnit, long time, long timerId) {
    final var keyBuffer = scheduledTimers.remove(timerId);
    if (keyBuffer == null) {
      return true;
    }
    final var aggregate = aggregates.get(keyBuffer);
    if (aggregate == null) {
      return true;
    }
    final var list = metrics.get(keyBuffer);
    if (list == null) {
      return true;
    }

    if (aggregate instanceof HistogramAggregate agg) {
      for (int i = 0; i < list.size(); i++) {
        ((HistogramRecorder) list.get(i)).swapAndUpdate(agg);
      }
      agg.publish(time);
      schedule(keyBuffer, agg.resolutionMs());
    }
    if (aggregate instanceof TpsAggregate agg) {
      for (int i = 0; i < list.size(); i++) {
        ((TpsRecorder) list.get(i)).swapAndUpdate(agg);
      }
      agg.publish(time);
      schedule(keyBuffer, TPS_RESOLUTION);
    }

    return true;
  }

  private void schedule(DirectBuffer keyBuffer, long resolutionMs) {
    final var deadline = cachedEpochClock.time() + resolutionMs;
    final var timerId = timerWheel.scheduleTimer(deadline);
    scheduledTimers.put(timerId, keyBuffer);
  }
}
