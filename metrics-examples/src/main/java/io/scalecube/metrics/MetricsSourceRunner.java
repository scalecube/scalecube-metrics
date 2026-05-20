package io.scalecube.metrics;

import org.agrona.concurrent.BackoffIdleStrategy;

public class MetricsSourceRunner {

  public static void main(String[] args) {
    final var metricsRecorder = MetricsRecorder.launch();
    System.out.println(metricsRecorder.context().metricsDir());

    final var highestTrackableValue = (long) 1e9;
    final var conversionFactor = 1e-3;
    final var resolutionMs = 1000;

    final var histogramMetric =
        metricsRecorder.newHistogram(
            keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", "latency"),
            highestTrackableValue,
            conversionFactor,
            resolutionMs);

    final var idleStrategy = new BackoffIdleStrategy();
    while (true) {
      final var s = System.nanoTime();
      burnCpuMicros(20);
      final var delta = System.nanoTime() - s;
      histogramMetric.record(delta);
      // Idle
      idleStrategy.idle();
    }
  }

  private static void burnCpuMicros(long micros) {
    long durationNanos = micros * 1000;
    long start = System.nanoTime();
    while ((System.nanoTime() - start) < durationNanos) {
      Thread.onSpinWait();
    }
  }
}
