package io.scalecube.metrics;

import io.scalecube.metrics.MetricsRecorder.Context;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;

public class MetricsReaderRunner {

  private static final double[] PERCENTILES =
      new double[] {
        10.0, // lower quantile – distribution shape
        50.0, // median
        90.0, // upper user experience
        95.0, // soft SLA
        99.0, // hard SLA
        99.5, // early jitter detection
        99.9, // jitter tail
        99.99, // rare stall detection
        99.999 // ghost-stall detection
      };

  public static void main(String[] args) {
    final var agent =
        new MetricsReaderAgent(
            "MetricsReaderAgent",
            new File(Context.DEFAULT_METRICS_DIR_NAME),
            true,
            SystemEpochClock.INSTANCE,
            Duration.ofSeconds(3),
            new MetricsHandler() {
              @Override
              public void onHistogram(
                  long timestamp,
                  DirectBuffer keyBuffer,
                  int keyOffset,
                  int keyLength,
                  Histogram accumulated,
                  Histogram distinct,
                  long highestTrackableValue,
                  double conversionFactor) {
                final var list = new ArrayList<String>();
                for (double percentile : PERCENTILES) {
                  list.add(
                      formatDouble(distinct.getValueAtPercentile(percentile) * conversionFactor));
                }
                System.out.println(timestamp + "| onHistogram: " + list);
              }
            });
    AgentRunner.startOnThread(
        new AgentRunner(new BackoffIdleStrategy(), Throwable::printStackTrace, null, agent));
  }

  private static String formatDouble(double upperBound) {
    return String.format("%.3f", upperBound);
  }
}
