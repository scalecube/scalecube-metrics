package io.scalecube.metrics;

import io.aeron.CommonContext;
import io.scalecube.metrics.CountersRegistry.Context;
import io.scalecube.metrics.aeron.CncCountersReaderAgent;
import java.io.File;
import java.time.Duration;
import java.util.List;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.CompositeAgent;
import org.agrona.concurrent.SystemEpochClock;

public class CountersReaderRunner {

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
    final var cncCountersReaderAgent =
        new CncCountersReaderAgent(
            "CncCountersReaderAgent",
            CommonContext.getAeronDirectoryName(),
            true,
            SystemEpochClock.INSTANCE,
            Duration.ofSeconds(3),
            new CountersHandler() {
              @Override
              public void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
                System.out.println(timestamp + "| Aeron counters: " + counterDescriptors);
              }
            });

    final var countersReaderAgent =
        new CountersReaderAgent(
            "CountersReaderAgent",
            new File(Context.DEFAULT_COUNTERS_DIR_NAME),
            true,
            SystemEpochClock.INSTANCE,
            Duration.ofSeconds(3),
            new CountersHandler() {
              @Override
              public void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
                System.out.println(timestamp + "| Normal counters: " + counterDescriptors);
              }
            });

    final var agentRunner =
        new AgentRunner(
            new BackoffIdleStrategy(),
            Throwable::printStackTrace,
            null,
            new CompositeAgent(cncCountersReaderAgent, countersReaderAgent));
    AgentRunner.startOnThread(agentRunner);
  }
}
