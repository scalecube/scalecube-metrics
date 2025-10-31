package io.scalecube.metrics.mimir;

import static org.testcontainers.utility.MountableFile.forClasspathResource;

import io.scalecube.metrics.MetricsReaderAgent;
import io.scalecube.metrics.MetricsRecorder;
import io.scalecube.metrics.MetricsTransmitter;
import java.time.Duration;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class MimirAllInOnePercentiles {

  public static void main(String[] args) {
    Network network = Network.newNetwork();

    final var mimir =
        new GenericContainer<>("grafana/mimir")
            .withExposedPorts(9009)
            .withNetwork(network)
            .withNetworkAliases("mimir")
            .withCopyFileToContainer(forClasspathResource("mimir.yml"), "/etc/mimir.yml")
            .withCommand("-config.file=/etc/mimir.yml", "-target=all", "-log.level=debug")
            .withLogConsumer(
                outputFrame -> System.err.print("[mimir] " + outputFrame.getUtf8String()));
    mimir.start();

    // Start Grafana container
    GenericContainer<?> grafana =
        new GenericContainer<>("grafana/grafana")
            .withExposedPorts(3000)
            .withNetwork(network)
            .withNetworkAliases("grafana")
            .withEnv("GF_SECURITY_ADMIN_USER", "user")
            .withEnv("GF_SECURITY_ADMIN_PASSWORD", "password")
            .withCopyFileToContainer(
                forClasspathResource("mimir.datasource.yml"),
                "/etc/grafana/provisioning/datasources/datasource.yml");
    grafana.start();

    final var mimirPort = mimir.getMappedPort(9009);
    final var pushUrl = "http://" + mimir.getHost() + ":" + mimirPort + "/api/v1/push";

    String grafanaUrl = "http://" + grafana.getHost() + ":" + grafana.getMappedPort(3000);
    System.out.println("Started Mimir on: " + mimirPort + " | pushUrl: " + pushUrl);
    System.out.println("Grafana is available at: " + grafanaUrl);

    final var metricsRecorder = MetricsRecorder.launch();
    final var metricsTransmitter = MetricsTransmitter.launch();
    final var mimirPublisher = MimirPublisher.launch(new MimirPublisher.Context().url(pushUrl));

    // Metrics

    AgentRunner.startOnThread(
        new AgentRunner(
            new BackoffIdleStrategy(),
            Throwable::printStackTrace,
            null,
            new MetricsReaderAgent(
                "MetricsReaderAgent",
                metricsTransmitter.context().broadcastBuffer(),
                SystemEpochClock.INSTANCE,
                Duration.ofSeconds(3),
                new MetricsMimirHandler(null, mimirPublisher.proxy()))));

    // Start measurements and burning cpu

    final var highestTrackableValue = (long) 1e9;
    final var conversionFactor = 1e-3;
    final var resolutionMs = 10000;
    final var latencyMetric =
        metricsRecorder.newHistogram(
            keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", "hft_latency"),
            highestTrackableValue,
            conversionFactor,
            resolutionMs);

    for (; ; ) {
      final var now = System.nanoTime();
      burnCpuMicros(20);
      latencyMetric.record(System.nanoTime() - now);
      Thread.onSpinWait();
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
