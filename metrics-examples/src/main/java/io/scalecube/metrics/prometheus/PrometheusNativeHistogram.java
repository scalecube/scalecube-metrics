package io.scalecube.metrics.prometheus;

import static org.testcontainers.utility.MountableFile.forClasspathResource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.scalecube.metrics.MetricsHandler;
import io.scalecube.metrics.MetricsReaderAgent;
import io.scalecube.metrics.MetricsRecorder;
import io.scalecube.metrics.MetricsTransmitter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class PrometheusNativeHistogram {

  public static void main(String[] args) throws IOException {
    Network network = Network.newNetwork();

    GenericContainer<?> prometheus =
        new GenericContainer<>("prom/prometheus")
            .withNetwork(network)
            .withNetworkAliases("prometheus")
            .withExposedPorts(9090)
            .withCopyFileToContainer(
                forClasspathResource("prometheus.yml"), "/etc/prometheus/prometheus.yml")
            .withCommand(
                "--config.file=/etc/prometheus/prometheus.yml",
                "--enable-feature=native-histograms",
                "--log.level=debug");
    prometheus.start();

    // Start Grafana container
    GenericContainer<?> grafana =
        new GenericContainer<>("grafana/grafana")
            .withNetwork(network)
            .withExposedPorts(3000)
            .withEnv("GF_SECURITY_ADMIN_USER", "user")
            .withEnv("GF_SECURITY_ADMIN_PASSWORD", "password")
            .withCopyFileToContainer(
                forClasspathResource("prometheus.datasource.yml"),
                "/etc/grafana/provisioning/datasources/datasource.yml");
    grafana.start();

    String grafanaUrl = "http://" + grafana.getHost() + ":" + grafana.getMappedPort(3000);
    System.out.println("Started prometheus on: " + prometheus.getMappedPort(9090));
    System.out.println("Grafana is available at: " + grafanaUrl);

    // Start scrape target

    final var socketAddress = new InetSocketAddress(8080);
    final var server = HttpServer.create(socketAddress, 0);
    final var metricsHandlerAdapter = new MetricsHandlerAdapter("hft_latency", "HFT latency");

    server.createContext("/metrics", metricsHandlerAdapter);
    server.setExecutor(null); // Use default executor
    server.start();
    System.out.println(Instant.now() + " | Server started on " + socketAddress);

    final var metricsRecorder = MetricsRecorder.launch();
    final var metricsTransmitter = MetricsTransmitter.launch();

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
                metricsHandlerAdapter)));

    // Start measurements and burning cpu

    final var highestTrackableValue = (long) 1e9;
    final var conversionFactor = 1e-3;
    final var resolutionMs = 1000;
    final var latencyMetric =
        metricsRecorder.newHistogram(
            keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", "hft_latency"),
            highestTrackableValue,
            conversionFactor,
            resolutionMs);

    for (; ; ) {
      final var now = System.nanoTime();
      burnCpuMicros(20);
      final var delta = System.nanoTime() - now;
      latencyMetric.record(delta);
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

  private static class MetricsHandlerAdapter implements MetricsHandler, HttpHandler {

    private final String metricName;
    private final String help;

    private final AtomicReference<Histogram> latestHistogram = new AtomicReference<>();

    public MetricsHandlerAdapter(String metricName, String help) {
      this.metricName = metricName;
      this.help = help;
    }

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
      final var latest = latestHistogram.get();
      if (latest != null) {
        latest.add(distinct);
        latestHistogram.set(latest);
      } else {
        latestHistogram.set(distinct);
      }
    }

    @Override
    public void handle(HttpExchange exchange) {
      try {
        Histogram histogram = latestHistogram.getAndSet(null);
        StringBuilder sb = new StringBuilder();

        sb.append("# HELP ").append(metricName).append(" ").append(help).append("\n");
        sb.append("# TYPE ").append(metricName).append(" histogram\n");

        if (histogram != null && histogram.getTotalCount() > 0) {
          System.out.println(Instant.now() + " | Scrape histogram");

          final long totalCount = histogram.getTotalCount();
          double totalSum = 0.0;
          long cumulativeCount = 0;

          for (var v : histogram.recordedValues()) {
            long rawValue = v.getValueIteratedTo();
            long count = v.getCountAtValueIteratedTo();
            double upperBound = rawValue / 1000.0; // nanos to micros

            cumulativeCount += count;
            totalSum += upperBound * count; // reuse upperBound here

            sb.append(metricName)
                .append("{native=\"true\",scale=\"3\",le=\"")
                .append(formatDouble(upperBound))
                .append("\"} ")
                .append(cumulativeCount)
                .append("\n");
          }

          // +Inf bucket
          sb.append(metricName)
              .append("{native=\"true\",scale=\"3\",le=\"+Inf\"} ")
              .append(totalCount)
              .append("\n");

          sb.append(metricName).append("_sum ").append(formatDouble(totalSum)).append("\n");
          sb.append(metricName).append("_count ").append(totalCount).append("\n");
        }

        sb.append("# EOF\n");

        byte[] response = sb.toString().getBytes();
        exchange
            .getResponseHeaders()
            .set("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8");
        exchange.sendResponseHeaders(200, response.length);

        try (OutputStream os = exchange.getResponseBody()) {
          os.write(response);
        }
      } catch (Exception e) {
        e.printStackTrace(System.err);
        try {
          exchange.sendResponseHeaders(500, -1);
        } catch (Exception ex) {
          // ignore
        }
      }
    }

    private static String formatDouble(double upperBound) {
      return String.format("%.3f", upperBound);
    }
  }
}
