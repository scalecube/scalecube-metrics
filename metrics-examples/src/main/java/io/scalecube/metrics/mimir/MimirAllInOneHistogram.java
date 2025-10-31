package io.scalecube.metrics.mimir;

import static org.testcontainers.utility.MountableFile.forClasspathResource;

import io.scalecube.metrics.KeyCodec;
import io.scalecube.metrics.MetricsHandler;
import io.scalecube.metrics.MetricsReaderAgent;
import io.scalecube.metrics.MetricsRecorder;
import io.scalecube.metrics.MetricsTransmitter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.xerial.snappy.Snappy;
import prometheus.Remote.WriteRequest;
import prometheus.Types;
import prometheus.Types.BucketSpan;
import prometheus.Types.Label;
import prometheus.Types.TimeSeries;

public class MimirAllInOneHistogram {

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
                new MimirHistogramHandler(pushUrl))));

    // Start measurements and burning cpu

    final var highestTrackableValue = (long) 1e9;
    final var conversionFactor = 1e-3;
    final var resolutionMs = 3000;
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

  private static class MimirHistogramHandler implements MetricsHandler {

    private final String pushUrl;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final KeyCodec keyCodec = new KeyCodec();

    private MimirHistogramHandler(String pushUrl) {
      this.pushUrl = pushUrl;
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
      final var key = keyCodec.decodeKey(keyBuffer, keyOffset);
      final var name = key.stringValue("name");

      final var histogram = buildPromNativeHistogram(accumulated, 8, timestamp);
      final var timeSeries = wrapIntoTimeSeries(name, histogram);
      try {
        push(timeSeries);
        System.out.println(Instant.now() + " | push(timeSeries)");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private static List<Double> computePromNativeBucketBoundaries(
        double minNs, double maxNs, int schema) {
      double min = minNs / 1000.0; // ns -> µs
      double max = maxNs / 1000.0; // ns -> µs
      double factor = Math.pow(2, Math.pow(2, -schema));
      List<Double> boundaries = new ArrayList<>();
      double current = min;
      while (current < max) {
        boundaries.add(current);
        current *= factor;
      }
      return boundaries;
    }

    private static List<Long> convertHdrToPromBuckets(Histogram hdr, List<Double> boundaries) {
      List<Long> bucketCounts = new ArrayList<>(boundaries.size());
      long cumulative = 0;
      for (double boundaryUs : boundaries) {
        long boundaryNs = (long) Math.ceil(boundaryUs * 1000); // µs -> ns for query
        long count = hdr.getCountBetweenValues(0, boundaryNs);
        bucketCounts.add(count - cumulative);
        cumulative = count;
      }
      return bucketCounts;
    }

    private static Types.Histogram buildPromNativeHistogram(
        Histogram hdr, int schema, long timestamp) {
      double minNs = hdr.getMinNonZeroValue();
      double maxNs = hdr.getMaxValue();

      List<Double> boundaries = computePromNativeBucketBoundaries(minNs, maxNs, schema);
      List<Long> bucketCounts = convertHdrToPromBuckets(hdr, boundaries);

      double totalSumNs = 0;
      for (HistogramIterationValue value : hdr.recordedValues()) {
        totalSumNs += value.getValueIteratedTo() * value.getCountAtValueIteratedTo();
      }
      double totalSumUs = totalSumNs / 1000.0;

      Types.Histogram.Builder builder =
          Types.Histogram.newBuilder()
              .setSchema(schema)
              .setCountInt(hdr.getTotalCount())
              .setZeroCountInt(0)
              .setSum(totalSumUs)
              .setTimestamp(timestamp);

      // Delta-encoded bucket counts
      List<Long> deltas = new ArrayList<>();
      List<BucketSpan> spans = new ArrayList<>();

      int baseBucket = 0;
      int spanStart = -1;
      int spanLength = 0;
      boolean inSpan = false;

      for (int i = 0; i < bucketCounts.size(); i++) {
        long count = bucketCounts.get(i);
        if (count > 0) {
          if (!inSpan) {
            spanStart = i;
            spanLength = 1;
            inSpan = true;
          } else {
            spanLength++;
          }
          deltas.add(count);
        } else {
          if (inSpan) {
            // Close the span
            spans.add(
                BucketSpan.newBuilder()
                    .setOffset(spanStart - baseBucket)
                    .setLength(spanLength)
                    .build());
            baseBucket = spanStart + spanLength;
            inSpan = false;
          }
        }
      }

      // If ended with a span
      if (inSpan) {
        spans.add(
            BucketSpan.newBuilder()
                .setOffset(spanStart - baseBucket)
                .setLength(spanLength)
                .build());
      }

      builder.addAllPositiveSpans(spans);
      builder.addAllPositiveDeltas(deltas);

      return builder.build();
    }

    private static TimeSeries wrapIntoTimeSeries(String name, Types.Histogram histogram) {
      TimeSeries.Builder builder = TimeSeries.newBuilder();
      builder.addLabels(Label.newBuilder().setName("__name__").setValue(name));
      builder.addLabels(Label.newBuilder().setName("service").setValue("hft_app"));
      builder.addHistograms(histogram);
      return builder.build();
    }

    private void push(TimeSeries timeSeries) throws Exception {
      byte[] payload = WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray();
      byte[] compressedPayload = Snappy.compress(payload);

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(pushUrl))
              .header("Content-Type", "application/x-protobuf")
              .header("Content-Encoding", "snappy")
              .header("X-Prometheus-Remote-Write-Version", "0.1.0")
              .POST(BodyPublishers.ofByteArray(compressedPayload))
              .build();

      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        System.err.println(
            "Failed to push metrics: HTTP " + response.statusCode() + ", body: " + response.body());
      }
    }
  }
}
