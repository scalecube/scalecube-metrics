package io.scalecube.metrics.mimir;

import static io.scalecube.metrics.MetricNames.sanitizeName;

import io.scalecube.metrics.Key;
import io.scalecube.metrics.KeyCodec;
import io.scalecube.metrics.MetricsHandler;
import io.scalecube.metrics.mimir.MimirPublisher.WriteProxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import prometheus.Remote.WriteRequest;
import prometheus.Types.Label;
import prometheus.Types.Sample;
import prometheus.Types.TimeSeries;
import prometheus.Types.TimeSeries.Builder;

/**
 * Handles metrics (histograms, tps values) and pushes them to Mimir via {@link WriteProxy}. Exports
 * percentiles, max, and count for histograms, and value snapshots for tps.
 */
public class MetricsMimirHandler implements MetricsHandler {

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

  private final Map<String, String> tags;
  private final WriteProxy writeProxy;

  private final KeyCodec keyCodec = new KeyCodec();

  /**
   * Constructor.
   *
   * @param tags tags (optional)
   * @param writeProxy writeProxy
   */
  public MetricsMimirHandler(Map<String, String> tags, WriteProxy writeProxy) {
    this.tags = tags;
    this.writeProxy = writeProxy;
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
    final var tags = toTags(key);

    if (name != null) {
      writeProxy.push(
          WriteRequest.newBuilder()
              .addAllTimeseries(toTimeSeriesList(timestamp, name, tags, conversionFactor, distinct))
              .build());
    }
  }

  @Override
  public void onTps(
      long timestamp, DirectBuffer keyBuffer, int keyOffset, int keyLength, long value) {
    final var key = keyCodec.decodeKey(keyBuffer, keyOffset);
    final var name = key.stringValue("name");
    final var tags = toTags(key);

    if (name != null) {
      writeProxy.push(
          WriteRequest.newBuilder()
              .addAllTimeseries(toTimeSeriesList(timestamp, name, tags, value))
              .build());
    }
  }

  private Map<String, String> toTags(Key key) {
    final var allTags = new HashMap<String, String>();
    if (tags != null) {
      allTags.putAll(tags);
    }

    for (var entry : key.tags().entrySet()) {
      final var tagId = entry.getKey();
      final var value = entry.getValue();
      allTags.put(tagId, String.valueOf(value));
    }

    return allTags;
  }

  private static List<TimeSeries> toTimeSeriesList(
      long timestamp,
      String name,
      Map<String, String> tags,
      double conversionFactor,
      Histogram histogram) {

    final var tsList = new ArrayList<TimeSeries>();

    // Percentile
    for (double percentile : PERCENTILES) {
      final var value = histogram.getValueAtPercentile(percentile) * conversionFactor;
      final var percentileBuilder =
          TimeSeries.newBuilder()
              .addLabels(Label.newBuilder().setName("__name__").setValue(name).build())
              .addLabels(
                  Label.newBuilder().setName("p").setValue(formatPercentile(percentile)).build())
              .addSamples(Sample.newBuilder().setValue(value).setTimestamp(timestamp).build());
      addLabels(percentileBuilder, tags);
      tsList.add(percentileBuilder.build());
    }

    // Max
    final var value = histogram.getMaxValue() * conversionFactor;
    final var maxBuilder =
        TimeSeries.newBuilder()
            .addLabels(Label.newBuilder().setName("__name__").setValue(name).build())
            .addLabels(Label.newBuilder().setName("p").setValue("max").build())
            .addSamples(Sample.newBuilder().setValue(value).setTimestamp(timestamp).build());
    addLabels(maxBuilder, tags);
    tsList.add(maxBuilder.build());

    // Count
    final var countBuilder =
        TimeSeries.newBuilder()
            .addLabels(Label.newBuilder().setName("__name__").setValue(name + "_count").build())
            .addSamples(
                Sample.newBuilder()
                    .setValue(histogram.getTotalCount())
                    .setTimestamp(timestamp)
                    .build());
    addLabels(countBuilder, tags);
    tsList.add(countBuilder.build());

    return tsList;
  }

  private static List<TimeSeries> toTimeSeriesList(
      long timestamp, String name, Map<String, String> tags, long value) {
    final var tsList = new ArrayList<TimeSeries>();

    final var tpsBuilder =
        TimeSeries.newBuilder()
            .addLabels(Label.newBuilder().setName("__name__").setValue(name).build())
            .addSamples(Sample.newBuilder().setValue(value).setTimestamp(timestamp).build());
    addLabels(tpsBuilder, tags);
    tsList.add(tpsBuilder.build());

    return tsList;
  }

  private static String formatPercentile(double value) {
    if (value == Math.floor(value)) {
      return String.valueOf((int) value);
    } else {
      return String.valueOf(value);
    }
  }

  private static void addLabels(Builder builder, Map<String, String> tags) {
    if (tags != null) {
      tags.forEach(
          (name, value) -> {
            if (!"name".equals(name)) {
              builder.addLabels(
                  Label.newBuilder()
                      .setName(sanitizeName(name))
                      .setValue(String.valueOf(value))
                      .build());
            }
          });
    }
  }
}
