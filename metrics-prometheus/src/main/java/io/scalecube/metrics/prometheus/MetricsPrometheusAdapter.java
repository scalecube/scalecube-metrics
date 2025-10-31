package io.scalecube.metrics.prometheus;

import static io.scalecube.metrics.MetricNames.sanitizeName;

import io.scalecube.metrics.Key;
import io.scalecube.metrics.KeyCodec;
import io.scalecube.metrics.MetricsHandler;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;

/**
 * Adapter that translates metrics (histograms, tps values) into the Prometheus text exposition
 * format. Metrics are being updated via the {@link MetricsHandler} interface and exposed to
 * Prometheus by implementing the {@link PrometheusWriter} contract.
 */
public class MetricsPrometheusAdapter implements MetricsHandler, PrometheusWriter {

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

  private final KeyCodec keyCodec = new KeyCodec();
  private final AtomicReference<Map<Key, HistogramItem>> histogramMapReference =
      new AtomicReference<>();
  private final AtomicReference<Map<Key, TpsItem>> tpsMapReference = new AtomicReference<>();

  /**
   * Constructor.
   *
   * @param tags (optional)
   */
  public MetricsPrometheusAdapter(Map<String, String> tags) {
    this.tags = tags;
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
      histogramMapReference.updateAndGet(
          map -> {
            if (map == null) {
              map = new LinkedHashMap<>();
            }
            map.put(
                key,
                new HistogramItem(
                    name, tags, accumulated, distinct, highestTrackableValue, conversionFactor));
            return map;
          });
    }
  }

  @Override
  public void onTps(
      long timestamp, DirectBuffer keyBuffer, int keyOffset, int keyLength, long value) {
    final var key = keyCodec.decodeKey(keyBuffer, keyOffset);
    final var name = key.stringValue("name");
    final var tags = toTags(key);

    if (name != null) {
      tpsMapReference.updateAndGet(
          map -> {
            if (map == null) {
              map = new LinkedHashMap<>();
            }
            map.put(key, new TpsItem(name, tags, value));
            return map;
          });
    }
  }

  @Override
  public void write(OutputStreamWriter writer) throws IOException {
    final var histogramMap = histogramMapReference.getAndSet(null);
    if (histogramMap != null) {
      for (var histogramItem : histogramMap.values()) {
        writeHistogram(writer, histogramItem);
      }
    }

    final var tpsMap = tpsMapReference.getAndSet(null);
    if (tpsMap != null) {
      for (var tpsItem : tpsMap.values()) {
        writeTps(writer, tpsItem);
      }
    }
  }

  private static void writeHistogram(OutputStreamWriter writer, HistogramItem histogramItem)
      throws IOException {
    final var name = histogramItem.name();
    final var conversionFactor = histogramItem.conversionFactor();
    final var histogram = histogramItem.distinct();
    final var tags = histogramItem.tags();

    // Percentile
    for (double percentile : PERCENTILES) {
      writer
          .append(sanitizeName(name))
          .append(formatLabels(addTag(tags, "p", formatPercentile(percentile))))
          .append(" ")
          .append(formatDouble(histogram.getValueAtPercentile(percentile) * conversionFactor))
          .append("\n");
    }

    // Max
    writer
        .append(sanitizeName(name))
        .append(formatLabels(addTag(tags, "p", "max")))
        .append(" ")
        .append(formatDouble(histogram.getMaxValue() * conversionFactor))
        .append("\n");

    // Count
    writer
        .append(sanitizeName(name + "_count"))
        .append(formatLabels(tags))
        .append(" ")
        .append(String.valueOf(histogram.getTotalCount()))
        .append("\n");
  }

  private static void writeTps(OutputStreamWriter writer, TpsItem tpsItem) throws IOException {
    final var name = tpsItem.name();
    final var tags = tpsItem.tags();
    final var value = tpsItem.value();

    writer
        .append(sanitizeName(name))
        .append(formatLabels(tags))
        .append(" ")
        .append(String.valueOf(value))
        .append("\n");
  }

  private static LinkedHashMap<String, String> addTag(
      Map<String, String> tags, String name, String value) {
    final var map = new LinkedHashMap<>(tags);
    map.put(name, value);
    return map;
  }

  private static String formatLabels(Map<String, String> labels) {
    if (labels == null || labels.isEmpty()) {
      return "";
    }

    return labels.entrySet().stream()
        .filter(entry -> !"name".equals(entry.getKey()))
        .map(
            entry ->
                sanitizeName(entry.getKey()) + "=\"" + escapeLabelValue(entry.getValue()) + "\"")
        .collect(Collectors.joining(",", "{", "}"));
  }

  private static String escapeLabelValue(String value) {
    // Escape backslashes and quotes as per Prometheus spec
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static String formatPercentile(double value) {
    if (value == Math.floor(value)) {
      return String.valueOf((int) value);
    } else {
      return String.valueOf(value);
    }
  }

  private static String formatDouble(double upperBound) {
    return String.format("%.3f", upperBound);
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

  private record HistogramItem(
      String name,
      Map<String, String> tags,
      Histogram accumulated,
      Histogram distinct,
      long highestTrackableValue,
      double conversionFactor) {}

  private record TpsItem(String name, Map<String, String> tags, long value) {}
}
