package io.scalecube.metrics.prometheus;

import static io.scalecube.metrics.MetricNames.sanitizeName;

import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import io.scalecube.metrics.Key;
import io.scalecube.metrics.KeyCodec;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Adapter that translates counters (backed by {@link org.agrona.concurrent.status.AtomicCounter})
 * into the Prometheus text exposition format. Counters are being updated via the {@link
 * CountersHandler} interface and exposed to Prometheus by implementing the {@link PrometheusWriter}
 * contract.
 */
public class CountersPrometheusAdapter implements CountersHandler, PrometheusWriter {

  private final Map<String, String> tags;

  private final KeyCodec keyCodec = new KeyCodec();
  private final AtomicReference<List<CounterDescriptor>> counterDescriptorsReference =
      new AtomicReference<>();

  /**
   * Constructor.
   *
   * @param tags (optional)
   */
  public CountersPrometheusAdapter(Map<String, String> tags) {
    this.tags = tags;
  }

  @Override
  public void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
    counterDescriptorsReference.set(List.copyOf(counterDescriptors));
  }

  @Override
  public void write(OutputStreamWriter writer) throws IOException {
    final var counterDescriptors = counterDescriptorsReference.getAndSet(null);
    if (counterDescriptors == null) {
      return;
    }

    for (var descriptor : counterDescriptors) {
      final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
      final var name = descriptor.label() != null ? descriptor.label() : key.stringValue("name");
      if (name != null) {
        writer
            .append(sanitizeName(name))
            .append(formatLabels(toTags(key)))
            .append(" ")
            .append(String.valueOf(descriptor.value()))
            .append("\n");
      }
    }
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
}
