package io.scalecube.metrics.mimir;

import static io.scalecube.metrics.MetricNames.sanitizeName;

import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import io.scalecube.metrics.Key;
import io.scalecube.metrics.KeyCodec;
import io.scalecube.metrics.mimir.MimirPublisher.WriteProxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prometheus.Remote.WriteRequest;
import prometheus.Types.Label;
import prometheus.Types.Sample;
import prometheus.Types.TimeSeries;
import prometheus.Types.TimeSeries.Builder;

/**
 * Handles counter updates and pushes them to Mimir via {@link WriteProxy}. Converts {@link
 * CounterDescriptor} data into Mimir {@link TimeSeries} with labels and values.
 */
public class CountersMimirHandler implements CountersHandler {

  private final Map<String, String> tags;
  private final WriteProxy writeProxy;

  private final KeyCodec keyCodec = new KeyCodec();

  /**
   * Constructor.
   *
   * @param tags tags (optional)
   * @param writeProxy writeProxy
   */
  public CountersMimirHandler(Map<String, String> tags, WriteProxy writeProxy) {
    this.tags = tags;
    this.writeProxy = writeProxy;
  }

  @Override
  public void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
    final var builder = WriteRequest.newBuilder();

    for (var descriptor : counterDescriptors) {
      final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
      final var visibility = key.stringValue("visibility");
      if (!"private".equals(visibility)) {
        final var name = descriptor.label() != null ? descriptor.label() : key.stringValue("name");
        if (name != null) {
          final var tags = toTags(key);
          final var value = descriptor.value();
          builder.addTimeseries(toTimeSeries(timestamp, name, tags, value));
        }
      }
    }

    if (builder.getTimeseriesCount() > 0) {
      writeProxy.push(builder.build());
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

  private static TimeSeries toTimeSeries(
      long timestamp, String name, Map<String, String> tags, long value) {
    final var builder =
        TimeSeries.newBuilder()
            .addLabels(Label.newBuilder().setName("__name__").setValue(sanitizeName(name)).build())
            .addSamples(Sample.newBuilder().setValue(value).setTimestamp(timestamp).build());
    addLabels(builder, tags);
    return builder.build();
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
