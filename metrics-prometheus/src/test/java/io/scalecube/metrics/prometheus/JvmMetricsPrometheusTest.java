package io.scalecube.metrics.prometheus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.metrics.jvm.JvmMetricsReaderAgent;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.agrona.concurrent.SystemEpochClock;
import org.junit.jupiter.api.Test;

// Typical wiring of an external scraper: JvmMetricsReaderAgent -> CountersPrometheusAdapter(tags)
class JvmMetricsPrometheusTest {

  private static final Map<String, String> POD_TAGS =
      Map.of("POD", "app-0", "APP", "app", "NAMESPACE", "default");
  private static final Pattern LINE = Pattern.compile("(\\w+)\\{(.*)} (-?\\d+)");
  private static final Pattern LABEL = Pattern.compile("(\\w+)=\"([^\"]*)\",?");

  @Test
  void writesJvmMetricsOfThisJvm() throws Exception {
    final var adapter = new CountersPrometheusAdapter(POD_TAGS);
    final var agent =
        new JvmMetricsReaderAgent(
            "jvm-" + ProcessHandle.current().pid(),
            new File(
                "/tmp/hsperfdata_" + System.getProperty("user.name"),
                String.valueOf(ProcessHandle.current().pid())),
            true,
            SystemEpochClock.INSTANCE,
            Duration.ofSeconds(3),
            adapter);
    agent.onStart();
    agent.doWork();

    final var output = new ByteArrayOutputStream();
    try (var writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
      adapter.write(writer);
    }

    final var names = new HashSet<String>();
    for (var line : output.toString(StandardCharsets.UTF_8).split("\n")) {
      final var matcher = LINE.matcher(line);
      assertTrue(matcher.matches(), "not a prometheus sample: " + line);
      final var name = matcher.group(1);
      final var labels = labels(matcher.group(2));
      final var gc = name.startsWith("jvm_gc_");

      final var expectedLabels = new HashMap<String, String>();
      POD_TAGS.forEach((tag, value) -> expectedLabels.put(tag.toLowerCase(), value));
      if (gc) {
        expectedLabels.put("gc", labels.get("gc"));
      }
      assertEquals(expectedLabels, labels, line);
      assertTrue(Long.parseLong(matcher.group(3)) >= 0, line);
      names.add(name);
    }

    assertEquals(
        Set.of(
            "jvm_safepoints_total",
            "jvm_safepoint_time_nanoseconds_total",
            "jvm_safepoint_sync_time_nanoseconds_total",
            "jvm_gc_collections_total",
            "jvm_gc_pause_nanoseconds_total",
            "jvm_memory_heap_used_bytes",
            "jvm_memory_heap_committed_bytes",
            "jvm_memory_heap_max_bytes",
            "jvm_threads_live",
            "jvm_threads_daemon",
            "jvm_threads_peak",
            "jvm_threads_started_total"),
        names);
  }

  private static Map<String, String> labels(String text) {
    final var labels = new HashMap<String, String>();
    final var matcher = LABEL.matcher(text);
    while (matcher.find()) {
      labels.put(matcher.group(1), matcher.group(2));
    }
    return labels;
  }
}
