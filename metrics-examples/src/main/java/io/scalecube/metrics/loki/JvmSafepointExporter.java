package io.scalecube.metrics.loki;

import static io.scalecube.metrics.MetricNames.sanitizeName;

import io.scalecube.metrics.Delay;
import io.scalecube.metrics.loki.LokiPublisher.WriteProxy;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: more work is needed: on restart can process gc.log that was already processed
public class JvmSafepointExporter implements Agent {

  private static final Logger LOGGER = LoggerFactory.getLogger(JvmSafepointExporter.class);

  private static final Duration READ_INTERVAL = Duration.ofSeconds(1);
  private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

  private static final DateTimeFormatter GC_LOG_TIMESTAMP_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

  private static final Pattern SAFEPOINT_PATTERN =
      Pattern.compile(
          "\\[(?<timestamp>[^]]+)] Safepoint \"(?<reason>[^\"]+)\", "
              + "Time since last: (?<sinceLast>\\d+) ns, "
              + "Reaching safepoint: (?<reaching>\\d+) ns, "
              + "Cleanup: (?<cleanup>\\d+) ns, "
              + "At safepoint: (?<at>\\d+) ns, "
              + "Total: (?<total>\\d+) ns");

  public enum State {
    INIT,
    RUNNING,
    CLEANUP,
    CLOSED
  }

  private final File gcLogDir;
  private final Map<String, String> labels;
  private final WriteProxy writeProxy;
  private final AgentInvoker publisherInvoker;

  private final Delay retryInterval;
  private final Delay readInterval;
  private FileChannel fileChannel;
  private final ByteBuffer chunkBuffer = ByteBuffer.allocate(DEFAULT_CHUNK_SIZE);
  private final StringBuilder lineBuffer = new StringBuilder();
  private State state = State.CLOSED;

  public JvmSafepointExporter(
      File gcLogDir,
      Map<String, String> labels,
      WriteProxy writeProxy,
      AgentInvoker publisherInvoker,
      EpochClock epochClock,
      Duration retryInterval) {
    this.gcLogDir = gcLogDir;
    this.labels = labels;
    this.writeProxy = writeProxy;
    this.publisherInvoker = publisherInvoker;
    this.retryInterval = new Delay(epochClock, retryInterval.toMillis());
    this.readInterval = new Delay(epochClock, READ_INTERVAL.toMillis());
  }

  @Override
  public String roleName() {
    return "JvmSafepointExporter";
  }

  @Override
  public void onStart() {
    if (state != State.CLOSED) {
      throw new AgentTerminationException("Illegal state: " + state);
    }
    state(State.INIT);
  }

  @Override
  public int doWork() throws Exception {
    try {
      if (publisherInvoker != null) {
        publisherInvoker.invoke();
      }
      return switch (state) {
        case INIT -> init();
        case RUNNING -> running();
        case CLEANUP -> cleanup();
        default -> throw new AgentTerminationException("Unknown state: " + state);
      };
    } catch (AgentTerminationException e) {
      throw e;
    } catch (Exception e) {
      state(State.CLEANUP);
      throw e;
    }
  }

  private int init() throws IOException {
    if (retryInterval.isNotOverdue()) {
      return 0;
    }

    // -Xlog:gc*,safepoint:$LOGS_DIR/$TS-$SERVICE_NAME-gc.log \

    final var filePath = findLatestGcLog(gcLogDir.toPath());
    if (!Files.exists(filePath) || Files.isDirectory(filePath)) {
      throw new IllegalArgumentException("Wrong file: " + filePath);
    }

    fileChannel = FileChannel.open(filePath);

    state(State.RUNNING);
    return 1;
  }

  private static Path findLatestGcLog(Path dir) throws IOException {
    try (var files = Files.list(dir)) {
      return files
          .filter(Files::isRegularFile)
          .filter(p -> p.getFileName().toString().contains("gc.log"))
          .max(Comparator.comparingLong(p -> p.toFile().lastModified()))
          .orElseThrow(() -> new FileNotFoundException("No matching gc.log files found in " + dir));
    }
  }

  private int running() throws IOException {
    if (readInterval.isOverdue()) {
      final int read = fileChannel.read(chunkBuffer.clear());
      if (read > 0) {
        final byte[] bytes = new byte[chunkBuffer.flip().remaining()];
        chunkBuffer.get(bytes);
        lineBuffer.append(new String(bytes, StandardCharsets.UTF_8));
      } else {
        readInterval.delay();
      }
    }

    int workCount = 0;
    final var events = new ArrayList<SafepointEvent>();
    int lineEnd;
    while ((lineEnd = lineBuffer.indexOf("\n")) >= 0) {
      String line = lineBuffer.substring(0, lineEnd).trim();
      lineBuffer.delete(0, lineEnd + 1);
      final var event = processLine(line);
      if (event != null) {
        workCount++;
        events.add(event);
      }
    }

    if (!events.isEmpty()) {
      writeProxy.push(toWriteRequest(events));
    }

    return workCount;
  }

  private static SafepointEvent processLine(String line) {
    final var matcher = SAFEPOINT_PATTERN.matcher(line);
    if (!matcher.find()) {
      return null;
    }
    return new SafepointEvent(
        ZonedDateTime.parse(matcher.group("timestamp"), GC_LOG_TIMESTAMP_FORMATTER).toInstant(),
        matcher.group("reason"),
        Long.parseLong(matcher.group("sinceLast")),
        Long.parseLong(matcher.group("reaching")),
        Long.parseLong(matcher.group("cleanup")),
        Long.parseLong(matcher.group("at")),
        Long.parseLong(matcher.group("total")));
  }

  private WriteRequest toWriteRequest(List<SafepointEvent> events) {
    final var streamLabels = streamLabels(labels);
    streamLabels.put("metric_name", "jvm_safepoint");

    final var values = events.stream().map(JvmSafepointExporter::toLogEntry).toList();

    return new WriteRequest(List.of(new WriteRequest.Stream(streamLabels, values)));
  }

  private static Map<String, String> streamLabels(Map<String, String> map) {
    final var labels = new HashMap<String, String>();
    if (map != null) {
      map.forEach((key, value) -> labels.put(sanitizeName(key), value));
    }
    return labels;
  }

  private static String[] toLogEntry(SafepointEvent event) {
    final var ts = event.timestamp();
    final var timestamp =
        String.valueOf(TimeUnit.SECONDS.toNanos(ts.getEpochSecond()) + ts.getNano());

    final var logLine =
        String.format(
            "reason=\"%s\" sinceLast=%.3f reaching=%.3f cleanup=%.3f at=%.3f total=%.3f",
            event.reason(),
            toMicros(event.sinceLastNs()),
            toMicros(event.reachingNs()),
            toMicros(event.cleanupNs()),
            toMicros(event.atSafepointNs()),
            toMicros(event.totalNs()));

    return new String[] {timestamp, logLine};
  }

  private static double toMicros(long nanos) {
    return nanos / (double) TimeUnit.MICROSECONDS.toNanos(1);
  }

  private int cleanup() {
    CloseHelper.quietClose(fileChannel);
    lineBuffer.setLength(0);

    State previous = state;
    if (previous != State.CLOSED) { // when it comes from onClose()
      retryInterval.delay();
      state(State.INIT);
    }
    return 1;
  }

  @Override
  public void onClose() {
    state(State.CLOSED);
    cleanup();
  }

  private void state(State state) {
    LOGGER.debug("[{}][state] {}->{}", roleName(), this.state, state);
    this.state = state;
  }

  record SafepointEvent(
      Instant timestamp,
      String reason,
      long sinceLastNs,
      long reachingNs,
      long cleanupNs,
      long atSafepointNs,
      long totalNs) {}
}
