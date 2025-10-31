package io.scalecube.metrics.mimir;

import io.scalecube.metrics.Delay;
import io.scalecube.metrics.mimir.MimirPublisher.WriteProxy;
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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prometheus.Remote.WriteRequest;
import prometheus.Types.Label;
import prometheus.Types.Sample;
import prometheus.Types.TimeSeries;
import prometheus.Types.TimeSeries.Builder;

// TODO: keep it, but more work is needed, there's problem on grafana with idle period
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
  private final Delay idleInterval;
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
    this.idleInterval = new Delay(epochClock, READ_INTERVAL.toMillis());
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
    final var tsList = new ArrayList<TimeSeries>();
    int lineEnd;
    while ((lineEnd = lineBuffer.indexOf("\n")) >= 0) {
      String line = lineBuffer.substring(0, lineEnd).trim();
      lineBuffer.delete(0, lineEnd + 1);
      final var event = processLine(line);
      if (event != null) {
        workCount++;
        tsList.addAll(toTimeSeriesList(event));
      }
    }

    if (!tsList.isEmpty()) {
      idleInterval.delay();
      writeProxy.push(WriteRequest.newBuilder().addAllTimeseries(tsList).build());
    }

    if (idleInterval.isOverdue()) {
      writeProxy.push(
          WriteRequest.newBuilder()
              .addAllTimeseries(
                  toTimeSeriesList(new SafepointEvent(Instant.now(), "Null", 0, 0, 0, 0, 0)))
              .build());
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

  private List<TimeSeries> toTimeSeriesList(SafepointEvent event) {
    final var tsList = new ArrayList<TimeSeries>();
    final var timestamp = event.timestamp().toEpochMilli();
    final var reason = event.reason();

    tsList.add(
        toTimeSeries("jvm_safepoint_reaching_micros", reason, event.reachingNs(), timestamp));
    tsList.add(toTimeSeries("jvm_safepoint_cleanup_micros", reason, event.cleanupNs(), timestamp));
    tsList.add(toTimeSeries("jvm_safepoint_at_micros", reason, event.atSafepointNs(), timestamp));
    tsList.add(toTimeSeries("jvm_safepoint_total_micros", reason, event.totalNs(), timestamp));

    return tsList;
  }

  private TimeSeries toTimeSeries(String metric, String reason, long value, long timestamp) {
    var builder =
        TimeSeries.newBuilder()
            .addLabels(Label.newBuilder().setName("__name__").setValue(metric).build())
            .addLabels(Label.newBuilder().setName("reason").setValue(reason).build())
            .addSamples(
                Sample.newBuilder().setValue(value / 1000.0).setTimestamp(timestamp).build());
    addLabels(builder);
    return builder.build();
  }

  private void addLabels(Builder builder) {
    if (labels != null) {
      labels.forEach(
          (name, value) ->
              builder.addLabels(Label.newBuilder().setName(name).setValue(value).build()));
    }
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
