package io.scalecube.metrics;

import static io.scalecube.metrics.MetricsRecorder.Context.METRICS_FILE;
import static org.HdrHistogram.Histogram.decodeFromByteBuffer;
import static org.agrona.IoUtil.mapExistingFile;

import io.scalecube.metrics.MetricsRecorder.Context;
import io.scalecube.metrics.MetricsRecorder.LayoutDescriptor;
import io.scalecube.metrics.sbe.HistogramDecoder;
import io.scalecube.metrics.sbe.MessageHeaderDecoder;
import io.scalecube.metrics.sbe.TpsDecoder;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.time.Duration;
import org.agrona.BufferUtil;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent that consumes metrics (histograms, tps values) from {@link Context#METRICS_FILE}, and
 * dispatches them to the supplied {@link MetricsHandler}.
 *
 * <p>Polls same way as {@link CountersReaderAgent}: every {@code readInterval} maps the file, reads
 * messages transmitted since the previous read, and unmaps the file. On first read, or when writer
 * process changed (pid, start time), reading starts from the latest message. If reader was lapped
 * between reads, lost messages are skipped.
 */
public class MetricsReaderAgent implements MessageHandler, Agent {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsReaderAgent.class);

  private static final int INITIAL_MESSAGE_BUFFER_LENGTH = 4096;

  public enum State {
    INIT,
    RUNNING,
    CLEANUP,
    CLOSED
  }

  private final String roleName;
  private final File metricsDir;
  private final boolean warnIfMetricsNotExists;
  private final MetricsHandler metricsHandler;
  private final EpochClock epochClock;
  private final long readInterval;

  private long nextReadTime;
  private long startTimestamp;
  private long pid;
  private long lappedCount;
  private final UnsafeBuffer headerBuffer = new UnsafeBuffer();
  private final UnsafeBuffer broadcastBuffer = new UnsafeBuffer();
  private BroadcastReceiver broadcastReceiver;
  private final ExpandableArrayBuffer messageBuffer =
      new ExpandableArrayBuffer(INITIAL_MESSAGE_BUFFER_LENGTH);
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final HistogramDecoder histogramDecoder = new HistogramDecoder();
  private final TpsDecoder tpsDecoder = new TpsDecoder();
  private State state = State.CLOSED;

  /**
   * Constructor.
   *
   * @param roleName roleName
   * @param metricsDir metrics directory with {@link Context#METRICS_FILE}
   * @param warnIfMetricsNotExists whether to log warning if metrics file does not exist
   * @param epochClock epochClock
   * @param readInterval interval of reading metrics file, also retry interval if the file is
   *     missing or invalid
   * @param metricsHandler callback handler for processing metrics (histograms, tps values)
   */
  public MetricsReaderAgent(
      String roleName,
      File metricsDir,
      boolean warnIfMetricsNotExists,
      EpochClock epochClock,
      Duration readInterval,
      MetricsHandler metricsHandler) {
    this.roleName = roleName;
    this.metricsDir = metricsDir;
    this.warnIfMetricsNotExists = warnIfMetricsNotExists;
    this.metricsHandler = metricsHandler;
    this.epochClock = epochClock;
    this.readInterval = readInterval.toMillis();
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  public void onStart() {
    if (state != State.CLOSED) {
      throw new IllegalStateException("Illegal state: " + state);
    }
    state(State.INIT);
  }

  @Override
  public int doWork() {
    try {
      return switch (state) {
        case INIT, RUNNING -> readMetrics();
        case CLEANUP -> cleanup();
        case CLOSED -> 0;
      };
    } catch (Exception e) {
      state(State.CLEANUP);
      throw e;
    }
  }

  private int readMetrics() {
    final var now = epochClock.time();
    if (now < nextReadTime) {
      return 0;
    } else {
      nextReadTime = now + readInterval;
    }

    final var metricsFile = new File(metricsDir, METRICS_FILE);
    if (!metricsFile.exists()) {
      if (warnIfMetricsNotExists) {
        LOGGER.warn("[{}] {} not exists", roleName(), metricsFile);
      }
      state(State.CLEANUP);
      return 0;
    }

    final var metricsByteBuffer = mapExistingFile(metricsFile, MapMode.READ_ONLY, METRICS_FILE);
    try {
      return readMetrics(metricsFile, metricsByteBuffer);
    } finally {
      broadcastBuffer.wrap(0, 0);
      headerBuffer.wrap(0, 0);
      BufferUtil.free(metricsByteBuffer);
    }
  }

  private int readMetrics(File metricsFile, MappedByteBuffer metricsByteBuffer) {
    final var fileLength = metricsByteBuffer.capacity();
    if (!LayoutDescriptor.isMetricsHeaderLengthSufficient(fileLength)) {
      LOGGER.warn("[{}] {} is truncated, length: {}", roleName(), metricsFile, fileLength);
      state(State.CLEANUP);
      return 0;
    }

    final var headerLength = LayoutDescriptor.HEADER_LENGTH;
    headerBuffer.wrap(metricsByteBuffer, 0, headerLength);
    final var metricsBufferLength = LayoutDescriptor.metricsBufferLength(headerBuffer);

    if (metricsBufferLength <= 0) {
      state(State.CLEANUP);
      return 0;
    }

    if (!LayoutDescriptor.isMetricsFileLengthSufficient(headerBuffer, fileLength)) {
      LOGGER.warn(
          "[{}] {} is shorter than its header declares, length: {}, declared: {}",
          roleName(),
          metricsFile,
          fileLength,
          headerLength + metricsBufferLength);
      state(State.CLEANUP);
      return 0;
    }

    // BroadcastReceiver reads through this buffer, re-wrapping it onto the new mapping keeps
    // receiver's position from the previous read
    broadcastBuffer.wrap(metricsByteBuffer, headerLength, metricsBufferLength);

    if (state == State.INIT
        || !LayoutDescriptor.isMetricsActive(headerBuffer, startTimestamp, pid)) {
      startTimestamp = LayoutDescriptor.startTimestamp(headerBuffer);
      pid = LayoutDescriptor.pid(headerBuffer);
      broadcastReceiver = new BroadcastReceiver(broadcastBuffer);
      broadcastReceiver.receiveNext(); // skip first (latest) one, start from the end
      lappedCount = broadcastReceiver.lappedCount();
      state(State.RUNNING);
      LOGGER.info("[{}] Initialized, now running, pid: {}", roleName(), pid);
      return 1;
    }

    return receiveMessages(metricsFile);
  }

  private int receiveMessages(File metricsFile) {
    int workCount = 0;
    while (broadcastReceiver.receiveNext()) {
      if (broadcastReceiver.lappedCount() != lappedCount) {
        lappedCount = broadcastReceiver.lappedCount();
        LOGGER.warn(
            "[{}] {} writer lapped reader, messages lost, lappedCount: {}",
            roleName(),
            metricsFile,
            lappedCount);
      }

      final var length = broadcastReceiver.length();
      messageBuffer.putBytes(0, broadcastReceiver.buffer(), broadcastReceiver.offset(), length);

      // Message could be overwritten while being copied, then skip it, as being lost
      if (broadcastReceiver.validate()) {
        onMessage(broadcastReceiver.typeId(), messageBuffer, 0, length);
        workCount++;
      }
    }
    return workCount;
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
    headerDecoder.wrap(buffer, index);
    switch (headerDecoder.templateId()) {
      case HistogramDecoder.TEMPLATE_ID:
        onHistogram(histogramDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case TpsDecoder.TEMPLATE_ID:
        onTps(tpsDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      default:
        break;
    }
  }

  private void onHistogram(HistogramDecoder decoder) {
    final var timestamp = decoder.timestamp();
    final var highestTrackableValue = decoder.highestTrackableValue();
    final var conversionFactor = decoder.conversionFactor();

    final var keyBuffer = decoder.buffer();
    final var keyOffset = decoder.limit() + HistogramDecoder.keyHeaderLength();
    final var keyLength = decoder.keyLength();
    decoder.skipKey();

    final var accumulatedBytes = new byte[decoder.accumulatedLength()];
    decoder.getAccumulated(accumulatedBytes, 0, accumulatedBytes.length);
    final var accumulated = decodeFromByteBuffer(ByteBuffer.wrap(accumulatedBytes), 1);

    final var distinctBytes = new byte[decoder.distinctLength()];
    decoder.getDistinct(distinctBytes, 0, distinctBytes.length);
    final var distinct = decodeFromByteBuffer(ByteBuffer.wrap(distinctBytes), 1);

    metricsHandler.onHistogram(
        timestamp,
        keyBuffer,
        keyOffset,
        keyLength,
        accumulated,
        distinct,
        highestTrackableValue,
        conversionFactor);
  }

  private void onTps(TpsDecoder decoder) {
    final var timestamp = decoder.timestamp();
    final var value = decoder.value();

    final var keyBuffer = decoder.buffer();
    final var keyOffset = decoder.limit() + TpsDecoder.keyHeaderLength();
    final var keyLength = decoder.keyLength();
    decoder.skipKey();

    metricsHandler.onTps(timestamp, keyBuffer, keyOffset, keyLength, value);
  }

  private int cleanup() {
    State previous = state;
    if (previous != State.CLOSED) { // when it comes from onClose()
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
    this.state = state;
  }

  public State state() {
    return state;
  }
}
