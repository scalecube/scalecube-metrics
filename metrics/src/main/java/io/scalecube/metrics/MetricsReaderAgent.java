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
import java.time.Duration;
import org.agrona.BufferUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent that consumes metrics (histograms, tps values) from the {@link BroadcastReceiver}, and
 * dispatches them them to the supplied {@link MetricsHandler}.
 */
public class MetricsReaderAgent implements MessageHandler, Agent {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsReaderAgent.class);

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

  private File metricsFile;
  private final Delay retryInterval;
  private final Delay heartbeatTimeout;
  private MappedByteBuffer metricsByteBuffer;
  private final UnsafeBuffer headerBuffer = new UnsafeBuffer();
  private long metricsStartTimestamp = -1;
  private long metricsPid = -1;
  private int metricsBufferLength = -1;
  private ByteBuffer scratchBuffer;
  private CopyBroadcastReceiver broadcastReceiver;
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final HistogramDecoder histogramDecoder = new HistogramDecoder();
  private final TpsDecoder tpsDecoder = new TpsDecoder();
  private State state = State.CLOSED;

  /**
   * Constructor.
   *
   * @param roleName roleName
   * @param metricsDir counters directory with {@link Context#METRICS_FILE}
   * @param warnIfMetricsNotExists whether to log warning if metrics file does not exist
   * @param epochClock epochClock
   * @param retryInterval retryInterval
   * @param metricsHandler callback handler for processing metrics (histograms, tps values)
   */
  public MetricsReaderAgent(
      String roleName,
      File metricsDir,
      boolean warnIfMetricsNotExists,
      EpochClock epochClock,
      Duration retryInterval,
      MetricsHandler metricsHandler) {
    this.roleName = roleName;
    this.metricsDir = metricsDir;
    this.warnIfMetricsNotExists = warnIfMetricsNotExists;
    this.metricsHandler = metricsHandler;
    this.retryInterval = new Delay(epochClock, retryInterval.toMillis());
    this.heartbeatTimeout = new Delay(epochClock, retryInterval.toMillis());
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  public void onStart() {
    if (state != State.CLOSED) {
      throw new AgentTerminationException("Illegal state: " + state);
    }
    state(State.INIT);
  }

  @Override
  public int doWork() {
    try {
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

  private int init() {
    if (retryInterval.isNotOverdue()) {
      return 0;
    }

    metricsFile = new File(metricsDir, METRICS_FILE);

    if (!isActive(metricsFile)) {
      state(State.CLEANUP);
      return 0;
    }

    metricsByteBuffer = mapExistingFile(metricsFile, METRICS_FILE);
    final var headerLength = LayoutDescriptor.HEADER_LENGTH;
    headerBuffer.wrap(metricsByteBuffer, 0, headerLength);
    metricsStartTimestamp = LayoutDescriptor.startTimestamp(headerBuffer);
    metricsPid = LayoutDescriptor.pid(headerBuffer);
    metricsBufferLength = LayoutDescriptor.metricsBufferLength(headerBuffer);

    if (metricsBufferLength <= 0) {
      state(State.CLEANUP);
      return 0;
    }

    scratchBuffer = ByteBuffer.allocateDirect(1024 * 1024);
    broadcastReceiver =
        new CopyBroadcastReceiver(
            new BroadcastReceiver(
                new UnsafeBuffer(metricsByteBuffer, headerLength, metricsBufferLength)),
            new UnsafeBuffer(scratchBuffer));
    broadcastReceiver.receive(
        (msgTypeId, buffer, index, length) -> {
          // no-op
        });

    state(State.RUNNING);
    LOGGER.info("[{}] Initialized, now running", roleName());
    return 1;
  }

  private int running() {
    if (heartbeatTimeout.isOverdue()) {
      heartbeatTimeout.delay();
      if (!isActive(metricsFile)) {
        state(State.CLEANUP);
        LOGGER.warn("[{}] {} is not active, proceed to cleanup", roleName(), metricsFile);
        return 0;
      }
    }
    return broadcastReceiver.receive(this);
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
    heartbeatTimeout.delay();
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

  private boolean isActive(File metricsFile) {
    if (!metricsFile.exists()) {
      if (warnIfMetricsNotExists) {
        LOGGER.warn("[{}] {} not exists", roleName(), metricsFile);
      }
      return false;
    }

    final var buffer = mapExistingFile(metricsFile, METRICS_FILE);
    try {
      if (!LayoutDescriptor.isMetricsHeaderLengthSufficient(buffer.capacity())) {
        LOGGER.warn("[{}] {} has not sufficient length", roleName(), metricsFile);
        return false;
      }
      headerBuffer.wrap(buffer, 0, LayoutDescriptor.HEADER_LENGTH);
      if (!LayoutDescriptor.isMetricsFileLengthSufficient(headerBuffer, buffer.capacity())) {
        LOGGER.warn("[{}] {} has not sufficient length", roleName(), metricsFile);
        return false;
      }
      if (metricsBufferLength != -1
          && !LayoutDescriptor.isMetricsActive(headerBuffer, metricsStartTimestamp, metricsPid)) {
        LOGGER.warn("[{}] {} is not active", roleName(), metricsFile);
        return false;
      }
    } finally {
      BufferUtil.free(buffer);
    }

    return true;
  }

  private int cleanup() {
    BufferUtil.free(scratchBuffer);
    scratchBuffer = null;
    BufferUtil.free(metricsByteBuffer);
    metricsByteBuffer = null;
    metricsFile = null;
    metricsStartTimestamp = -1;
    metricsPid = -1;
    metricsBufferLength = -1;

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
    this.state = state;
  }
}
