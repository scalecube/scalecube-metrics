package io.scalecube.metrics;

import static io.scalecube.metrics.MetricsRecorder.Context.METRICS_FILE;
import static org.agrona.IoUtil.mapExistingFile;

import io.scalecube.metrics.MetricsRecorder.LayoutDescriptor;
import java.io.File;
import java.nio.MappedByteBuffer;
import java.time.Duration;
import org.agrona.BufferUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetricsTransmitterAgent implements Agent, MessageHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsTransmitterAgent.class);

  public enum State {
    INIT,
    RUNNING,
    CLEANUP,
    CLOSED
  }

  private final File metricsDir;
  private final boolean warnIfMetricsNotExists;
  private final BroadcastTransmitter broadcastTransmitter;

  private final Delay retryInterval;
  private final Delay heartbeatTimeout;
  private File metricsFile;
  private RingBuffer metricsBuffer;
  private MappedByteBuffer metricsByteBuffer;
  private final UnsafeBuffer headerBuffer = new UnsafeBuffer();
  private long metricsStartTimestamp = -1;
  private long metricsPid = -1;
  private State state = State.CLOSED;

  MetricsTransmitterAgent(
      File metricsDir,
      boolean warnIfMetricsNotExists,
      EpochClock epochClock,
      Duration retryInterval,
      Duration heartbeatTimeout,
      BroadcastTransmitter broadcastTransmitter) {
    this.metricsDir = metricsDir;
    this.warnIfMetricsNotExists = warnIfMetricsNotExists;
    this.broadcastTransmitter = broadcastTransmitter;
    this.retryInterval = new Delay(epochClock, retryInterval.toMillis());
    this.heartbeatTimeout = new Delay(epochClock, heartbeatTimeout.toMillis());
  }

  @Override
  public String roleName() {
    return "MetricsTransmitterAgent";
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
    headerBuffer.wrap(metricsByteBuffer, 0, LayoutDescriptor.HEADER_LENGTH);
    metricsStartTimestamp = LayoutDescriptor.startTimestamp(headerBuffer);
    metricsPid = LayoutDescriptor.pid(headerBuffer);

    final var headerLength = LayoutDescriptor.HEADER_LENGTH;
    final var totalLength = metricsByteBuffer.capacity();
    final var length = totalLength - headerLength;

    metricsBuffer =
        new ManyToOneRingBuffer(new UnsafeBuffer(metricsByteBuffer, headerLength, length));

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
    return metricsBuffer.read(this, 100);
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
      if (metricsStartTimestamp != -1
          && !LayoutDescriptor.isMetricsActive(headerBuffer, metricsStartTimestamp, metricsPid)) {
        LOGGER.warn("[{}] {} is not active", roleName(), metricsFile);
        return false;
      }
    } finally {
      BufferUtil.free(buffer);
    }

    return true;
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
    heartbeatTimeout.delay();
    broadcastTransmitter.transmit(msgTypeId, buffer, index, length);
  }

  private int cleanup() {
    BufferUtil.free(metricsByteBuffer);
    metricsByteBuffer = null;
    metricsFile = null;
    metricsStartTimestamp = -1;
    metricsPid = -1;

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

  public State state() {
    return state;
  }
}
