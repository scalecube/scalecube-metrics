package io.scalecube.metrics;

import static io.scalecube.metrics.CountersRegistry.Context.COUNTERS_FILE;
import static org.agrona.IoUtil.mapExistingFile;

import io.scalecube.metrics.CountersRegistry.Context;
import io.scalecube.metrics.CountersRegistry.LayoutDescriptor;
import java.io.File;
import java.nio.MappedByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import org.agrona.BufferUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent that periodically reads counters from mapped counters file {@link Context#COUNTERS_FILE},
 * and invokes {@link CountersHandler} with the counters values.
 */
public class CountersReaderAgent implements Agent {

  private static final Logger LOGGER = LoggerFactory.getLogger(CountersReaderAgent.class);

  public enum State {
    INIT,
    RUNNING,
    CLEANUP,
    CLOSED
  }

  private final String roleName;
  private final File countersDir;
  private final boolean warnIfCountersNotExists;
  private final EpochClock epochClock;
  private final CountersHandler countersHandler;

  private final Delay readInterval;
  private File countersFile;
  private MappedByteBuffer countersByteBuffer;
  private final UnsafeBuffer headerBuffer = new UnsafeBuffer();
  private long countersStartTimestamp = -1;
  private long countersPid = -1;
  private CountersReader countersReader;
  private State state = State.CLOSED;

  /**
   * Constructor.
   *
   * @param roleName roleName
   * @param countersDir counters directory with {@link Context#COUNTERS_FILE}
   * @param warnIfCountersNotExists whether to log warning if counters file does not exist
   * @param epochClock epochClock
   * @param readInterval interval at which to read counters
   * @param countersHandler callback handler to process counters
   */
  public CountersReaderAgent(
      String roleName,
      File countersDir,
      boolean warnIfCountersNotExists,
      EpochClock epochClock,
      Duration readInterval,
      CountersHandler countersHandler) {
    this.roleName = roleName;
    this.countersDir = countersDir;
    this.warnIfCountersNotExists = warnIfCountersNotExists;
    this.epochClock = epochClock;
    this.countersHandler = countersHandler;
    this.readInterval = new Delay(epochClock, readInterval.toMillis());
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
    if (readInterval.isNotOverdue()) {
      return 0;
    }

    countersFile = new File(countersDir, COUNTERS_FILE);

    if (!isActive(countersFile)) {
      state(State.CLEANUP);
      return 0;
    }

    countersByteBuffer = mapExistingFile(countersFile, COUNTERS_FILE);
    headerBuffer.wrap(countersByteBuffer, 0, LayoutDescriptor.HEADER_LENGTH);
    countersStartTimestamp = LayoutDescriptor.startTimestamp(headerBuffer);
    countersPid = LayoutDescriptor.pid(headerBuffer);

    countersReader =
        new CountersReader(
            LayoutDescriptor.createCountersMetaDataBuffer(countersByteBuffer, headerBuffer),
            LayoutDescriptor.createCountersValuesBuffer(countersByteBuffer, headerBuffer));

    state(State.RUNNING);
    LOGGER.info("[{}] Initialized, now running", roleName());
    return 1;
  }

  private int running() {
    if (readInterval.isNotOverdue()) {
      return 0;
    }

    readInterval.delay();
    if (!isActive(countersFile)) {
      state(State.CLEANUP);
      LOGGER.warn("[{}] {} is not active, proceed to cleanup", roleName(), countersFile);
      return 0;
    }

    final var timestamp = epochClock.time();
    final var counterDescriptors = new ArrayList<CounterDescriptor>();
    countersReader.forEach(
        (counterId, typeId, keyBuffer, label) -> {
          final var counterValue = countersReader.getCounterValue(counterId);
          counterDescriptors.add(
              new CounterDescriptor(counterId, typeId, counterValue, keyBuffer, label));
        });
    countersHandler.accept(timestamp, counterDescriptors);

    return 0;
  }

  private boolean isActive(File countersFile) {
    if (!countersFile.exists()) {
      if (warnIfCountersNotExists) {
        LOGGER.warn("[{}] {} not exists", roleName(), countersFile);
      }
      return false;
    }

    final var buffer = mapExistingFile(countersFile, COUNTERS_FILE);
    try {
      if (!LayoutDescriptor.isCountersHeaderLengthSufficient(buffer.capacity())) {
        LOGGER.warn("[{}] {} has not sufficient length", roleName(), countersFile);
        return false;
      }
      headerBuffer.wrap(buffer, 0, LayoutDescriptor.HEADER_LENGTH);
      if (!LayoutDescriptor.isCountersFileLengthSufficient(headerBuffer, buffer.capacity())) {
        LOGGER.warn("[{}] {} has not sufficient length", roleName(), countersFile);
        return false;
      }
      if (countersStartTimestamp != -1
          && !LayoutDescriptor.isCountersActive(
              headerBuffer, countersStartTimestamp, countersPid)) {
        LOGGER.warn("[{}] {} is not active", roleName(), countersFile);
        return false;
      }
    } finally {
      BufferUtil.free(buffer);
    }

    return true;
  }

  private int cleanup() {
    BufferUtil.free(countersByteBuffer);
    countersByteBuffer = null;
    countersFile = null;
    countersStartTimestamp = -1;
    countersPid = -1;

    State previous = state;
    if (previous != State.CLOSED) { // when it comes from onClose()
      readInterval.delay();
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
