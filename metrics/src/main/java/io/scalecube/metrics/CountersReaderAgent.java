package io.scalecube.metrics;

import io.scalecube.metrics.CountersRegistry.Context;
import java.io.File;
import java.time.Duration;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
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
  private final EpochClock epochClock;
  private final CountersHandler countersHandler;

  private final Delay readInterval;
  private File countersFile;
  private final CountersSnapshot countersSnapshot;
  private final boolean keepOpen;
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
   * @param keepOpen set to true to keep the mmaped file open the whole time, false if it should be
   *     closed immediately after reading
   */
  public CountersReaderAgent(
      String roleName,
      File countersDir,
      boolean warnIfCountersNotExists,
      EpochClock epochClock,
      Duration readInterval,
      CountersHandler countersHandler,
      boolean keepOpen) {
    this.roleName = roleName;
    this.epochClock = epochClock;
    this.countersHandler = countersHandler;
    this.readInterval = new Delay(epochClock, readInterval.toMillis());
    this.countersSnapshot =
        new CountersSnapshot(roleName, countersDir, warnIfCountersNotExists, false);
    this.keepOpen = keepOpen;
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
        case RUNNING -> {
          final var val = running();
          if (!keepOpen) {
            cleanup();
          }
          yield val;
        }
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
    if (!countersSnapshot.attach()) {
      state(State.CLEANUP);
      return 1;
    }

    state(State.RUNNING);
    LOGGER.info("[{}] Initialized, now running", roleName());
    return 1;
  }

  private int running() {
    if (readInterval.isNotOverdue()) {
      return 0;
    }

    readInterval.delay();

    if (!countersSnapshot.isActive()) {
      state(State.CLEANUP);
      LOGGER.warn("[{}] {} is not active, proceed to cleanup", roleName(), countersFile);
      return 1;
    }

    countersHandler.accept(epochClock.time(), countersSnapshot.snapshotCounters());
    return 0;
  }

  private int cleanup() {
    countersSnapshot.close();

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
    this.state = state;
  }

  public State state() {
    return state;
  }
}
