package io.scalecube.metrics.aeron;

import io.aeron.Aeron;
import io.aeron.CncFileDescriptor;
import io.aeron.exceptions.DriverTimeoutException;
import io.scalecube.metrics.CountersHandler;
import io.scalecube.metrics.Delay;
import java.time.Duration;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent that periodically reads counters from mapped counters file {@link
 * CncFileDescriptor#CNC_FILE}, and invokes {@link CountersHandler} with the counters values.
 */
public class CncCountersReaderAgent implements Agent {

  private static final Logger LOGGER = LoggerFactory.getLogger(CncCountersReaderAgent.class);

  public enum State {
    INIT,
    RUNNING,
    CLEANUP,
    CLOSED
  }

  private final String roleName;
  private final EpochClock epochClock;
  private final CncCountersSnapshot countersSnapshot;
  private final CountersHandler countersHandler;
  private final Delay readInterval;
  private State state = State.CLOSED;

  /**
   * Constructor.
   *
   * @param roleName roleName
   * @param aeronDirectoryName aeronDirectoryName
   * @param warnIfCncNotExists whether to log warning if counters file does not exist
   * @param epochClock epochClock
   * @param readInterval interval at which to read counters
   * @param driverTimeout media-driver timeout (see {@link Aeron.Context#driverTimeoutMs(long)})
   * @param countersHandler callback handler to process counters
   */
  public CncCountersReaderAgent(
      String roleName,
      String aeronDirectoryName,
      boolean warnIfCncNotExists,
      EpochClock epochClock,
      Duration readInterval,
      Duration driverTimeout,
      CountersHandler countersHandler) {
    this.roleName = roleName;
    this.epochClock = epochClock;
    this.readInterval = new Delay(epochClock, readInterval.toMillis());
    this.countersHandler = countersHandler;
    this.countersSnapshot =
        new CncCountersSnapshot(
            roleName, aeronDirectoryName, warnIfCncNotExists, driverTimeout, false);
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

    if (!countersSnapshot.attach()) {
      state(State.CLEANUP);
      return 0;
    }

    state(State.RUNNING);
    LOGGER.info("[{}] Initialized, now running", roleName());
    return 1;
  }

  private int running() {
    try {
      countersSnapshot.conductorAgentInvoker().invoke();
    } catch (AgentTerminationException | DriverTimeoutException e) {
      state(State.CLEANUP);
      LOGGER.warn(
          "[{}] conductorAgentInvoker has thrown exception: {}, proceed to cleanup",
          roleName(),
          e.toString());
      return 0;
    }

    if (readInterval.isNotOverdue()) {
      return 0;
    }

    readInterval.delay();

    countersHandler.accept(epochClock.time(), countersSnapshot.snapshot());

    return 0;
  }

  private int cleanup() {
    CloseHelper.quietCloseAll(countersSnapshot);

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
