package io.scalecube.metrics.aeron;

import static io.aeron.CncFileDescriptor.CNC_FILE;

import io.aeron.Aeron;
import io.aeron.CncFileDescriptor;
import io.aeron.RethrowingErrorHandler;
import io.aeron.exceptions.DriverTimeoutException;
import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import io.scalecube.metrics.Delay;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import org.agrona.CloseHelper;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.CountersReader;
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
  private final String aeronDirectoryName;
  private final boolean warnIfCncNotExists;
  private final EpochClock epochClock;
  private final Duration driverTimeout;
  private final CountersHandler countersHandler;

  private final Delay readInterval;
  private Aeron aeron;
  private CountersReader countersReader;
  private AgentInvoker conductorAgentInvoker;
  private final Int2ObjectHashMap<KeyConverter> keyConverters = new Int2ObjectHashMap<>();
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
    this.aeronDirectoryName = aeronDirectoryName;
    this.warnIfCncNotExists = warnIfCncNotExists;
    this.epochClock = epochClock;
    this.driverTimeout = driverTimeout;
    this.countersHandler = countersHandler;
    this.readInterval = new Delay(epochClock, readInterval.toMillis());
    ArchiveCountersAdapter.populate(keyConverters);
    ClusterCountersAdapter.populate(keyConverters);
    ClusteredServiceCountersAdapter.populate(keyConverters);
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

    final var cncFile = new File(aeronDirectoryName, CNC_FILE);
    if (!cncFile.exists()) {
      if (warnIfCncNotExists) {
        LOGGER.warn("[{}] {} not exists", roleName(), cncFile);
      }
      state(State.CLEANUP);
      return 0;
    }

    aeron =
        Aeron.connect(
            new Aeron.Context()
                .useConductorAgentInvoker(true)
                .aeronDirectoryName(aeronDirectoryName)
                .errorHandler(RethrowingErrorHandler.INSTANCE)
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE)
                .driverTimeoutMs(driverTimeout.toMillis()));
    conductorAgentInvoker = aeron.conductorAgentInvoker();
    countersReader = aeron.countersReader();

    state(State.RUNNING);
    LOGGER.info("[{}] Initialized, now running", roleName());
    return 1;
  }

  private int running() {
    try {
      conductorAgentInvoker.invoke();
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

    final var timestamp = epochClock.time();
    final var counterDescriptors = new ArrayList<CounterDescriptor>();
    countersReader.forEach(
        (counterId, typeId, keyBuffer, label) -> {
          final var keyConverter = keyConverters.get(typeId);
          if (keyConverter != null) {
            counterDescriptors.add(
                new CounterDescriptor(
                    counterId,
                    typeId,
                    countersReader.getCounterValue(counterId),
                    keyConverter.convert(keyBuffer, label),
                    null));
          }
        });
    countersHandler.accept(timestamp, counterDescriptors);

    return 0;
  }

  private int cleanup() {
    CloseHelper.quietCloseAll(aeron);
    aeron = null;
    conductorAgentInvoker = null;
    countersReader = null;

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
