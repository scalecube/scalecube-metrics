package io.scalecube.metrics;

import java.time.Duration;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent that consumes metrics (histograms, tps values) from the {@link BroadcastReceiver}, and
 * dispatches them to user-supplied {@link MetricsHandler}.
 */
public class MetricsReaderAgent implements Agent {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsReaderAgent.class);

  public enum State {
    INIT,
    RUNNING,
    CLEANUP,
    CLOSED
  }

  private final String roleName;
  private final AtomicBuffer broadcastBuffer;
  private final MetricsHandler metricsHandler;

  private final Delay retryInterval;
  private MetricsReader metricsReader;
  private State state = State.CLOSED;

  /**
   * Constructor.
   *
   * @param roleName roleName
   * @param broadcastBuffer buffer from where to consume SBE metrics messages
   * @param epochClock epochClock
   * @param retryInterval retryInterval
   * @param metricsHandler callback handler for processing metrics (histograms, tps values)
   */
  public MetricsReaderAgent(
      String roleName,
      AtomicBuffer broadcastBuffer,
      EpochClock epochClock,
      Duration retryInterval,
      MetricsHandler metricsHandler) {
    this.roleName = roleName;
    this.metricsHandler = metricsHandler;
    this.broadcastBuffer = broadcastBuffer;
    this.retryInterval = new Delay(epochClock, retryInterval.toMillis());
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

    metricsReader = new MetricsReader(broadcastBuffer);

    state(State.RUNNING);
    LOGGER.info("[{}] Initialized, now running", roleName());
    return 1;
  }

  private int running() {
    return metricsReader.read(metricsHandler);
  }

  private int cleanup() {
    CloseHelper.quietCloseAll(metricsReader);

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
}
