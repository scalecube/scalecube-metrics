package io.scalecube.metrics;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static org.agrona.IoUtil.ensureDirectoryExists;
import static org.agrona.concurrent.broadcast.BroadcastBufferDescriptor.TRAILER_LENGTH;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component that reads metrics data (histograms, tps values) from many-to-one ring-buffer produced
 * by {@link MetricsRecorder}(s), and publishing those metrics to one-to-many ring-buffer for
 * further processing by multiple subscribers.
 *
 * @see MetricsRecorder
 */
public class MetricsTransmitter implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsTransmitter.class);

  private final Context context;

  private final AgentInvoker agentInvoker;
  private final AgentRunner agentRunner;

  private MetricsTransmitter(Context context) {
    context.conclude();
    this.context = context;

    final var agent =
        new MetricsTransmitterAgent(
            context.metricsDir(),
            context.warnIfMetricsNotExists(),
            context.epochClock(),
            context.retryInterval(),
            context.heartbeatTimeout(),
            context.broadcastTransmitter());

    if (context.useAgentInvoker()) {
      agentRunner = null;
      agentInvoker = new AgentInvoker(context.errorHandler(), null, agent);
    } else {
      agentInvoker = null;
      agentRunner = new AgentRunner(context.idleStrategy(), context.errorHandler(), null, agent);
    }
  }

  /**
   * Launch {@link MetricsTransmitter} with default {@link Context}.
   *
   * @return newly started {@link MetricsTransmitter}
   */
  public static MetricsTransmitter launch() {
    return launch(new Context());
  }

  /**
   * Launch {@link MetricsTransmitter} with provided {@link Context}.
   *
   * @param context context
   * @return newly started {@link MetricsTransmitter}
   */
  public static MetricsTransmitter launch(Context context) {
    final var metricsRegistry = new MetricsTransmitter(context);
    if (metricsRegistry.agentInvoker != null) {
      metricsRegistry.agentInvoker.start();
    } else {
      AgentRunner.startOnThread(metricsRegistry.agentRunner);
    }
    return metricsRegistry;
  }

  /**
   * Returns {@link Context} instance.
   *
   * @return {@link Context} instance
   */
  public Context context() {
    return context;
  }

  /**
   * Returns {@link AgentInvoker} instance when running without threads, or null if running with
   * {@link AgentRunner}.
   *
   * @return {@link AgentInvoker} instance, or null
   */
  public AgentInvoker agentInvoker() {
    return agentInvoker;
  }

  @Override
  public void close() {
    CloseHelper.quietCloseAll(agentInvoker, agentRunner);
  }

  public static class Context {

    public static final String METRICS_DIRECTORY_NAME_PROP_NAME =
        "scalecube.metrics.transmitter.metricsDirectoryName";
    public static final String WARN_IF_METRICS_NOT_EXISTS_PROP_NAME =
        "scalecube.metrics.transmitter.warnIfMetricsNotExists";
    public static final String RETRY_INTERVAL_PROP_NAME =
        "scalecube.metrics.transmitter.retryInterval";
    public static final String HEARTBEAT_TIMEOUT_PROP_NAME =
        "scalecube.metrics.transmitter.heartbeatTimeout";
    public static final String BROADCAST_BUFFER_LENGTH_PROP_NAME =
        "scalecube.metrics.transmitter.broadcastBufferLength";
    public static final String IDLE_STRATEGY_PROP_NAME =
        "scalecube.metrics.transmitter.idleStrategy";

    public static final int DEFAULT_METRICS_BROADCAST_BUFFER_LENGTH = 32 * 1024 * 1024;

    private static final AtomicIntegerFieldUpdater<Context> IS_CONCLUDED_UPDATER =
        newUpdater(Context.class, "isConcluded");

    private volatile int isConcluded;

    private File metricsDir;
    private String metricsDirectoryName = System.getProperty(METRICS_DIRECTORY_NAME_PROP_NAME);
    private boolean warnIfMetricsNotExists =
        Boolean.getBoolean(WARN_IF_METRICS_NOT_EXISTS_PROP_NAME);
    private EpochClock epochClock;
    private Duration retryInterval;
    private Duration heartbeatTimeout;
    private int broadcastBufferLength = Integer.getInteger(BROADCAST_BUFFER_LENGTH_PROP_NAME, 0);
    private AtomicBuffer broadcastBuffer;
    private BroadcastTransmitter broadcastTransmitter;
    private boolean useAgentInvoker;
    private ErrorHandler errorHandler;
    private IdleStrategy idleStrategy;

    public Context() {}

    public Context(Properties props) {
      metricsDirectoryName(props.getProperty(METRICS_DIRECTORY_NAME_PROP_NAME));
      warnIfMetricsNotExists(props.getProperty(WARN_IF_METRICS_NOT_EXISTS_PROP_NAME));
      retryInterval(props.getProperty(RETRY_INTERVAL_PROP_NAME));
      heartbeatTimeout(props.getProperty(HEARTBEAT_TIMEOUT_PROP_NAME));
      broadcastBufferLength(props.getProperty(BROADCAST_BUFFER_LENGTH_PROP_NAME));
      idleStrategy(props.getProperty(IDLE_STRATEGY_PROP_NAME));
    }

    private void conclude() {
      if (0 != IS_CONCLUDED_UPDATER.getAndSet(this, 1)) {
        throw new ConcurrentModificationException();
      }

      concludeMetricsDirectory();
      concludeTransmitter();

      if (epochClock == null) {
        epochClock = SystemEpochClock.INSTANCE;
      }

      if (retryInterval == null) {
        retryInterval = Duration.ofSeconds(3);
      }

      if (heartbeatTimeout == null) {
        heartbeatTimeout = Duration.ofSeconds(1);
      }

      if (errorHandler == null) {
        errorHandler = ex -> LOGGER.error("Exception occurred: ", ex);
      }

      if (idleStrategy == null) {
        idleStrategy = new BackoffIdleStrategy();
      }
    }

    private void concludeMetricsDirectory() {
      if (metricsDirectoryName == null) {
        metricsDirectoryName = MetricsRecorder.Context.DEFAULT_METRICS_DIR_NAME;
      }

      if (metricsDir == null) {
        try {
          metricsDir = new File(metricsDirectoryName).getCanonicalFile();
        } catch (IOException e) {
          LangUtil.rethrowUnchecked(e);
        }
      }

      ensureDirectoryExists(metricsDir, "metrics");
    }

    private void concludeTransmitter() {
      if (broadcastBufferLength == 0) {
        broadcastBufferLength = DEFAULT_METRICS_BROADCAST_BUFFER_LENGTH;
      }

      final var min = DEFAULT_METRICS_BROADCAST_BUFFER_LENGTH;
      if (broadcastBufferLength < min) {
        throw new IllegalArgumentException("broadcastBufferLength must be at least " + min);
      }
      if (!BitUtil.isPowerOfTwo(broadcastBufferLength)) {
        throw new IllegalArgumentException("broadcastBufferLength must be power of 2");
      }

      final var broadcastByteBuffer =
          ByteBuffer.allocateDirect(broadcastBufferLength + TRAILER_LENGTH);
      broadcastBuffer = new UnsafeBuffer(broadcastByteBuffer);
      broadcastTransmitter = new BroadcastTransmitter(broadcastBuffer);
    }

    public File metricsDir() {
      return metricsDir;
    }

    public Context metricsDir(File metricsDir) {
      this.metricsDir = metricsDir;
      return this;
    }

    public String metricsDirectoryName() {
      return metricsDirectoryName;
    }

    public Context metricsDirectoryName(String metricsDirectoryName) {
      this.metricsDirectoryName = metricsDirectoryName;
      return this;
    }

    public boolean warnIfMetricsNotExists() {
      return warnIfMetricsNotExists;
    }

    public Context warnIfMetricsNotExists(boolean warnIfMetricsNotExists) {
      this.warnIfMetricsNotExists = warnIfMetricsNotExists;
      return this;
    }

    public Context warnIfMetricsNotExists(String warnIfMetricsNotExists) {
      if (warnIfMetricsNotExists != null) {
        this.warnIfMetricsNotExists = Boolean.parseBoolean(warnIfMetricsNotExists);
      }
      return this;
    }

    public EpochClock epochClock() {
      return epochClock;
    }

    public Context epochClock(EpochClock epochClock) {
      this.epochClock = epochClock;
      return this;
    }

    public Duration retryInterval() {
      return retryInterval;
    }

    public Context retryInterval(Duration retryInterval) {
      this.retryInterval = retryInterval;
      return this;
    }

    public Context retryInterval(String retryInterval) {
      if (retryInterval != null) {
        this.retryInterval =
            Duration.ofNanos(SystemUtil.parseDuration("retryInterval", retryInterval));
      }
      return this;
    }

    public Duration heartbeatTimeout() {
      return heartbeatTimeout;
    }

    public Context heartbeatTimeout(Duration heartbeatTimeout) {
      this.heartbeatTimeout = heartbeatTimeout;
      return this;
    }

    public Context heartbeatTimeout(String heartbeatTimeout) {
      if (heartbeatTimeout != null) {
        this.heartbeatTimeout =
            Duration.ofNanos(SystemUtil.parseDuration("heartbeatTimeout", heartbeatTimeout));
      }
      return this;
    }

    public boolean useAgentInvoker() {
      return useAgentInvoker;
    }

    public Context useAgentInvoker(boolean useAgentInvoker) {
      this.useAgentInvoker = useAgentInvoker;
      return this;
    }

    public ErrorHandler errorHandler() {
      return errorHandler;
    }

    public Context errorHandler(ErrorHandler errorHandler) {
      this.errorHandler = errorHandler;
      return this;
    }

    public Context idleStrategy(IdleStrategy idleStrategy) {
      this.idleStrategy = idleStrategy;
      return this;
    }

    public Context idleStrategy(String idleStrategy) {
      if (idleStrategy != null) {
        try {
          this.idleStrategy =
              (IdleStrategy) Class.forName(idleStrategy).getConstructor().newInstance();
        } catch (Exception ex) {
          LangUtil.rethrowUnchecked(ex);
        }
      }
      return this;
    }

    public IdleStrategy idleStrategy() {
      return idleStrategy;
    }

    public int broadcastBufferLength() {
      return broadcastBufferLength;
    }

    public Context broadcastBufferLength(int broadcastBufferLength) {
      this.broadcastBufferLength = broadcastBufferLength;
      return this;
    }

    public Context broadcastBufferLength(String broadcastBufferLength) {
      if (broadcastBufferLength != null) {
        this.broadcastBufferLength = Integer.parseInt(broadcastBufferLength);
      }
      return this;
    }

    public AtomicBuffer broadcastBuffer() {
      return broadcastBuffer;
    }

    BroadcastTransmitter broadcastTransmitter() {
      return broadcastTransmitter;
    }
  }
}
