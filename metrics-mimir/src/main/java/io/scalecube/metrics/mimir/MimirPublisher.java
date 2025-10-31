package io.scalecube.metrics.mimir;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.SystemEpochClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prometheus.Remote.WriteRequest;

/**
 * Component that publishes {@link WriteRequest} objects to Mimir. Requests gets accumulated in the
 * queue, and being published by time interval.
 */
public class MimirPublisher implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MimirPublisher.class);

  private final Context context;

  private final AgentInvoker agentInvoker;
  private final AgentRunner agentRunner;

  private MimirPublisher(Context context) {
    context.conclude();
    this.context = context;

    final var agent =
        new MimirPublisherAgent(
            context.url(),
            context.epochClock(),
            context.retryInterval(),
            context.publishInterval(),
            context.writeLimit(),
            context.writeQueue());

    if (context.useAgentInvoker()) {
      agentRunner = null;
      agentInvoker = new AgentInvoker(context.errorHandler(), null, agent);
    } else {
      agentInvoker = null;
      agentRunner = new AgentRunner(context.idleStrategy(), context.errorHandler(), null, agent);
    }
  }

  /**
   * Launch {@link MimirPublisher} with default {@link Context}.
   *
   * @return newly started {@link MimirPublisher}
   */
  public static MimirPublisher launch() {
    return launch(new Context());
  }

  /**
   * Launch {@link MimirPublisher} with provided {@link Context}.
   *
   * @param context context
   * @return newly started {@link MimirPublisher}
   */
  public static MimirPublisher launch(Context context) {
    final var metricsReceiver = new MimirPublisher(context);
    if (metricsReceiver.agentInvoker != null) {
      metricsReceiver.agentInvoker.start();
    } else {
      AgentRunner.startOnThread(metricsReceiver.agentRunner);
    }
    return metricsReceiver;
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

  /**
   * Returns {@link WriteProxy} instance.
   *
   * @return {@link WriteProxy} instance
   */
  public WriteProxy proxy() {
    return new WriteProxy(context.writeQueue());
  }

  @Override
  public void close() {
    CloseHelper.quietCloseAll(agentInvoker, agentRunner);
  }

  public static class Context {

    private static final AtomicIntegerFieldUpdater<MimirPublisher.Context> IS_CONCLUDED_UPDATER =
        newUpdater(MimirPublisher.Context.class, "isConcluded");
    private volatile int isConcluded;

    private Duration retryInterval;
    private Duration publishInterval;
    private EpochClock epochClock;
    private boolean useAgentInvoker;
    private ErrorHandler errorHandler;
    private IdleStrategy idleStrategy;
    private String url;
    private Integer writeLimit;
    private Integer writeQueueCapacity;
    private ManyToOneConcurrentArrayQueue<WriteRequest> writeQueue;

    public Context() {}

    private void conclude() {
      if (0 != IS_CONCLUDED_UPDATER.getAndSet(this, 1)) {
        throw new ConcurrentModificationException();
      }

      if (retryInterval == null) {
        retryInterval = Duration.ofSeconds(3);
      }

      if (publishInterval == null) {
        publishInterval = Duration.ofSeconds(5);
      }

      if (epochClock == null) {
        epochClock = SystemEpochClock.INSTANCE;
      }

      if (errorHandler == null) {
        errorHandler = ex -> LOGGER.error("Exception occurred: ", ex);
      }

      if (idleStrategy == null) {
        idleStrategy = new BackoffIdleStrategy();
      }

      Objects.requireNonNull(url, "url");

      if (writeLimit == null) {
        writeLimit = 100;
      }

      if (writeQueueCapacity == null) {
        writeQueueCapacity = 64 * 1024;
      }

      if (writeQueue == null) {
        writeQueue = new ManyToOneConcurrentArrayQueue<>(writeQueueCapacity);
      }
    }

    public Duration retryInterval() {
      return retryInterval;
    }

    public Context retryInterval(Duration retryInterval) {
      this.retryInterval = retryInterval;
      return this;
    }

    public Duration publishInterval() {
      return publishInterval;
    }

    public Context publishInterval(Duration publishInterval) {
      this.publishInterval = publishInterval;
      return this;
    }

    public EpochClock epochClock() {
      return epochClock;
    }

    public Context epochClock(EpochClock epochClock) {
      this.epochClock = epochClock;
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

    public IdleStrategy idleStrategy() {
      return idleStrategy;
    }

    public Context idleStrategy(IdleStrategy idleStrategy) {
      this.idleStrategy = idleStrategy;
      return this;
    }

    public String url() {
      return url;
    }

    public Context url(String url) {
      this.url = url;
      return this;
    }

    public Integer writeLimit() {
      return writeLimit;
    }

    public Context writeLimit(Integer writeLimit) {
      this.writeLimit = writeLimit;
      return this;
    }

    public Integer writeQueueCapacity() {
      return writeQueueCapacity;
    }

    public Context writeQueueCapacity(Integer writeQueueCapacity) {
      this.writeQueueCapacity = writeQueueCapacity;
      return this;
    }

    public ManyToOneConcurrentArrayQueue<WriteRequest> writeQueue() {
      return writeQueue;
    }

    public Context writeQueue(ManyToOneConcurrentArrayQueue<WriteRequest> writeQueue) {
      this.writeQueue = writeQueue;
      return this;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Context.class.getSimpleName() + "[", "]")
          .add("retryInterval=" + retryInterval)
          .add("publishInterval=" + publishInterval)
          .add("epochClock=" + epochClock)
          .add("useAgentInvoker=" + useAgentInvoker)
          .add("errorHandler=" + errorHandler)
          .add("idleStrategy=" + idleStrategy)
          .add("url='" + url + "'")
          .add("writeLimit=" + writeLimit)
          .add("writeQueueCapacity=" + writeQueueCapacity)
          .add("writeQueue=" + writeQueue)
          .toString();
    }
  }

  /**
   * Wrapper around {@code write-queue} for the {@link WriteRequest} objects. Being used to push
   * {@link WriteRequest}(s) that later will be published to Mimir.
   */
  public static class WriteProxy {

    private final ManyToOneConcurrentArrayQueue<WriteRequest> writeQueue;

    private WriteProxy(ManyToOneConcurrentArrayQueue<WriteRequest> writeQueue) {
      this.writeQueue = writeQueue;
    }

    /**
     * Pushes {@link WriteRequest} to the queue (if cannot fit in, request is dropped).
     *
     * @param request {@code prometheus.WriteRequest}
     */
    public void push(WriteRequest request) {
      writeQueue.offer(request);
    }
  }
}
