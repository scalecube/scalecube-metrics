package io.scalecube.metrics;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.IoUtil.delete;
import static org.agrona.IoUtil.ensureDirectoryExists;
import static org.agrona.IoUtil.mapExistingFile;
import static org.agrona.IoUtil.mapNewFile;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for allocating and aggregating metrics (histograms, tps values). Responsible for
 * publishing aggregated metrics data to the many-to-one ring-buffer for further processing. Can run
 * in multiple instances, but metrics aggregation occurs only per concrete instance.
 *
 * @see HistogramMetric
 * @see TpsMetric
 * @see MetricsTransmitter
 */
public class MetricsRecorder implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsRecorder.class);

  private final Context context;

  private final AgentInvoker agentInvoker;
  private final AgentRunner agentRunner;
  private final Queue<Object> metricsQueue = new ConcurrentLinkedDeque<>();

  private MetricsRecorder(Context context) {
    try {
      context.conclude();
      this.context = context;

      final var agent =
          new MetricsRecorderAgent(
              metricsQueue,
              context.epochClock(),
              context.cachedEpochClock(),
              context.metricsPublication());

      if (context.useAgentInvoker()) {
        agentRunner = null;
        agentInvoker = new AgentInvoker(context.errorHandler(), null, agent);
      } else {
        agentInvoker = null;
        agentRunner = new AgentRunner(context.idleStrategy(), context.errorHandler(), null, agent);
      }
    } catch (ConcurrentModificationException e) {
      throw e;
    } catch (Exception e) {
      CloseHelper.quietClose(context::close);
      throw e;
    }
  }

  /**
   * Launch {@link MetricsRecorder} with default {@link Context}.
   *
   * @return newly started {@link MetricsRecorder}
   */
  public static MetricsRecorder launch() {
    return launch(new Context());
  }

  /**
   * Launch {@link MetricsRecorder} with provided {@link Context}.
   *
   * @param context context
   * @return newly started {@link MetricsRecorder}
   */
  public static MetricsRecorder launch(Context context) {
    final var metricsRegistry = new MetricsRecorder(context);
    if (metricsRegistry.agentInvoker != null) {
      metricsRegistry.agentInvoker.start();
    } else {
      AgentRunner.startOnThread(metricsRegistry.agentRunner);
    }
    return metricsRegistry;
  }

  /**
   * Allocates new {@link HistogramMetric}.
   *
   * @param consumer consumer for {@link KeyFlyweight}
   * @param highestTrackableValue highestTrackableValue for {@link org.HdrHistogram.Histogram}
   * @param conversionFactor multiplier for data conversion of the histogram values
   * @param resolutionMs window size of measurement in milliseconds, it controls how often
   *     histograms are cut off and flushed
   * @return newly created {@link HistogramMetric}
   */
  public HistogramMetric newHistogram(
      Consumer<KeyFlyweight> consumer,
      long highestTrackableValue,
      double conversionFactor,
      long resolutionMs) {
    final var keyFlyweight = new KeyFlyweight();
    final var keyBuffer = new ExpandableArrayBuffer();
    consumer.accept(keyFlyweight.wrap(keyBuffer, 0));
    final var metric =
        new HistogramRecorder(
            keyFlyweight.buffer(), highestTrackableValue, conversionFactor, resolutionMs);
    metricsQueue.add(metric);
    return metric;
  }

  /**
   * Allocates new {@link TpsMetric}.
   *
   * @param consumer consumer for {@link KeyFlyweight}
   * @return newly created {@link TpsMetric}
   */
  public TpsMetric newTps(Consumer<KeyFlyweight> consumer) {
    final var keyFlyweight = new KeyFlyweight();
    final var keyBuffer = new ExpandableArrayBuffer();
    consumer.accept(keyFlyweight.wrap(keyBuffer, 0));
    final var metric = new TpsRecorder(keyFlyweight.buffer());
    metricsQueue.add(metric);
    return metric;
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
    context.close();
  }

  public static class Context {

    public static final String METRICS_DIRECTORY_NAME_PROP_NAME =
        "scalecube.metrics.recorder.metricsDirectoryName";
    public static final String DIR_DELETE_ON_SHUTDOWN_PROP_NAME =
        "scalecube.metrics.recorder.dirDeleteOnShutdown";
    public static final String METRICS_BUFFER_LENGTH_PROP_NAME =
        "scalecube.metrics.recorder.metricsBufferLength";
    public static final String IDLE_STRATEGY_PROP_NAME = "scalecube.metrics.recorder.idleStrategy";

    public static final String METRICS_FILE = "metrics.dat";
    public static final String DEFAULT_METRICS_DIR_NAME;
    public static final int DEFAULT_METRICS_BUFFER_LENGTH = 32 * 1024 * 1024;

    static {
      String baseDirName = null;

      if (SystemUtil.isLinux()) {
        final File devShmDir = new File("/dev/shm");
        if (devShmDir.exists()) {
          baseDirName = "/dev/shm/metrics";
        }
      }

      if (baseDirName == null) {
        baseDirName = SystemUtil.tmpDirName() + "metrics";
      }

      DEFAULT_METRICS_DIR_NAME = baseDirName + '-' + System.getProperty("user.name", "default");
    }

    private static final AtomicIntegerFieldUpdater<Context> IS_CONCLUDED_UPDATER =
        newUpdater(Context.class, "isConcluded");
    private static final AtomicIntegerFieldUpdater<Context> IS_CLOSED_UPDATER =
        newUpdater(Context.class, "isClosed");

    private volatile int isConcluded;
    private volatile int isClosed;

    private File metricsDir;
    private String metricsDirectoryName = System.getProperty(METRICS_DIRECTORY_NAME_PROP_NAME);
    private boolean dirDeleteOnShutdown = Boolean.getBoolean(DIR_DELETE_ON_SHUTDOWN_PROP_NAME);
    private EpochClock epochClock;
    private CachedEpochClock cachedEpochClock;
    private int metricsBufferLength = Integer.getInteger(METRICS_BUFFER_LENGTH_PROP_NAME, 0);
    private MappedByteBuffer metricsByteBuffer;
    private ManyToOneRingBuffer metricsBuffer;
    private boolean useAgentInvoker;
    private ErrorHandler errorHandler;
    private IdleStrategy idleStrategy;

    public Context() {}

    public Context(Properties props) {
      metricsDirectoryName(props.getProperty(METRICS_DIRECTORY_NAME_PROP_NAME));
      dirDeleteOnShutdown(props.getProperty(DIR_DELETE_ON_SHUTDOWN_PROP_NAME));
      metricsBufferLength(props.getProperty(METRICS_BUFFER_LENGTH_PROP_NAME));
      idleStrategy(props.getProperty(IDLE_STRATEGY_PROP_NAME));
    }

    private void conclude() {
      if (0 != IS_CONCLUDED_UPDATER.getAndSet(this, 1)) {
        throw new ConcurrentModificationException();
      }

      concludeMetricsDirectory();
      concludeMetricsBuffer();

      if (epochClock == null) {
        epochClock = SystemEpochClock.INSTANCE;
      }

      if (cachedEpochClock == null) {
        cachedEpochClock = new CachedEpochClock();
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
        metricsDirectoryName = DEFAULT_METRICS_DIR_NAME;
      }

      if (metricsDir == null) {
        try {
          metricsDir = new File(metricsDirectoryName).getCanonicalFile();
        } catch (IOException e) {
          LangUtil.rethrowUnchecked(e);
        }
      }

      if (metricsDir.isDirectory()) {
        final var file = new File(metricsDir, METRICS_FILE);
        if (file.exists()) {
          final var mappedByteBuffer = mapExistingFile(file, METRICS_FILE);
          try {
            if (!LayoutDescriptor.isMetricsHeaderLengthSufficient(mappedByteBuffer.capacity())) {
              delete(metricsDir, false);
            } else {
              final var headerBuffer = LayoutDescriptor.createHeaderBuffer(mappedByteBuffer);
              final var startTimestamp = ManagementFactory.getRuntimeMXBean().getStartTime();
              final var pid = ManagementFactory.getRuntimeMXBean().getPid();
              if (!LayoutDescriptor.isMetricsActive(headerBuffer, startTimestamp, pid)) {
                delete(metricsDir, false);
              }
            }
          } finally {
            BufferUtil.free(mappedByteBuffer);
          }
        }
      }

      ensureDirectoryExists(metricsDir, "metrics");
    }

    private void concludeMetricsBuffer() {
      if (metricsBufferLength == 0) {
        metricsBufferLength = DEFAULT_METRICS_BUFFER_LENGTH;
      }

      final var min = DEFAULT_METRICS_BUFFER_LENGTH;
      if (metricsBufferLength < min) {
        throw new IllegalArgumentException("metricsBufferLength must be at least " + min);
      }
      if (!BitUtil.isPowerOfTwo(metricsBufferLength)) {
        throw new IllegalArgumentException("metricsBufferLength must be power of 2");
      }

      final var file = new File(metricsDir, METRICS_FILE);
      final var headerLength = LayoutDescriptor.HEADER_LENGTH;
      final var bufferLength = metricsBufferLength + TRAILER_LENGTH;
      final var totalLength = headerLength + bufferLength;

      if (!file.exists()) {
        final var mappedByteBuffer = mapNewFile(file, totalLength);
        try {
          final var headerBuffer = LayoutDescriptor.createHeaderBuffer(mappedByteBuffer);
          final var startTimestamp = ManagementFactory.getRuntimeMXBean().getStartTime();
          final var pid = ManagementFactory.getRuntimeMXBean().getPid();
          LayoutDescriptor.fillHeaderBuffer(headerBuffer, startTimestamp, pid, bufferLength);
          mappedByteBuffer.force();
        } finally {
          BufferUtil.free(mappedByteBuffer);
        }
      }

      metricsByteBuffer = mapExistingFile(file, METRICS_FILE);
      metricsBuffer =
          new ManyToOneRingBuffer(new UnsafeBuffer(metricsByteBuffer, headerLength, bufferLength));
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

    public boolean dirDeleteOnShutdown() {
      return dirDeleteOnShutdown;
    }

    public Context dirDeleteOnShutdown(boolean dirDeleteOnShutdown) {
      this.dirDeleteOnShutdown = dirDeleteOnShutdown;
      return this;
    }

    public Context dirDeleteOnShutdown(String dirDeleteOnShutdown) {
      if (dirDeleteOnShutdown != null) {
        return dirDeleteOnShutdown(Boolean.parseBoolean(dirDeleteOnShutdown));
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

    public CachedEpochClock cachedEpochClock() {
      return cachedEpochClock;
    }

    public Context cachedEpochClock(CachedEpochClock cachedEpochClock) {
      this.cachedEpochClock = cachedEpochClock;
      return this;
    }

    public int metricsBufferLength() {
      return metricsBufferLength;
    }

    public Context metricsBufferLength(int metricsBufferLength) {
      this.metricsBufferLength = metricsBufferLength;
      return this;
    }

    public Context metricsBufferLength(String metricsBufferLength) {
      if (metricsBufferLength != null) {
        return metricsBufferLength(Integer.parseInt(metricsBufferLength));
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
          return idleStrategy(
              (IdleStrategy) Class.forName(idleStrategy).getConstructor().newInstance());
        } catch (Exception ex) {
          LangUtil.rethrowUnchecked(ex);
        }
      }
      return this;
    }

    public IdleStrategy idleStrategy() {
      return idleStrategy;
    }

    MetricsPublication metricsPublication() {
      return new MetricsPublication(metricsBuffer);
    }

    public static String generateMetricsDirectoryName() {
      return DEFAULT_METRICS_DIR_NAME + "-" + UUID.randomUUID();
    }

    private void close() {
      if (IS_CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
        BufferUtil.free(metricsByteBuffer);
        metricsByteBuffer = null;
        if (dirDeleteOnShutdown && metricsDir != null) {
          delete(metricsDir, false);
        }
      }
    }
  }

  public static class LayoutDescriptor {

    public static final int HEADER_LENGTH = CACHE_LINE_LENGTH * 2;
    public static final int START_TIMESTAMP_OFFSET = 0;
    public static final int PID_OFFSET = 8;
    public static final int METRICS_BUFFER_LENGTH_OFFSET = 16;

    public static UnsafeBuffer createHeaderBuffer(ByteBuffer buffer) {
      return new UnsafeBuffer(buffer, 0, HEADER_LENGTH);
    }

    public static long startTimestamp(DirectBuffer headerBuffer) {
      return headerBuffer.getLong(START_TIMESTAMP_OFFSET);
    }

    public static long pid(DirectBuffer headerBuffer) {
      return headerBuffer.getLong(PID_OFFSET);
    }

    public static int metricsBufferLength(DirectBuffer headerBuffer) {
      return headerBuffer.getInt(METRICS_BUFFER_LENGTH_OFFSET);
    }

    public static boolean isMetricsHeaderLengthSufficient(int length) {
      return length >= HEADER_LENGTH;
    }

    public static boolean isMetricsFileLengthSufficient(DirectBuffer headerBuffer, int fileLength) {
      final var totalLength = HEADER_LENGTH + metricsBufferLength(headerBuffer);
      return totalLength >= fileLength;
    }

    public static boolean isMetricsActive(
        DirectBuffer headerBuffer, long startTimestamp, long pid) {
      return startTimestamp(headerBuffer) == startTimestamp && pid(headerBuffer) == pid;
    }

    public static void fillHeaderBuffer(
        UnsafeBuffer headerBuffer, long startTimestamp, long pid, int metricsBufferLength) {
      headerBuffer.putLong(START_TIMESTAMP_OFFSET, startTimestamp);
      headerBuffer.putLong(PID_OFFSET, pid);
      headerBuffer.putInt(METRICS_BUFFER_LENGTH_OFFSET, metricsBufferLength);
    }
  }

  static class MetricsPublication {

    private final ManyToOneRingBuffer metricsBuffer;

    MetricsPublication(ManyToOneRingBuffer metricsBuffer) {
      this.metricsBuffer = metricsBuffer;
    }

    void publish(DirectBuffer buffer, int offset, int length) {
      final var index = metricsBuffer.tryClaim(1, length);
      if (index > 0) {
        try {
          metricsBuffer.buffer().putBytes(index, buffer, offset, length);
          metricsBuffer.commit(index);
        } catch (Exception ex) {
          metricsBuffer.abort(index);
          LangUtil.rethrowUnchecked(ex);
        }
      }
    }
  }
}
