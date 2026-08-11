package io.scalecube.metrics;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.IoUtil.delete;
import static org.agrona.IoUtil.ensureDirectoryExists;
import static org.agrona.IoUtil.mapNewFile;
import static org.agrona.concurrent.status.CountersReader.COUNTER_LENGTH;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ConcurrentCountersManager;
import org.agrona.concurrent.status.CountersManager;

/**
 * Manages {@link CountersManager} with buffers backed by memory-mapped file. Provides centralized
 * mechanism to initialize and manage {@link CountersManager}.
 */
public class CountersRegistry implements AutoCloseable {

  private final Context context;
  private final CountersManager countersManager;

  private CountersRegistry(Context context) {
    this.context = context;
    try {
      context.conclude();
      countersManager =
          new ConcurrentCountersManager(
              new UnsafeBuffer(context.countersMetaDataBuffer()),
              new UnsafeBuffer(context.countersValuesBuffer()));
    } catch (ConcurrentModificationException e) {
      throw e;
    } catch (Exception e) {
      CloseHelper.quietClose(context::close);
      throw e;
    }
  }

  public static CountersRegistry create() {
    return create(new Context());
  }

  public static CountersRegistry create(Context context) {
    return new CountersRegistry(context);
  }

  public Context context() {
    return context;
  }

  public CountersManager countersManager() {
    return countersManager;
  }

  @Override
  public void close() {
    CloseHelper.quietClose(context::close);
  }

  public static class Context {

    public static final String COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME =
        "scalecube.metrics.counters.countersValuesBufferLength";
    public static final String COUNTERS_DIR_NAME_PROP_NAME =
        "scalecube.metrics.counters.countersDirectoryName";
    public static final String DIR_DELETE_ON_SHUTDOWN_PROP_NAME =
        "scalecube.metrics.counters.dirDeleteOnShutdown";

    public static final String COUNTERS_FILE = "counters.dat";
    public static final String DEFAULT_COUNTERS_DIR_NAME;
    public static final int DEFAULT_COUNTERS_VALUES_BUFFER_LENGTH = 2 * 1024 * 1024;

    static {
      String baseDirName = null;

      if (SystemUtil.isLinux()) {
        final File devShmDir = new File("/dev/shm");
        if (devShmDir.exists()) {
          baseDirName = "/dev/shm/counters";
        }
      }

      if (baseDirName == null) {
        baseDirName = SystemUtil.tmpDirName() + "counters";
      }

      DEFAULT_COUNTERS_DIR_NAME = baseDirName + '-' + System.getProperty("user.name", "default");
    }

    private static final AtomicIntegerFieldUpdater<Context> IS_CONCLUDED_UPDATER =
        newUpdater(Context.class, "isConcluded");
    private static final AtomicIntegerFieldUpdater<Context> IS_CLOSED_UPDATER =
        newUpdater(Context.class, "isClosed");

    private volatile int isConcluded;
    private volatile int isClosed;

    private int countersValuesBufferLength;
    private String countersDirectoryName;
    private File countersDir;
    private boolean dirDeleteOnShutdown;
    private MappedByteBuffer mappedByteBuffer;
    private UnsafeBuffer countersMetaDataBuffer;
    private UnsafeBuffer countersValuesBuffer;

    public Context() {
      this(System.getProperties());
    }

    public Context(Properties props) {
      countersDirectoryName(props);
      countersValuesBufferLength(props);
      dirDeleteOnShutdown(props);
    }

    private static String getProperty(Properties props, String name) {
      final var value = props.getProperty(name);
      return "@null".equals(value) ? null : value;
    }

    private static String getProperty(Properties props, String name, String defaultValue) {
      final var value = getProperty(props, name);
      return value != null ? value : defaultValue;
    }

    private static int getProperty(Properties props, String name, int defaultValue) {
      final var value = getProperty(props, name);
      return value != null ? Integer.parseInt(value) : defaultValue;
    }

    private static boolean getProperty(Properties props, String name, boolean defaultValue) {
      final var value = getProperty(props, name);
      return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }

    private void conclude() {
      if (0 != IS_CONCLUDED_UPDATER.getAndSet(this, 1)) {
        throw new ConcurrentModificationException();
      }

      concludeCountersDirectory();
      concludeCountersBuffers();
    }

    private void concludeCountersDirectory() {
      if (countersDir == null) {
        try {
          countersDir = new File(countersDirectoryName).getCanonicalFile();
        } catch (IOException e) {
          LangUtil.rethrowUnchecked(e);
        }
      }

      if (countersDir.isDirectory()) {
        delete(countersDir, false);
      }

      ensureDirectoryExists(countersDir, "counters");
    }

    private void concludeCountersBuffers() {
      final var min = DEFAULT_COUNTERS_VALUES_BUFFER_LENGTH;
      if (countersValuesBufferLength < min) {
        throw new IllegalArgumentException("countersValuesBufferLength must be at least " + min);
      }
      if (!BitUtil.isPowerOfTwo(countersValuesBufferLength)) {
        throw new IllegalArgumentException("countersValuesBufferLength must be power of 2");
      }

      mappedByteBuffer =
          mapNewFile(
              new File(countersDir, COUNTERS_FILE),
              LayoutDescriptor.countersFileLength(countersValuesBufferLength));

      final var headerBuffer = LayoutDescriptor.createHeaderBuffer(mappedByteBuffer);
      final var startTimestamp = ManagementFactory.getRuntimeMXBean().getStartTime();
      final var pid = ManagementFactory.getRuntimeMXBean().getPid();
      LayoutDescriptor.fillHeaderBuffer(
          headerBuffer, startTimestamp, pid, countersValuesBufferLength);
      mappedByteBuffer.force();

      countersMetaDataBuffer =
          LayoutDescriptor.createCountersMetaDataBuffer(mappedByteBuffer, headerBuffer);
      countersValuesBuffer =
          LayoutDescriptor.createCountersValuesBuffer(mappedByteBuffer, headerBuffer);
    }

    public int countersValuesBufferLength() {
      return countersValuesBufferLength;
    }

    public Context countersValuesBufferLength(int countersValuesBufferLength) {
      this.countersValuesBufferLength = countersValuesBufferLength;
      return this;
    }

    public Context countersValuesBufferLength(Properties props) {
      return countersValuesBufferLength(
          getProperty(
              props,
              COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME,
              DEFAULT_COUNTERS_VALUES_BUFFER_LENGTH));
    }

    public String countersDirectoryName() {
      return countersDirectoryName;
    }

    public Context countersDirectoryName(String countersDirectoryName) {
      this.countersDirectoryName = countersDirectoryName;
      return this;
    }

    public Context countersDirectoryName(Properties props) {
      return countersDirectoryName(
          getProperty(props, COUNTERS_DIR_NAME_PROP_NAME, DEFAULT_COUNTERS_DIR_NAME));
    }

    public File countersDir() {
      return countersDir;
    }

    public Context countersDir(File countersDir) {
      this.countersDir = countersDir;
      return this;
    }

    public boolean dirDeleteOnShutdown() {
      return dirDeleteOnShutdown;
    }

    public Context dirDeleteOnShutdown(boolean dirDeleteOnShutdown) {
      this.dirDeleteOnShutdown = dirDeleteOnShutdown;
      return this;
    }

    public Context dirDeleteOnShutdown(Properties props) {
      return dirDeleteOnShutdown(getProperty(props, DIR_DELETE_ON_SHUTDOWN_PROP_NAME, false));
    }

    UnsafeBuffer countersMetaDataBuffer() {
      return countersMetaDataBuffer;
    }

    UnsafeBuffer countersValuesBuffer() {
      return countersValuesBuffer;
    }

    public static String generateCountersDirectoryName() {
      return DEFAULT_COUNTERS_DIR_NAME + "-" + UUID.randomUUID();
    }

    private void close() {
      if (IS_CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
        BufferUtil.free(mappedByteBuffer);
        mappedByteBuffer = null;
        if (dirDeleteOnShutdown && countersDir != null) {
          delete(countersDir, false);
        }
      }
    }
  }

  public static class LayoutDescriptor {

    public static final int HEADER_LENGTH = CACHE_LINE_LENGTH * 2;
    public static final int START_TIMESTAMP_OFFSET = 0;
    public static final int PID_OFFSET = 8;
    public static final int COUNTERS_VALUES_BUFFER_LENGTH_OFFSET = 16;

    private LayoutDescriptor() {}

    public static int countersFileLength(int countersValuesBufferLength) {
      return LayoutDescriptor.HEADER_LENGTH
          + LayoutDescriptor.countersMetaDataBufferLength(countersValuesBufferLength)
          + countersValuesBufferLength;
    }

    public static int countersMetaDataBufferLength(int countersValuesBufferLength) {
      return countersValuesBufferLength * (METADATA_LENGTH / COUNTER_LENGTH);
    }

    public static UnsafeBuffer createHeaderBuffer(ByteBuffer buffer) {
      return new UnsafeBuffer(buffer, 0, HEADER_LENGTH);
    }

    public static long startTimestamp(DirectBuffer headerBuffer) {
      return headerBuffer.getLong(START_TIMESTAMP_OFFSET);
    }

    public static long pid(DirectBuffer headerBuffer) {
      return headerBuffer.getLong(PID_OFFSET);
    }

    public static int countersValuesBufferLength(DirectBuffer headerBuffer) {
      return headerBuffer.getInt(COUNTERS_VALUES_BUFFER_LENGTH_OFFSET);
    }

    public static UnsafeBuffer createCountersMetaDataBuffer(
        ByteBuffer buffer, DirectBuffer headerBuffer) {
      final var offset = HEADER_LENGTH;
      final var countersValuesBufferLength = countersValuesBufferLength(headerBuffer);
      final var length = countersMetaDataBufferLength(countersValuesBufferLength);
      return new UnsafeBuffer(buffer, offset, length);
    }

    public static UnsafeBuffer createCountersValuesBuffer(
        ByteBuffer buffer, DirectBuffer headerBuffer) {
      final var countersValuesBufferLength = countersValuesBufferLength(headerBuffer);
      final var offset = HEADER_LENGTH + countersMetaDataBufferLength(countersValuesBufferLength);
      final var length = countersValuesBufferLength;
      return new UnsafeBuffer(buffer, offset, length);
    }

    public static boolean isCountersHeaderLengthSufficient(int length) {
      return length >= HEADER_LENGTH;
    }

    public static boolean isCountersFileLengthSufficient(
        DirectBuffer headerBuffer, int fileLength) {
      final var countersValuesBufferLength = countersValuesBufferLength(headerBuffer);
      final var countersMetaDataBufferLength =
          countersMetaDataBufferLength(countersValuesBufferLength);
      final var totalLength =
          HEADER_LENGTH + countersMetaDataBufferLength + countersValuesBufferLength;
      return totalLength >= fileLength;
    }

    public static boolean isCountersActive(
        DirectBuffer headerBuffer, long startTimestamp, long pid) {
      return startTimestamp(headerBuffer) == startTimestamp && pid(headerBuffer) == pid;
    }

    public static void fillHeaderBuffer(
        UnsafeBuffer headerBuffer, long startTimestamp, long pid, int countersValuesBufferLength) {
      headerBuffer.putLong(START_TIMESTAMP_OFFSET, startTimestamp);
      headerBuffer.putLong(PID_OFFSET, pid);
      headerBuffer.putInt(COUNTERS_VALUES_BUFFER_LENGTH_OFFSET, countersValuesBufferLength);
    }
  }
}
