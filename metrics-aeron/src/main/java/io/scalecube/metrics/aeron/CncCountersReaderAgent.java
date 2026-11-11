package io.scalecube.metrics.aeron;

import static io.aeron.CncFileDescriptor.CNC_FILE;
import static io.aeron.CncFileDescriptor.createCountersMetaDataBuffer;
import static io.aeron.CncFileDescriptor.createCountersValuesBuffer;
import static io.aeron.CncFileDescriptor.createMetaDataBuffer;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.IoUtil.mapExistingFile;

import io.aeron.CncFileDescriptor;
import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import java.io.File;
import java.nio.channels.FileChannel.MapMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;
import org.agrona.BufferUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
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
    READ_COUNTERS,
    CLEANUP,
    CLOSED
  }

  private final String roleName;
  private final String aeronDirectoryName;
  private final boolean warnIfCncNotExists;
  private final EpochClock epochClock;
  private final long readInterval;
  private final CountersHandler countersHandler;

  private long nextReadTime;
  private final Int2ObjectHashMap<KeyConverter> keyConverters = new Int2ObjectHashMap<>();
  private State state = State.CLOSED;
  // Converters get a heap copy of the key, never a view of cnc.dat: the file is unmapped after each
  // read, so a key kept past it would read unmapped memory (SIGSEGV). Reused for every key: the
  // converters build their own key and keep no reference to this one.
  private final UnsafeBuffer keyBufferCopy =
      new UnsafeBuffer(new byte[CountersReader.MAX_KEY_LENGTH]);

  /**
   * Constructor.
   *
   * @param roleName roleName
   * @param aeronDirectoryName aeronDirectoryName
   * @param warnIfCncNotExists whether to log warning if counters file does not exist
   * @param epochClock epochClock
   * @param readInterval interval at which to read counters
   * @param countersHandler callback handler to process counters
   */
  public CncCountersReaderAgent(
      String roleName,
      String aeronDirectoryName,
      boolean warnIfCncNotExists,
      EpochClock epochClock,
      Duration readInterval,
      CountersHandler countersHandler) {
    this(
        roleName,
        aeronDirectoryName,
        warnIfCncNotExists,
        epochClock,
        readInterval,
        DriverCountersAdapter.NO_STREAMS,
        countersHandler);
  }

  /**
   * Constructor.
   *
   * @param roleName roleName
   * @param aeronDirectoryName aeronDirectoryName
   * @param warnIfCncNotExists whether to log warning if counters file does not exist
   * @param epochClock epochClock
   * @param readInterval interval at which to read counters
   * @param streams selects the streams whose driver position counters are read: {@link
   *     DriverCountersAdapter#NO_STREAMS}, {@link DriverCountersAdapter#ALL_STREAMS} or {@link
   *     DriverCountersAdapter#streams(int...)}
   * @param countersHandler callback handler to process counters
   */
  public CncCountersReaderAgent(
      String roleName,
      String aeronDirectoryName,
      boolean warnIfCncNotExists,
      EpochClock epochClock,
      Duration readInterval,
      IntPredicate streams,
      CountersHandler countersHandler) {
    this.roleName = roleName;
    this.aeronDirectoryName = aeronDirectoryName;
    this.warnIfCncNotExists = warnIfCncNotExists;
    this.epochClock = epochClock;
    this.readInterval = readInterval.toMillis();
    this.countersHandler = countersHandler;
    ArchiveCountersAdapter.populate(keyConverters);
    ClusterCountersAdapter.populate(keyConverters);
    ClusteredServiceCountersAdapter.populate(keyConverters);
    DriverCountersAdapter.populate(keyConverters, streams);
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  public void onStart() {
    if (state != State.CLOSED) {
      throw new IllegalStateException("Illegal state: " + state);
    }
    state(State.READ_COUNTERS);
  }

  @Override
  public int doWork() {
    try {
      return switch (state) {
        case READ_COUNTERS -> readCounters();
        case CLEANUP -> cleanup();
        case CLOSED -> 0;
      };
    } catch (Exception e) {
      state(State.CLEANUP);
      throw e;
    }
  }

  private int readCounters() {
    final var now = epochClock.time();
    if (now < nextReadTime) {
      return 0;
    } else {
      nextReadTime = now + readInterval;
    }

    final var cncFile = new File(aeronDirectoryName, CNC_FILE);
    if (!cncFile.exists()) {
      if (warnIfCncNotExists) {
        LOGGER.warn("[{}] {} not exists", roleName(), cncFile);
      }
      state(State.CLEANUP);
      return 0;
    }

    final var cncByteBuffer = mapExistingFile(cncFile, MapMode.READ_ONLY, CNC_FILE);
    try {
      final var cncMetaData = createMetaDataBuffer(cncByteBuffer);
      final var countersReader =
          new CountersReader(
              createCountersMetaDataBuffer(cncByteBuffer, cncMetaData),
              createCountersValuesBuffer(cncByteBuffer, cncMetaData),
              US_ASCII);
      countersHandler.accept(now, readCounters(countersReader));
    } finally {
      BufferUtil.free(cncByteBuffer);
    }

    return 0;
  }

  private List<CounterDescriptor> readCounters(CountersReader countersReader) {
    final var counterDescriptors = new ArrayList<CounterDescriptor>();
    countersReader.forEach(
        (counterId, typeId, keyBuffer, label) -> {
          final var keyConverter = keyConverters.get(typeId);
          if (keyConverter != null) {
            keyBuffer.getBytes(0, keyBufferCopy, 0, keyBuffer.capacity());
            // null: the converter skips this counter
            final var key = keyConverter.convert(keyBufferCopy, label);
            if (key != null) {
              counterDescriptors.add(
                  new CounterDescriptor(
                      counterId, typeId, countersReader.getCounterValue(counterId), key, null));
            }
          }
        });
    return counterDescriptors;
  }

  private int cleanup() {
    State previous = state;
    if (previous != State.CLOSED) { // when it comes from onClose()
      state(State.READ_COUNTERS);
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
