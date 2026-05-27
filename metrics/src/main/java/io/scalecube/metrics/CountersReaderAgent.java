package io.scalecube.metrics;

import static io.scalecube.metrics.CounterTags.COUNTER_VISIBILITY;
import static io.scalecube.metrics.CounterTags.WRITE_EPOCH_ID;
import static io.scalecube.metrics.CounterVisibility.PRIVATE;
import static io.scalecube.metrics.CountersRegistry.Context.COUNTERS_FILE;
import static org.agrona.IoUtil.mapExistingFile;

import io.scalecube.metrics.CountersRegistry.Context;
import io.scalecube.metrics.CountersRegistry.LayoutDescriptor;
import java.io.File;
import java.nio.channels.FileChannel.MapMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.agrona.BufferUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.Agent;
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
    READ_COUNTERS,
    CLEANUP,
    CLOSED
  }

  private final String roleName;
  private final File countersDir;
  private final boolean warnIfCountersNotExists;
  private final EpochClock epochClock;
  private final long readInterval;
  private final CountersHandler countersHandler;

  private long lastReadInterval;
  private final UnsafeBuffer headerBuffer = new UnsafeBuffer();
  private final KeyCodec keyCodec = new KeyCodec();
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
    this.readInterval = readInterval.toMillis();
    this.countersHandler = countersHandler;
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
    if (lastReadInterval + readInterval > now) {
      return 0;
    } else {
      lastReadInterval = now;
    }

    final var countersFile = new File(countersDir, COUNTERS_FILE);
    if (!countersFile.exists()) {
      if (warnIfCountersNotExists) {
        LOGGER.warn("[{}] {} not exists", roleName(), countersFile);
      }
      state(State.CLEANUP);
      return 0;
    }

    final var countersByteBuffer = mapExistingFile(countersFile, MapMode.READ_ONLY, COUNTERS_FILE);
    try {
      headerBuffer.wrap(countersByteBuffer, 0, LayoutDescriptor.HEADER_LENGTH);
      final var countersValuesBufferLength =
          LayoutDescriptor.countersValuesBufferLength(headerBuffer);

      if (countersValuesBufferLength <= 0) {
        state(State.CLEANUP);
        return 0;
      }

      final var countersReader =
          new CountersReader(
              LayoutDescriptor.createCountersMetaDataBuffer(countersByteBuffer, headerBuffer),
              LayoutDescriptor.createCountersValuesBuffer(countersByteBuffer, headerBuffer));

      countersHandler.accept(now, readCounters(countersReader));
    } finally {
      BufferUtil.free(countersByteBuffer);
    }

    return 0;
  }

  private List<CounterDescriptor> readCounters(CountersReader countersReader) {
    final var snapshot = new ArrayList<CounterDescriptor>();
    final var writeGroups = new Int2ObjectHashMap<IntHashSet>();

    countersReader.forEach(
        (counterId, typeId, keyBuffer, label) -> {
          final var value = countersReader.getCounterValue(counterId);
          final var keyBufferCopy = new UnsafeBuffer(new byte[keyBuffer.capacity()]);
          keyBuffer.getBytes(0, keyBufferCopy, 0, keyBufferCopy.capacity());
          final var counter = new CounterDescriptor(counterId, typeId, value, keyBufferCopy, label);
          final var key = keyCodec.decodeKey(keyBufferCopy, 0);

          if (hasWriteEpochId(key)) {
            writeGroups.computeIfAbsent(writeEpochId(key), k -> new IntHashSet()).add(counterId);
          } else if (!hasPrivateVisibility(key)) {
            snapshot.add(counter);
          }
        });

    tryReadWriteGroups(countersReader, snapshot, writeGroups);
    return snapshot;
  }

  private void tryReadWriteGroups(
      CountersReader countersReader,
      List<CounterDescriptor> snapshot,
      Int2ObjectHashMap<IntHashSet> writeGroups) {
    writeGroups.forEachInt(
        (epochId, counterIds) -> {
          final var epochCounter = CounterDescriptor.getCounter(countersReader, epochId);
          if (epochCounter != null) {
            for (int attempt = 0; attempt < 16; attempt++) {
              final var epochBefore = countersReader.getCounterValue(epochId);

              // odd => writer in progress, retry
              if ((epochBefore & 1) != 0) {
                Thread.onSpinWait();
                continue;
              }

              // try read counters, skip if not present
              final var counters = getCountersByIds(countersReader, counterIds);
              if (counters == null) {
                break;
              }

              final long epochAfter = countersReader.getCounterValue(epochId);
              if (epochAfter != epochBefore) {
                Thread.onSpinWait();
                continue;
              }

              snapshot.addAll(counters);
              break;
            }
          }
        });
  }

  private static List<CounterDescriptor> getCountersByIds(
      CountersReader countersReader, IntHashSet counterIds) {
    final var list = new ArrayList<CounterDescriptor>();
    for (var counterId : counterIds) {
      final var counter = CounterDescriptor.getCounter(countersReader, counterId);
      if (counter != null) {
        list.add(counter);
      } else {
        return null;
      }
    }
    return list;
  }

  private static boolean hasWriteEpochId(Key key) {
    return writeEpochId(key) != null;
  }

  private static Integer writeEpochId(Key key) {
    return key.intValue(WRITE_EPOCH_ID);
  }

  private static boolean hasPrivateVisibility(Key key) {
    return key.enumValue(COUNTER_VISIBILITY, CounterVisibility::get) == PRIVATE;
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
