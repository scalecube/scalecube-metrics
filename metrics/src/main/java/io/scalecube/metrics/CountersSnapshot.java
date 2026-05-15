package io.scalecube.metrics;

import static io.scalecube.metrics.CounterTags.COUNTER_VISIBILITY;
import static io.scalecube.metrics.CounterTags.WRITE_EPOCH_ID;
import static io.scalecube.metrics.CounterVisibility.PRIVATE;
import static io.scalecube.metrics.CountersRegistry.Context.COUNTERS_FILE;
import static org.agrona.IoUtil.mapExistingFile;

import io.scalecube.metrics.CountersRegistry.Context;
import io.scalecube.metrics.CountersRegistry.LayoutDescriptor;
import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads counter files and created a snapshot of them. */
public class CountersSnapshot implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CountersSnapshot.class);

  private final String roleName;
  private final boolean warnIfCountersNotExists;

  private File countersFile;
  private MappedByteBuffer countersByteBuffer;
  private final UnsafeBuffer headerBuffer = new UnsafeBuffer();
  private final KeyCodec keyCodec = new KeyCodec();
  private long countersPid = -1;
  private int countersValuesBufferLength = -1;
  private CountersReader countersReader;
  private long countersStartTimestamp;
  private final boolean copyKeyBuffer;

  /**
   * Constructor.
   *
   * @param roleName role name
   * @param countersDir counters directory with {@link Context#COUNTERS_FILE}
   * @param warnIfCountersNotExists whether to log warning if counters file does not exist
   * @param copyKeyBuffer whether to create copies of keyBuffers
   */
  public CountersSnapshot(
      String roleName, File countersDir, boolean warnIfCountersNotExists, boolean copyKeyBuffer) {
    this.roleName = roleName;
    this.warnIfCountersNotExists = warnIfCountersNotExists;
    this.copyKeyBuffer = copyKeyBuffer;
    this.countersFile = new File(countersDir, COUNTERS_FILE);
  }

  public boolean attach() {
    if (!isActive()) {
      return false;
    }

    countersByteBuffer = mapExistingFile(countersFile, MapMode.READ_ONLY, COUNTERS_FILE);
    headerBuffer.wrap(countersByteBuffer, 0, LayoutDescriptor.HEADER_LENGTH);
    countersStartTimestamp = LayoutDescriptor.startTimestamp(headerBuffer);
    countersPid = LayoutDescriptor.pid(headerBuffer);
    countersValuesBufferLength = LayoutDescriptor.countersValuesBufferLength(headerBuffer);

    if (countersValuesBufferLength <= 0) {
      return false;
    }

    countersReader =
        new CountersReader(
            LayoutDescriptor.createCountersMetaDataBuffer(countersByteBuffer, headerBuffer),
            LayoutDescriptor.createCountersValuesBuffer(countersByteBuffer, headerBuffer));

    return true;
  }

  public List<CounterDescriptor> snapshotCounters() {
    final var snapshot = new ArrayList<CounterDescriptor>();
    final var writeGroups = new Int2ObjectHashMap<IntHashSet>();

    countersReader.forEach(
        (counterId, typeId, keyBuffer, label) -> {
          final var value = countersReader.getCounterValue(counterId);
          DirectBuffer keyBufferCopy = keyBuffer;
          if (copyKeyBuffer) {
            final var tmpBuffer = new UnsafeBuffer(new byte[keyBuffer.capacity()]);
            keyBuffer.getBytes(0, tmpBuffer, 0, tmpBuffer.capacity());
            keyBufferCopy = tmpBuffer;
          }
          final var counter = new CounterDescriptor(counterId, typeId, value, keyBufferCopy, label);
          final var key = keyCodec.decodeKey(keyBufferCopy, 0);

          if (hasWriteEpochId(key)) {
            writeGroups.computeIfAbsent(writeEpochId(key), k -> new IntHashSet()).add(counterId);
          } else if (!hasPrivateVisibility(key)) {
            snapshot.add(counter);
          }
        });

    trySnapshotWriteGroups(snapshot, writeGroups);
    return snapshot;
  }

  private void trySnapshotWriteGroups(
      List<CounterDescriptor> snapshot, Int2ObjectHashMap<IntHashSet> writeGroups) {
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
              final var counters = readCounters(counterIds);
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

  private List<CounterDescriptor> readCounters(IntHashSet counterIds) {
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

  public boolean isActive() {
    if (!countersFile.exists()) {
      if (warnIfCountersNotExists) {
        LOGGER.debug("{} not exists", countersFile);
      }
      return false;
    }

    final var buffer = mapExistingFile(countersFile, MapMode.READ_ONLY, COUNTERS_FILE);
    try {
      if (!LayoutDescriptor.isCountersHeaderLengthSufficient(buffer.capacity())) {
        LOGGER.debug("[{}] {} has invalid length", roleName, countersFile);
        return false;
      }
      headerBuffer.wrap(buffer, 0, LayoutDescriptor.HEADER_LENGTH);
      if (!LayoutDescriptor.isCountersFileLengthSufficient(headerBuffer, buffer.capacity())) {
        LOGGER.debug("[{}] {} has invalid length", roleName, countersFile);
        return false;
      }
      if (countersValuesBufferLength != -1
          && !LayoutDescriptor.isCountersActive(
              headerBuffer, countersStartTimestamp, countersPid)) {
        LOGGER.debug("[{}] {} is not active", roleName, countersFile);
        return false;
      }
    } finally {
      BufferUtil.free(buffer);
    }

    return true;
  }

  @Override
  public void close() {
    BufferUtil.free(countersByteBuffer);
    countersByteBuffer = null;
    countersFile = null;
    countersStartTimestamp = -1;
    countersPid = -1;
    countersValuesBufferLength = -1;
  }
}
