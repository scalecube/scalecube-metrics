package io.scalecube.metrics.aeron;

import static io.aeron.AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_ERROR_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_MAX_CYCLE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

import io.scalecube.metrics.KeyFlyweight;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2ObjectHashMap;

public class ArchiveCountersAdapter {

  private static final Pattern KIND_PATTERN = Pattern.compile("^(archive-[^\\s:]+)");

  private static final int RECORDING_ID_OFFSET = 0;
  private static final int SESSION_ID_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;
  private static final int SOURCE_IDENTITY_LENGTH_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
  private static final int SOURCE_IDENTITY_OFFSET = SOURCE_IDENTITY_LENGTH_OFFSET + SIZE_OF_INT;

  private final KeyFlyweight keyFlyweight = new KeyFlyweight();

  public static void populate(Int2ObjectHashMap<KeyConverter> map) {
    final var adapter = new ArchiveCountersAdapter();
    map.put(ARCHIVE_ERROR_COUNT_TYPE_ID, adapter::archiveErrorCount);
    map.put(ARCHIVE_CONTROL_SESSIONS_TYPE_ID, adapter::archiveControlSessionCount);
    map.put(ARCHIVE_MAX_CYCLE_TIME_TYPE_ID, adapter::archiveMaxCycleTime);
    map.put(
        ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
        adapter::archiveCycleTimeThresholdExceededCount);
    map.put(ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID, adapter::archiveRecorderMaxWriteTime);
    map.put(ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID, adapter::archiveRecorderTotalWriteBytes);
    map.put(ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID, adapter::archiveRecorderTotalWriteTime);
    map.put(ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID, adapter::archiveReplayerMaxReadTime);
    map.put(ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID, adapter::archiveReplayerTotalReadBytes);
    map.put(ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID, adapter::archiveReplayerTotalReadTime);
    map.put(ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID, adapter::archiveRecordingSessionCount);
    map.put(ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID, adapter::archiveReplaySessionCount);
    map.put(ARCHIVE_RECORDING_POSITION_TYPE_ID, adapter::archiveRecordingPosition);
  }

  private DirectBuffer archiveErrorCount(DirectBuffer keyBuffer, String label) {
    return newKey(2)
        .stringValue("name", "archive_error_count")
        .longValue("archiveId", keyBuffer.getLong(0))
        .buffer();
  }

  private DirectBuffer archiveControlSessionCount(DirectBuffer keyBuffer, String label) {
    return newKey(2)
        .stringValue("name", "archive_control_session_count")
        .longValue("archiveId", keyBuffer.getLong(0))
        .buffer();
  }

  private DirectBuffer archiveMaxCycleTime(DirectBuffer keyBuffer, String label) {
    return newKey(3)
        .stringValue("name", "archive_max_cycle_time_nanos")
        .longValue("archiveId", keyBuffer.getLong(0))
        .stringValue("kind", getKind(label))
        .buffer();
  }

  private DirectBuffer archiveCycleTimeThresholdExceededCount(
      DirectBuffer keyBuffer, String label) {
    return newKey(3)
        .stringValue("name", "archive_cycle_time_threshold_exceeded_count")
        .longValue("archiveId", keyBuffer.getLong(0))
        .stringValue("kind", getKind(label))
        .buffer();
  }

  private DirectBuffer archiveRecorderMaxWriteTime(DirectBuffer keyBuffer, String label) {
    return newKey(2)
        .stringValue("name", "archive_recorder_max_write_time_nanos")
        .longValue("archiveId", keyBuffer.getLong(0))
        .buffer();
  }

  private DirectBuffer archiveRecorderTotalWriteBytes(DirectBuffer keyBuffer, String label) {
    return newKey(2)
        .stringValue("name", "archive_recorder_total_write_bytes")
        .longValue("archiveId", keyBuffer.getLong(0))
        .buffer();
  }

  private DirectBuffer archiveRecorderTotalWriteTime(DirectBuffer keyBuffer, String label) {
    return newKey(2)
        .stringValue("name", "archive_recorder_total_write_time_nanos")
        .longValue("archiveId", keyBuffer.getLong(0))
        .buffer();
  }

  private DirectBuffer archiveReplayerMaxReadTime(DirectBuffer keyBuffer, String label) {
    return newKey(2)
        .stringValue("name", "archive_replayer_max_read_time_nanos")
        .longValue("archiveId", keyBuffer.getLong(0))
        .buffer();
  }

  private DirectBuffer archiveReplayerTotalReadBytes(DirectBuffer keyBuffer, String label) {
    return newKey(2)
        .stringValue("name", "archive_replayer_total_read_bytes")
        .longValue("archiveId", keyBuffer.getLong(0))
        .buffer();
  }

  private DirectBuffer archiveReplayerTotalReadTime(DirectBuffer keyBuffer, String label) {
    return newKey(2)
        .stringValue("name", "archive_replayer_total_read_time_nanos")
        .longValue("archiveId", keyBuffer.getLong(0))
        .buffer();
  }

  private DirectBuffer archiveRecordingSessionCount(DirectBuffer keyBuffer, String label) {
    return newKey(2)
        .stringValue("name", "archive_recording_session_count")
        .longValue("archiveId", keyBuffer.getLong(0))
        .buffer();
  }

  private DirectBuffer archiveReplaySessionCount(DirectBuffer keyBuffer, String label) {
    return newKey(2)
        .stringValue("name", "archive_replay_session_count")
        .longValue("archiveId", keyBuffer.getLong(0))
        .buffer();
  }

  private DirectBuffer archiveRecordingPosition(DirectBuffer keyBuffer, String label) {
    final var sourceIdentityLength = keyBuffer.getInt(SOURCE_IDENTITY_LENGTH_OFFSET);
    final var archiveIdOffset = SOURCE_IDENTITY_OFFSET + sourceIdentityLength;
    final var archiveId = keyBuffer.getLong(archiveIdOffset);
    final var streamId = getStreamId(label);

    return newKey(3)
        .stringValue("name", "archive_recording_position")
        .longValue("archiveId", archiveId)
        .intValue("streamId", streamId)
        .buffer();
  }

  private KeyFlyweight newKey(int tagsCount) {
    return keyFlyweight.wrap(new ExpandableArrayBuffer(), 0).tagsCount(tagsCount);
  }

  private static String getKind(String input) {
    Matcher matcher = KIND_PATTERN.matcher(input);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      throw new IllegalArgumentException("Wrong input: " + input);
    }
  }

  private static int getStreamId(String label) {
    String[] tokens = label.split(" ");
    if (tokens.length >= 4) {
      try {
        return Integer.parseInt(tokens[3]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Wrong input: " + label, e);
      }
    } else {
      throw new IllegalArgumentException("Wrong input: " + label);
    }
  }
}
