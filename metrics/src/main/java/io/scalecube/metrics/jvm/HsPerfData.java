package io.scalecube.metrics.jvm;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.agrona.DirectBuffer;

/**
 * Values of HotSpot {@code hsperfdata} file ({@code /tmp/hsperfdata_<user>/<pid>}), the
 * memory-mapped file every HotSpot JVM writes by default ({@code -XX:+UsePerfData}).
 *
 * <p>Layout as defined in HotSpot {@code perfMemory.hpp} ({@code PerfDataPrologue}, {@code
 * PerfDataEntry}), walked the same way as JDK reader {@code
 * sun.jvmstat.perfdata.monitor.v2_0.PerfDataBuffer}. Only scalar {@code long} entries and string
 * entries are kept, other entries are skipped.
 *
 * @param longs scalar long entries by name
 * @param strings string entries by name
 */
public record HsPerfData(Map<String, Long> longs, Map<String, String> strings) {

  static final int MAGIC = 0xcafec0c0;
  static final int SUPPORTED_MAJOR_VERSION = 2;

  // PerfDataPrologue
  static final int MAGIC_OFFSET = 0;
  static final int BYTE_ORDER_OFFSET = 4;
  static final int MAJOR_VERSION_OFFSET = 5;
  static final int ACCESSIBLE_OFFSET = 7;
  static final int USED_OFFSET = 8;
  static final int ENTRY_OFFSET_OFFSET = 24;
  static final int NUM_ENTRIES_OFFSET = 28;
  static final int PROLOGUE_LENGTH = 32;

  // PerfDataEntry
  static final int ENTRY_LENGTH_OFFSET = 0;
  static final int NAME_OFFSET_OFFSET = 4;
  static final int VECTOR_LENGTH_OFFSET = 8;
  static final int DATA_TYPE_OFFSET = 12;
  static final int DATA_UNITS_OFFSET = 14;
  static final int DATA_OFFSET_OFFSET = 16;
  static final int ENTRY_HEADER_LENGTH = 20;

  static final byte TYPE_LONG = 'J';
  static final byte TYPE_BYTE = 'B';
  static final byte UNITS_STRING = 5;

  /**
   * Parses {@code hsperfdata} buffer.
   *
   * @param buffer buffer with mapped {@code hsperfdata} file
   * @return parsed values, or null if the JVM has not finished initializing the file yet
   * @throws IllegalArgumentException if the buffer is not a supported {@code hsperfdata} file
   */
  public static HsPerfData parse(DirectBuffer buffer) {
    final var capacity = buffer.capacity();
    if (capacity < PROLOGUE_LENGTH) {
      return null; // file created, not sized yet
    }

    final var magic = buffer.getInt(MAGIC_OFFSET, ByteOrder.BIG_ENDIAN);
    if (magic == 0) {
      return null; // file sized, prologue not written yet
    }
    if (magic != MAGIC) {
      throw new IllegalArgumentException("Bad magic: 0x" + Integer.toHexString(magic));
    }

    final var majorVersion = buffer.getByte(MAJOR_VERSION_OFFSET);
    if (majorVersion != SUPPORTED_MAJOR_VERSION) {
      throw new IllegalArgumentException("Unsupported major version: " + majorVersion);
    }

    if (buffer.getByte(ACCESSIBLE_OFFSET) == 0) {
      return null;
    }

    final var order =
        buffer.getByte(BYTE_ORDER_OFFSET) == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    final var numEntries = buffer.getInt(NUM_ENTRIES_OFFSET, order);
    final var used = buffer.getInt(USED_OFFSET, order);
    final var limit = Math.min(used, capacity);

    final var longs = new HashMap<String, Long>();
    final var strings = new HashMap<String, String>();

    var offset = buffer.getInt(ENTRY_OFFSET_OFFSET, order);
    for (int i = 0; i < numEntries; i++) {
      offset += readEntry(buffer, order, offset, limit, longs, strings);
    }

    return new HsPerfData(Map.copyOf(longs), Map.copyOf(strings));
  }

  private static int readEntry(
      DirectBuffer buffer,
      ByteOrder order,
      int offset,
      int limit,
      Map<String, Long> longs,
      Map<String, String> strings) {
    if (offset < PROLOGUE_LENGTH || offset > limit - ENTRY_HEADER_LENGTH) {
      throw new IllegalArgumentException("Entry out of bounds, offset: " + offset);
    }

    final var entryLength = buffer.getInt(offset + ENTRY_LENGTH_OFFSET, order);
    final var nameOffset = buffer.getInt(offset + NAME_OFFSET_OFFSET, order);
    final var vectorLength = buffer.getInt(offset + VECTOR_LENGTH_OFFSET, order);
    final var dataOffset = buffer.getInt(offset + DATA_OFFSET_OFFSET, order);
    checkEntry(offset, limit, entryLength, nameOffset, dataOffset, vectorLength);

    final var dataType = buffer.getByte(offset + DATA_TYPE_OFFSET);
    final var dataUnits = buffer.getByte(offset + DATA_UNITS_OFFSET);
    final var name = readString(buffer, offset + nameOffset, entryLength - nameOffset);

    if (dataType == TYPE_LONG && vectorLength == 0) {
      if (dataOffset > entryLength - Long.BYTES) {
        throw new IllegalArgumentException("Malformed entry at offset: " + offset);
      }
      longs.put(name, buffer.getLong(offset + dataOffset, order));
    } else if (dataType == TYPE_BYTE && dataUnits == UNITS_STRING) {
      strings.put(name, readString(buffer, offset + dataOffset, vectorLength));
    }

    return entryLength;
  }

  private static void checkEntry(
      int offset, int limit, int entryLength, int nameOffset, int dataOffset, int vectorLength) {
    final var lengthValid = entryLength >= ENTRY_HEADER_LENGTH && entryLength <= limit - offset;
    final var nameValid = nameOffset >= ENTRY_HEADER_LENGTH && nameOffset < entryLength;
    final var dataValid =
        dataOffset >= ENTRY_HEADER_LENGTH
            && dataOffset <= entryLength
            && vectorLength >= 0
            && vectorLength <= entryLength - dataOffset;
    if (!lengthValid || !nameValid || !dataValid) {
      throw new IllegalArgumentException("Malformed entry at offset: " + offset);
    }
  }

  private static String readString(DirectBuffer buffer, int index, int maxLength) {
    var length = 0;
    while (length < maxLength && buffer.getByte(index + length) != 0) {
      length++;
    }
    final var bytes = new byte[length];
    buffer.getBytes(index, bytes);
    return new String(bytes, StandardCharsets.US_ASCII);
  }
}
