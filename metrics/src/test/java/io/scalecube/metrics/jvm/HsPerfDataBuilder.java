package io.scalecube.metrics.jvm;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;

// Writes hsperfdata layout the way HotSpot perfMemory/perfData do; the layout is taken from
// HotSpot perfMemory.hpp, not from HsPerfData, so the parser is checked against the format itself
class HsPerfDataBuilder {

  private static final int CAPACITY = 32 * 1024;
  private static final int MAGIC = 0xcafec0c0;

  // PerfDataPrologue
  private static final int MAGIC_OFFSET = 0;
  private static final int BYTE_ORDER_OFFSET = 4;
  private static final int MAJOR_VERSION_OFFSET = 5;
  private static final int ACCESSIBLE_OFFSET = 7;
  private static final int USED_OFFSET = 8;
  private static final int ENTRY_OFFSET_OFFSET = 24;
  private static final int NUM_ENTRIES_OFFSET = 28;
  private static final int PROLOGUE_LENGTH = 32;

  // PerfDataEntry
  private static final int ENTRY_LENGTH_OFFSET = 0;
  private static final int NAME_OFFSET_OFFSET = 4;
  private static final int VECTOR_LENGTH_OFFSET = 8;
  private static final int DATA_TYPE_OFFSET = 12;
  private static final int DATA_UNITS_OFFSET = 14;
  private static final int DATA_OFFSET_OFFSET = 16;
  private static final int ENTRY_HEADER_LENGTH = 20;

  private static final byte TYPE_LONG = 'J';
  private static final byte TYPE_BYTE = 'B';
  private static final byte UNITS_NONE = 1;
  private static final byte UNITS_STRING = 5;

  private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[CAPACITY]);
  private int offset = PROLOGUE_LENGTH;
  private int numEntries;
  private int magic = MAGIC;
  private int majorVersion = 2;
  private boolean accessible = true;

  HsPerfDataBuilder magic(int magic) {
    this.magic = magic;
    return this;
  }

  HsPerfDataBuilder majorVersion(int majorVersion) {
    this.majorVersion = majorVersion;
    return this;
  }

  HsPerfDataBuilder accessible(boolean accessible) {
    this.accessible = accessible;
    return this;
  }

  HsPerfDataBuilder longEntry(String name, long value) {
    final var dataOffset = align(ENTRY_HEADER_LENGTH + name.length() + 1, Long.BYTES);
    writeEntryHeader(name, dataOffset + Long.BYTES, 0, TYPE_LONG, UNITS_NONE, dataOffset);
    buffer.putLong(offset + dataOffset, value, ByteOrder.LITTLE_ENDIAN);
    return next(dataOffset + Long.BYTES);
  }

  HsPerfDataBuilder longVectorEntry(String name, long... values) {
    final var dataOffset = align(ENTRY_HEADER_LENGTH + name.length() + 1, Long.BYTES);
    final var entryLength = dataOffset + values.length * Long.BYTES;
    writeEntryHeader(name, entryLength, values.length, TYPE_LONG, UNITS_NONE, dataOffset);
    for (int i = 0; i < values.length; i++) {
      buffer.putLong(offset + dataOffset + i * Long.BYTES, values[i], ByteOrder.LITTLE_ENDIAN);
    }
    return next(entryLength);
  }

  HsPerfDataBuilder stringEntry(String name, String value) {
    final var dataOffset = ENTRY_HEADER_LENGTH + name.length() + 1;
    final var vectorLength = value.length() + 1;
    final var entryLength = align(dataOffset + vectorLength, Long.BYTES);
    writeEntryHeader(name, entryLength, vectorLength, TYPE_BYTE, UNITS_STRING, dataOffset);
    buffer.putBytes(offset + dataOffset, value.getBytes(StandardCharsets.US_ASCII));
    return next(entryLength);
  }

  // Entry whose length field is broken; walking it as is would loop forever
  HsPerfDataBuilder entryWithLength(int entryLength) {
    writeEntryHeader("broken", entryLength, 0, TYPE_LONG, UNITS_NONE, 32);
    numEntries++;
    offset += 40;
    return this;
  }

  // num_entries claims entries that were never written
  HsPerfDataBuilder phantomEntries(int count) {
    numEntries += count;
    return this;
  }

  UnsafeBuffer build() {
    buffer.putInt(MAGIC_OFFSET, magic, ByteOrder.BIG_ENDIAN);
    buffer.putByte(BYTE_ORDER_OFFSET, (byte) 1);
    buffer.putByte(MAJOR_VERSION_OFFSET, (byte) majorVersion);
    buffer.putByte(MAJOR_VERSION_OFFSET + 1, (byte) 0);
    buffer.putByte(ACCESSIBLE_OFFSET, (byte) (accessible ? 1 : 0));
    buffer.putInt(USED_OFFSET, offset, ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(ENTRY_OFFSET_OFFSET, PROLOGUE_LENGTH, ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(NUM_ENTRIES_OFFSET, numEntries, ByteOrder.LITTLE_ENDIAN);
    return buffer;
  }

  byte[] buildBytes() {
    return build().byteArray();
  }

  private void writeEntryHeader(
      String name, int entryLength, int vectorLength, byte type, byte units, int dataOffset) {
    buffer.putInt(offset + ENTRY_LENGTH_OFFSET, entryLength, ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(offset + NAME_OFFSET_OFFSET, ENTRY_HEADER_LENGTH, ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(offset + VECTOR_LENGTH_OFFSET, vectorLength, ByteOrder.LITTLE_ENDIAN);
    buffer.putByte(offset + DATA_TYPE_OFFSET, type);
    buffer.putByte(offset + DATA_UNITS_OFFSET, units);
    buffer.putInt(offset + DATA_OFFSET_OFFSET, dataOffset, ByteOrder.LITTLE_ENDIAN);
    buffer.putBytes(offset + ENTRY_HEADER_LENGTH, name.getBytes(StandardCharsets.US_ASCII));
  }

  private HsPerfDataBuilder next(int entryLength) {
    offset += entryLength;
    numEntries++;
    return this;
  }

  private static int align(int value, int alignment) {
    return (value + alignment - 1) & -alignment;
  }
}
