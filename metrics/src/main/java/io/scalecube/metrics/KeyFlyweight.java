package io.scalecube.metrics;

import static io.scalecube.metrics.sbe.ValueType.BYTE;
import static io.scalecube.metrics.sbe.ValueType.DOUBLE;
import static io.scalecube.metrics.sbe.ValueType.INT;
import static io.scalecube.metrics.sbe.ValueType.LONG;
import static io.scalecube.metrics.sbe.ValueType.SHORT;
import static io.scalecube.metrics.sbe.ValueType.STRING;
import static org.agrona.BitUtil.SIZE_OF_BYTE;
import static org.agrona.BitUtil.SIZE_OF_DOUBLE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.SIZE_OF_SHORT;

import io.scalecube.metrics.sbe.KeyEncoder;
import io.scalecube.metrics.sbe.KeyEncoder.TagsEncoder;
import java.util.function.ToIntFunction;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Flyweight-style encoder for serializing tag-value pairs into a buffer using SBE (Simple Binary
 * Encoding).
 */
public class KeyFlyweight {

  private final KeyEncoder keyEncoder = new KeyEncoder();
  private TagsEncoder tagsEncoder;
  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();
  private MutableDirectBuffer buffer;
  private int offset;
  private int length;

  public KeyFlyweight() {}

  /**
   * Wraps this flyweight around buffer at given offset.
   *
   * @param buffer buffer to write into
   * @param offset starting offset
   * @return this {@link KeyFlyweight} instance
   */
  public KeyFlyweight wrap(MutableDirectBuffer buffer, int offset) {
    this.buffer = buffer;
    this.offset = offset;
    this.tagsEncoder = null;
    keyEncoder.wrap(buffer, offset);
    return self();
  }

  /**
   * Specifies the number of tag entries to encode.
   *
   * @param count number of tags
   * @return this {@link KeyFlyweight} instance
   */
  public KeyFlyweight tagsCount(int count) {
    tagsEncoder = keyEncoder.tagsCount(count);
    return self();
  }

  /**
   * Sets {@code byte} value for the specified tag.
   *
   * @param tag tag name
   * @param value {@code byte} value
   * @return this {@link KeyFlyweight} instance
   */
  public KeyFlyweight byteValue(String tag, byte value) {
    ensureTagsEncoder();
    tagsEncoder.next();
    tagsEncoder.tag(tag).valueType(BYTE);
    valueBuffer.putByte(0, value);
    tagsEncoder.putValue(valueBuffer, 0, SIZE_OF_BYTE);
    return self();
  }

  /**
   * Sets {@code short} value for the specified tag.
   *
   * @param tag tag name
   * @param value {@code short} value
   * @return this {@link KeyFlyweight} instance
   */
  public KeyFlyweight shortValue(String tag, short value) {
    ensureTagsEncoder();
    tagsEncoder.next();
    tagsEncoder.tag(tag).valueType(SHORT);
    valueBuffer.putShort(0, value);
    tagsEncoder.putValue(valueBuffer, 0, SIZE_OF_SHORT);
    return self();
  }

  /**
   * Sets {@code int} value for the specified tag.
   *
   * @param tag tag name
   * @param value {@code int} value
   * @return this {@link KeyFlyweight} instance
   */
  public KeyFlyweight intValue(String tag, int value) {
    ensureTagsEncoder();
    tagsEncoder.next();
    tagsEncoder.tag(tag).valueType(INT);
    valueBuffer.putInt(0, value);
    tagsEncoder.putValue(valueBuffer, 0, SIZE_OF_INT);
    return self();
  }

  /**
   * Sets {@code long} value for the specified tag.
   *
   * @param tag tag name
   * @param value {@code long} value
   * @return this {@link KeyFlyweight} instance
   */
  public KeyFlyweight longValue(String tag, long value) {
    ensureTagsEncoder();
    tagsEncoder.next();
    tagsEncoder.tag(tag).valueType(LONG);
    valueBuffer.putLong(0, value);
    tagsEncoder.putValue(valueBuffer, 0, SIZE_OF_LONG);
    return self();
  }

  /**
   * Sets {@code double} value for the specified tag.
   *
   * @param tag tag name
   * @param value {@code double} value
   * @return this {@link KeyFlyweight} instance
   */
  public KeyFlyweight doubleValue(String tag, double value) {
    ensureTagsEncoder();
    tagsEncoder.next();
    tagsEncoder.tag(tag).valueType(DOUBLE);
    valueBuffer.putDouble(0, Math.round(value * 1000.0) / 1000.0);
    tagsEncoder.putValue(valueBuffer, 0, SIZE_OF_DOUBLE);
    return self();
  }

  /**
   * Sets {@code String} value for the specified tag.
   *
   * @param tag tag name
   * @param value {@code String} value
   * @return this {@link KeyFlyweight} instance
   */
  public KeyFlyweight stringValue(String tag, String value) {
    ensureTagsEncoder();
    tagsEncoder.next();
    tagsEncoder.tag(tag).valueType(STRING);
    final var length = valueBuffer.putStringWithoutLengthAscii(0, value);
    tagsEncoder.putValue(valueBuffer, 0, length);
    return self();
  }

  /**
   * Sets {@code CharSequence} value for the specified tag.
   *
   * @param tag tag name
   * @param value {@code CharSequence} value
   * @return this {@link KeyFlyweight} instance
   */
  public KeyFlyweight stringValue(String tag, CharSequence value) {
    ensureTagsEncoder();
    tagsEncoder.next();
    tagsEncoder.tag(tag).valueType(STRING);
    final var length = valueBuffer.putStringWithoutLengthAscii(0, value);
    tagsEncoder.putValue(valueBuffer, 0, length);
    return self();
  }

  /**
   * Sets an enum tag value encoded as byte.
   *
   * @param tag name tag name
   * @param enumValue enum instance
   * @param encoder function converting enum to byte
   * @param <T> enum type
   * @return this {@link KeyFlyweight} instance
   */
  public <T extends Enum<T>> KeyFlyweight enumValue(
      String tag, T enumValue, ToIntFunction<T> encoder) {
    ensureTagsEncoder();
    tagsEncoder.next();
    tagsEncoder.tag(tag).valueType(BYTE);
    valueBuffer.putByte(0, (byte) encoder.applyAsInt(enumValue));
    tagsEncoder.putValue(valueBuffer, 0, SIZE_OF_BYTE);
    return self();
  }

  /**
   * Returns wrapped buffer.
   *
   * @return wrapped buffer
   */
  public MutableDirectBuffer buffer() {
    return buffer;
  }

  /**
   * Returns wrapped buffer starting offset.
   *
   * @return starting offset
   */
  public int offset() {
    return offset;
  }

  /**
   * Returns encoded length.
   *
   * @return encoded length
   */
  public int length() {
    return length;
  }

  private KeyFlyweight self() {
    length = keyEncoder.encodedLength();
    return this;
  }

  private void ensureTagsEncoder() {
    if (tagsEncoder == null) {
      throw new IllegalStateException("tagsCount() must be called before setting values");
    }
  }
}
