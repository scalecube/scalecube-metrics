package io.scalecube.metrics;

import io.scalecube.metrics.sbe.KeyDecoder;
import java.util.HashMap;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

/**
 * Decodes buffer containing serialized {@link Key} structure. Uses SBE (Simple Binary Encoding) to
 * deserialize tag-value pairs. Supposed to be used on metrics/counters consumption side.
 */
public class KeyCodec {

  private final KeyDecoder keyDecoder = new KeyDecoder();
  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();

  public KeyCodec() {}

  /**
   * Decodes key structure from the given buffer and offset.
   *
   * @param keyBuffer buffer containing the encoded key
   * @param keyOffset offset within the buffer
   * @return decoded {@link Key} object
   */
  public Key decodeKey(DirectBuffer keyBuffer, int keyOffset) {
    keyDecoder.wrap(keyBuffer, keyOffset, KeyDecoder.BLOCK_LENGTH, KeyDecoder.SCHEMA_VERSION);
    final var tags = new HashMap<String, Object>();

    keyDecoder
        .tags()
        .forEach(
            decoder -> {
              final var tag = decoder.tag();
              final var valueLength = decoder.valueLength();
              decoder.getValue(valueBuffer, 0, valueLength);

              Object value = null;
              switch (decoder.valueType()) {
                case BYTE:
                  value = valueBuffer.getByte(0);
                  break;
                case SHORT:
                  value = valueBuffer.getShort(0);
                  break;
                case INT:
                  value = valueBuffer.getInt(0);
                  break;
                case LONG:
                  value = valueBuffer.getLong(0);
                  break;
                case DOUBLE:
                  value = valueBuffer.getDouble(0);
                  break;
                case STRING:
                  value = valueBuffer.getStringWithoutLengthAscii(0, valueLength);
                  break;
                default:
                  break;
              }

              tags.put(tag, value);
            });

    return new Key(tags);
  }
}
