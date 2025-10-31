package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Map;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class KeyFlyweightTest {

  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final KeyFlyweight keyFlyweight = new KeyFlyweight();
  private final KeyCodec keyCodec = new KeyCodec();

  @Test
  void testEncodeTags() {
    keyFlyweight
        .wrap(keyBuffer, 0)
        .tagsCount(4)
        .intValue("tag1", 100)
        .longValue("tag2", 100L)
        .doubleValue("tag3", 100.0)
        .stringValue("tag4", "hello");

    final var key = keyCodec.decodeKey(keyBuffer, 0);
    final var tags = key.tags();
    assertEquals(4, tags.size(), "tags.size");
    assertEquals(100, tags.get("tag1"));
    assertEquals(100L, tags.get("tag2"));
    assertEquals(100.0, tags.get("tag3"));
    assertEquals("hello", tags.get("tag4"));
  }

  @Test
  void testEncodeAllSupportedTypes() {
    keyFlyweight
        .wrap(keyBuffer, 0)
        .tagsCount(6)
        .longValue("tag1", 100L)
        .doubleValue("tag2", 100.500)
        .stringValue("tag3", "hello")
        .longValue("tag4", 100L)
        .doubleValue("tag5", 100.500)
        .stringValue("tag6", "hello");

    final var key = keyCodec.decodeKey(keyBuffer, 0);
    final var tags = key.tags();
    assertEquals(6, tags.size(), "tags.size");
    assertEquals(100L, tags.get("tag1"));
    assertEquals(100.500, tags.get("tag2"));
    assertEquals("hello", tags.get("tag3"));
    assertEquals(100L, tags.get("tag4"));
    assertEquals(100.500, tags.get("tag5"));
    assertEquals("hello", tags.get("tag6"));
  }

  @Test
  void testEncodeOnlyByteTags() {
    keyFlyweight
        .wrap(keyBuffer, 0)
        .tagsCount(3)
        .byteValue("tag1", (byte) 1)
        .byteValue("tag2", (byte) 2)
        .byteValue("tag3", (byte) 3);

    final var key = keyCodec.decodeKey(keyBuffer, 0);
    final var tags = key.tags();
    assertEquals(3, tags.size(), "tags.size");
    assertEquals((byte) 1, key.byteValue("tag1"));
    assertEquals((byte) 2, key.byteValue("tag2"));
    assertEquals((byte) 3, key.byteValue("tag3"));
  }

  @Test
  void testEncodeOnlyShortTags() {
    keyFlyweight
        .wrap(keyBuffer, 0)
        .tagsCount(3)
        .shortValue("tag1", (short) 1)
        .shortValue("tag2", (short) 2)
        .shortValue("tag3", (short) 3);

    final var key = keyCodec.decodeKey(keyBuffer, 0);
    final var tags = key.tags();
    assertEquals(3, tags.size(), "tags.size");
    assertEquals((short) 1, key.shortValue("tag1"));
    assertEquals((short) 2, key.shortValue("tag2"));
    assertEquals((short) 3, key.shortValue("tag3"));
  }

  @Test
  void testEncodeOnlyIntTags() {
    keyFlyweight
        .wrap(keyBuffer, 0)
        .tagsCount(3)
        .intValue("tag1", 1)
        .intValue("tag2", 2)
        .intValue("tag3", 3);

    final var key = keyCodec.decodeKey(keyBuffer, 0);
    final var tags = key.tags();
    assertEquals(3, tags.size(), "tags.size");
    assertEquals(1, key.intValue("tag1"));
    assertEquals(2, key.intValue("tag2"));
    assertEquals(3, key.intValue("tag3"));
  }

  @Test
  void testEncodeOnlyLongTags() {
    keyFlyweight
        .wrap(keyBuffer, 0)
        .tagsCount(3)
        .longValue("tag1", 1L)
        .longValue("tag2", 2L)
        .longValue("tag3", 3L);

    final var key = keyCodec.decodeKey(keyBuffer, 0);
    final var tags = key.tags();
    assertEquals(3, tags.size(), "tags.size");
    assertEquals(1, key.longValue("tag1"));
    assertEquals(2, key.longValue("tag2"));
    assertEquals(3, key.longValue("tag3"));
  }

  @Test
  void testEncodeOnlyDoubleTags() {
    keyFlyweight
        .wrap(keyBuffer, 0)
        .tagsCount(3)
        .doubleValue("tag1", 1.0)
        .doubleValue("tag2", 2.0)
        .doubleValue("tag3", 3.0);

    final var key = keyCodec.decodeKey(keyBuffer, 0);
    final var tags = key.tags();
    assertEquals(3, tags.size(), "tags.size");
    assertEquals(1.0, key.doubleValue("tag1"));
    assertEquals(2.0, key.doubleValue("tag2"));
    assertEquals(3.0, key.doubleValue("tag3"));
  }

  @Test
  void testEncodeOnlyStringTags() {
    keyFlyweight
        .wrap(keyBuffer, 0)
        .tagsCount(3)
        .stringValue("tag1", "foo")
        .stringValue("tag2", "bar")
        .stringValue("tag3", "baz");

    final var key = keyCodec.decodeKey(keyBuffer, 0);
    final var tags = key.tags();
    assertEquals(3, tags.size(), "tags.size");
    assertEquals("foo", key.stringValue("tag1"));
    assertEquals("bar", key.stringValue("tag2"));
    assertEquals("baz", key.stringValue("tag3"));
  }

  @Test
  void testEquality() {
    final var n = 100;
    final var bufferList = new ArrayList<UnsafeBuffer>();

    for (int i = 0, offset = 0; i < n; i++, offset += keyFlyweight.length()) {
      keyFlyweight.wrap(keyBuffer, offset).tagsCount(1).stringValue("tag1", "10");
      bufferList.add(
          new UnsafeBuffer(keyFlyweight.buffer(), keyFlyweight.offset(), keyFlyweight.length()));
    }

    for (UnsafeBuffer b1 : bufferList) {
      for (UnsafeBuffer b2 : bufferList) {
        if (b1 != b2) {
          assertEquals(b1, b2);
        }
      }
    }
  }

  @Test
  void testOverwriteBuffer() {
    keyFlyweight.wrap(keyBuffer, 0).tagsCount(2).intValue("tag1", 42).stringValue("tag2", "foo");

    var key1 = keyCodec.decodeKey(keyBuffer, 0);

    keyFlyweight.wrap(keyBuffer, 0).tagsCount(1).stringValue("tag1", "bar");

    var key2 = keyCodec.decodeKey(keyBuffer, 0);

    assertEquals(2, key1.tags().size());
    assertEquals(1, key2.tags().size());
    assertEquals("bar", key2.tags().get("tag1"));
  }

  @Test
  void testLongStringValue() {
    String longString = "a".repeat(1024);
    keyFlyweight.wrap(keyBuffer, 0).tagsCount(1).stringValue("tag1", longString);
    var key = keyCodec.decodeKey(keyBuffer, 0);
    assertEquals(longString, key.stringValue("tag1"));
  }

  @Test
  void testEncodingWithOffsets() {
    var offset1 = 0;
    var offset2 = 512;

    keyFlyweight.wrap(keyBuffer, offset1).tagsCount(1).intValue("tag1", 123);
    keyFlyweight.wrap(keyBuffer, offset2).tagsCount(1).intValue("tag1", 456);

    assertEquals(123, keyCodec.decodeKey(keyBuffer, offset1).intValue("tag1"));
    assertEquals(456, keyCodec.decodeKey(keyBuffer, offset2).intValue("tag1"));
  }

  @Test
  void testEnumRoundTrip() {
    enum Color {
      RED,
      GREEN,
      BLUE
    }

    final var toCode = Map.of(Color.RED, (byte) 1, Color.GREEN, (byte) 2, Color.BLUE, (byte) 3);
    final var fromCode = Map.of((byte) 1, Color.RED, (byte) 2, Color.GREEN, (byte) 3, Color.BLUE);

    keyFlyweight.wrap(keyBuffer, 0).tagsCount(1).enumValue("tag1", Color.GREEN, toCode::get);
    var key = keyCodec.decodeKey(keyBuffer, 0);
    var decoded = key.enumValue("tag1", fromCode::get);
    assertEquals(Color.GREEN, decoded);
  }
}
