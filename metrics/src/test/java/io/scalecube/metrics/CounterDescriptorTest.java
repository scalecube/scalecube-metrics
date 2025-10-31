package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.*;

import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CounterDescriptorTest {

  private static final int TYPE_ID = 100;
  private static final int ANOTHER_TYPE_ID = 200;
  private static final int VALUE = 100500;

  private final CountersRegistry countersRegistry = CountersRegistry.create();
  private final CountersManager countersManager = countersRegistry.countersManager();
  private final CounterAllocator counterAllocator = new CounterAllocator(countersManager);

  @BeforeEach
  void beforeEach() {
    counterAllocator.newCounter(TYPE_ID, "foo", null);
    counterAllocator.newCounter(TYPE_ID, "bar", null);
    counterAllocator.newCounter(TYPE_ID, "baz", null);
    counterAllocator.newCounter(TYPE_ID, "by_name", null);
    counterAllocator.newCounter(TYPE_ID, "by_value", null).set(VALUE);
    counterAllocator.newCounter(ANOTHER_TYPE_ID, "another_type_id", null);
    counterAllocator.newCounter(
        TYPE_ID, "by_byte_tag", k -> k.tagsCount(1).byteValue("byte_tag", (byte) 1));
    counterAllocator.newCounter(
        TYPE_ID, "by_short_tag", k -> k.tagsCount(1).shortValue("short_tag", (short) 1));
    counterAllocator.newCounter(TYPE_ID, "by_int_tag", k -> k.tagsCount(1).intValue("int_tag", 1));
    counterAllocator.newCounter(
        TYPE_ID, "by_long_tag", k -> k.tagsCount(1).longValue("long_tag", 1L));
    counterAllocator.newCounter(
        TYPE_ID, "by_double_tag", k -> k.tagsCount(1).doubleValue("double_tag", 1.0));
    counterAllocator.newCounter(
        TYPE_ID, "by_string_tag", k -> k.tagsCount(1).stringValue("string_tag", "42"));
  }

  @AfterEach
  void afterEach() {
    CloseHelper.quietCloseAll(countersRegistry);
  }

  @Test
  void findFirstCounter() {
    final var firstCounter =
        CounterDescriptor.findFirstCounter(countersManager, descriptor -> true);
    assertNotNull(firstCounter);
    assertEquals(TYPE_ID, firstCounter.typeId());
    assertEquals("foo", firstCounter.label());
  }

  @Test
  void findLastCounter() {
    final var lastCounter = CounterDescriptor.findLastCounter(countersManager, descriptor -> true);
    assertNotNull(lastCounter);
    assertEquals(TYPE_ID, lastCounter.typeId());
    assertEquals("by_string_tag", lastCounter.label());
  }

  @Test
  void findAllCounters() {
    final var allCounters =
        CounterDescriptor.findAllCounters(
            countersManager, descriptor -> descriptor.label().startsWith("b"));
    assertEquals(10, allCounters.size());
  }

  @Test
  void byName() {
    final var counter =
        CounterDescriptor.findFirstCounter(countersManager, CounterDescriptor.byName("by_name"));
    assertNotNull(counter);
    assertEquals("by_name", counter.label());
  }

  @Test
  void byValue() {
    final var counter =
        CounterDescriptor.findFirstCounter(countersManager, CounterDescriptor.byValue(VALUE));
    assertNotNull(counter);
    assertEquals("by_value", counter.label());
    assertEquals(VALUE, counter.value());
  }

  @Test
  void byType() {
    final var counter =
        CounterDescriptor.findFirstCounter(
            countersManager, CounterDescriptor.byType(ANOTHER_TYPE_ID));
    assertNotNull(counter);
    assertEquals(ANOTHER_TYPE_ID, counter.typeId());
    assertEquals("another_type_id", counter.label());
  }

  @Test
  void byByteTag() {
    final var counter =
        CounterDescriptor.findFirstCounter(
            countersManager, CounterDescriptor.byTag("byte_tag", (byte) 1));
    assertNotNull(counter);
    assertEquals("by_byte_tag", counter.label());
  }

  @Test
  void byShortTag() {
    final var counter =
        CounterDescriptor.findFirstCounter(
            countersManager, CounterDescriptor.byTag("short_tag", (short) 1));
    assertNotNull(counter);
    assertEquals("by_short_tag", counter.label());
  }

  @Test
  void byIntTag() {
    final var counter =
        CounterDescriptor.findFirstCounter(countersManager, CounterDescriptor.byTag("int_tag", 1));
    assertNotNull(counter);
    assertEquals("by_int_tag", counter.label());
  }

  @Test
  void byLongTag() {
    final var counter =
        CounterDescriptor.findFirstCounter(
            countersManager, CounterDescriptor.byTag("long_tag", 1L));
    assertNotNull(counter);
    assertEquals("by_long_tag", counter.label());
  }

  @Test
  void byDoubleTag() {
    final var counter =
        CounterDescriptor.findFirstCounter(
            countersManager, CounterDescriptor.byTag("double_tag", 1.0));
    assertNotNull(counter);
    assertEquals("by_double_tag", counter.label());
  }

  @Test
  void byStringTag() {
    final var counter =
        CounterDescriptor.findFirstCounter(
            countersManager, CounterDescriptor.byTag("string_tag", "42"));
    assertNotNull(counter);
    assertEquals("by_string_tag", counter.label());
  }

  @Test
  void byEnumTag() {
    // Allocate a counter with a byte representing enum ordinal
    counterAllocator.newCounter(
        TYPE_ID,
        "by_enum_tag",
        k -> k.tagsCount(1).byteValue("enum_tag", (byte) SampleEnum.B.ordinal()));

    final var counter =
        CounterDescriptor.findFirstCounter(
            countersManager,
            CounterDescriptor.byTag("enum_tag", SampleEnum.B, b -> SampleEnum.values()[b]));
    assertNotNull(counter);
    assertEquals("by_enum_tag", counter.label());
  }

  enum SampleEnum {
    A,
    B,
    C
  }
}
