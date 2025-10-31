package io.scalecube.metrics;

import static io.scalecube.metrics.CountersRegistry.Context.COUNTERS_DIR_NAME_PROP_NAME;
import static io.scalecube.metrics.CountersRegistry.Context.COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME;
import static io.scalecube.metrics.CountersRegistry.Context.DIR_DELETE_ON_SHUTDOWN_PROP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.metrics.CountersRegistry.LayoutDescriptor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CountersRegistryTest {

  private static final int TYPE_ID = 100;

  private static final AtomicInteger INT_COUNTER = new AtomicInteger(1);
  private static final AtomicLong LONG_COUNTER = new AtomicLong(1000L);
  private static final AtomicInteger PATH_COUNTER = new AtomicInteger(1);
  private static final Random RANDOM = new Random(12345L);

  private final KeyCodec keyCodec = new KeyCodec();
  private CountersRegistry countersRegistry;
  private CountersManager countersManager;

  @BeforeEach
  void beforeEach() {
    countersRegistry = CountersRegistry.create();
    countersManager = countersRegistry.countersManager();
  }

  @AfterEach
  void afterEach() {
    CloseHelper.quietClose(countersRegistry);
  }

  @Test
  void testPopulateFromProperties() {
    // given
    final var countersDirectoryName = nextPath();
    final var countersValuesBufferLength = nextLong();
    final var dirDeleteOnShutdown = nextBoolean();

    Properties props = new Properties();
    props.setProperty(COUNTERS_DIR_NAME_PROP_NAME, countersDirectoryName);
    props.setProperty(
        COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME, String.valueOf(countersValuesBufferLength));
    props.setProperty(DIR_DELETE_ON_SHUTDOWN_PROP_NAME, String.valueOf(dirDeleteOnShutdown));

    // when
    CountersRegistry.Context context = new CountersRegistry.Context(props);

    // then
    assertEquals(countersDirectoryName, context.countersDirectoryName());
    assertEquals(countersValuesBufferLength, context.countersValuesBufferLength());
    assertEquals(dirDeleteOnShutdown, context.dirDeleteOnShutdown());
  }

  @Test
  void testLabel() {
    final String label = "ABC";
    final var counter = countersManager.newCounter(label);
    assertNotNull(counter);
    assertEquals(label, countersManager.getCounterLabel(counter.id()));
  }

  @Test
  void testBufferLengthTooSmall() {
    var context = new CountersRegistry.Context().countersValuesBufferLength(1024);
    assertThrows(IllegalArgumentException.class, () -> CountersRegistry.create(context));
  }

  @Test
  void testBufferLengthNotPowerOfTwo() {
    var context = new CountersRegistry.Context().countersValuesBufferLength(12345);
    assertThrows(IllegalArgumentException.class, () -> CountersRegistry.create(context));
  }

  @Test
  void testCounterValueUpdates() {
    final var counter = countersManager.newCounter("update_counter", TYPE_ID);
    for (int i = 0; i < 100; i++) {
      counter.set(i);
      assertEquals(i, countersManager.getCounterValue(counter.id()));
    }
  }

  @Test
  void testCounterWithKeyFlyweight() {
    final var counter =
        countersManager.newCounter(
            "fly",
            TYPE_ID,
            buffer ->
                new KeyFlyweight()
                    .wrap(buffer, 0)
                    .tagsCount(3)
                    .intValue("tag1", 1)
                    .intValue("tag2", 2)
                    .intValue("tag3", 3));
    counter.set(101);

    final var descriptor = CounterDescriptor.getCounter(countersManager, counter.id());

    assertEquals(TYPE_ID, descriptor.typeId());
    assertEquals(101, descriptor.value());

    final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
    assertEquals(1, key.intValue("tag1"));
    assertEquals(2, key.intValue("tag2"));
    assertEquals(3, key.intValue("tag3"));
  }

  @Test
  void testCounterWithEmptyKeyFlyweight() {
    final var counter = countersManager.newCounter("fly", TYPE_ID);
    counter.set(101);

    final var descriptor = CounterDescriptor.getCounter(countersManager, counter.id());

    assertEquals(TYPE_ID, descriptor.typeId());
    assertEquals(101, descriptor.value());

    final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
    assertNotNull(key, "key");
    assertEquals(0, key.tags().size(), "tags.size");
  }

  @Test
  void testCounterWithoutKeyFlyweight() {
    final var sessionCounter = countersManager.newCounter("session_counter");
    final var counterDescriptor =
        CounterDescriptor.getCounter(countersManager, sessionCounter.id());
    final var key = keyCodec.decodeKey(counterDescriptor.keyBuffer(), 0);
    assertNotNull(key, "key");
    assertEquals(0, key.tags().size(), "tags.size");
  }

  @Test
  void testHeaderLayoutParsing() {
    final var context = countersRegistry.context();
    final var mappedBuffer = context.countersMetaDataBuffer().byteBuffer();
    final var headerBuffer = LayoutDescriptor.createHeaderBuffer(mappedBuffer);

    assertEquals(
        context.countersValuesBufferLength(),
        LayoutDescriptor.countersValuesBufferLength(headerBuffer));
  }

  @Test
  void testCounterAllocation() {
    final var countersValuesBufferLength = countersRegistry.context().countersValuesBufferLength();
    final var capacity = countersValuesBufferLength / CountersReader.COUNTER_LENGTH;

    for (int i = 0; i < capacity; i++) {
      final var counter = countersManager.newCounter("counter_" + i, TYPE_ID);
      counter.set(i);
    }

    final var capCounter = new MutableInteger();
    countersManager.forEach(
        (counterId, typeId, keyBuffer, label) -> {
          assertTrue(counterId >= 0, "counterId");
          assertTrue(typeId >= 0, "typeId");
          final var counterValue = countersManager.getCounterValue(counterId);
          assertEquals(capCounter.value, counterValue, "counterValue");
          assertEquals("counter_" + capCounter.value, label, "label");
          capCounter.increment();
        });

    assertEquals(capacity, capCounter.value);
  }

  @Test
  void testCapacityLimit() {
    final var countersValuesBufferLength = countersRegistry.context().countersValuesBufferLength();
    final var capacity = countersValuesBufferLength / CountersReader.COUNTER_LENGTH;

    for (int i = 0; i < capacity; i++) {
      countersManager.newCounter("counter_" + i);
    }

    // Allocate one more
    assertThrows(IllegalStateException.class, () -> countersManager.newCounter("foo"));
  }

  @Test
  void testCounterMetadata() {
    final String keyString = "This is key";
    final var keyBuffer = new UnsafeBuffer(keyString.getBytes(StandardCharsets.US_ASCII));

    final String labelString = "This is label";
    final var labelBuffer = new UnsafeBuffer(labelString.getBytes(StandardCharsets.US_ASCII));

    final var typeId = 100;
    final var value = 42;
    final var counter =
        countersManager.newCounter(
            typeId, keyBuffer, 0, keyBuffer.capacity(), labelBuffer, 0, labelBuffer.capacity());
    counter.set(value);

    final var counterDescriptor = CounterDescriptor.getCounter(countersManager, counter.id());

    assertEquals(
        keyString,
        counterDescriptor.keyBuffer().getStringWithoutLengthAscii(0, keyBuffer.capacity()),
        "keyString");
    assertEquals(labelString, counterDescriptor.label(), "labelString");
    assertEquals(counter.id(), counterDescriptor.counterId(), "counterId");
    assertEquals(typeId, counterDescriptor.typeId(), "typeId");
    assertEquals(value, counterDescriptor.value(), "value");
  }

  @Test
  void testCounterAllocatorWithoutKey() {
    final var counterAllocator = new CounterAllocator(countersRegistry.countersManager());
    final var fooCounter = counterAllocator.newCounter(TYPE_ID, "foo_counter", null);
    fooCounter.set(100500);
    final var counterDescriptor = CounterDescriptor.getCounter(countersManager, fooCounter.id());
    assertNotNull(counterDescriptor);
    assertEquals("foo_counter", counterDescriptor.label(), "labelString");
    assertEquals(TYPE_ID, counterDescriptor.typeId(), "typeId");
    assertEquals(100500, counterDescriptor.value(), "value");

    final var key = keyCodec.decodeKey(counterDescriptor.keyBuffer(), 0);
    assertEquals(0, key.tags().size(), "tags.size");
  }

  @Test
  void testCounterAllocatorWithEmptyKey() {
    final var counterAllocator = new CounterAllocator(countersRegistry.countersManager());
    final var fooCounter = counterAllocator.newCounter(TYPE_ID, "foo_counter", keyFlyweight -> {});
    fooCounter.set(100500);
    final var counterDescriptor = CounterDescriptor.getCounter(countersManager, fooCounter.id());
    assertNotNull(counterDescriptor);
    assertEquals("foo_counter", counterDescriptor.label(), "labelString");
    assertEquals(TYPE_ID, counterDescriptor.typeId(), "typeId");
    assertEquals(100500, counterDescriptor.value(), "value");

    final var key = keyCodec.decodeKey(counterDescriptor.keyBuffer(), 0);
    assertEquals(0, key.tags().size(), "tags.size");
  }

  @Test
  void testCounterAllocatorWithKey() {
    final var counterAllocator = new CounterAllocator(countersRegistry.countersManager());
    final var fooCounter =
        counterAllocator.newCounter(
            TYPE_ID,
            "fly",
            keyFlyweight ->
                keyFlyweight
                    .tagsCount(3)
                    .intValue("tag1", 1)
                    .intValue("tag2", 2)
                    .intValue("tag3", 3));
    fooCounter.set(101);

    final var descriptor = CounterDescriptor.getCounter(countersManager, fooCounter.id());

    assertEquals(TYPE_ID, descriptor.typeId());
    assertEquals(101, descriptor.value());

    final var key = keyCodec.decodeKey(descriptor.keyBuffer(), 0);
    assertEquals(1, key.intValue("tag1"));
    assertEquals(2, key.intValue("tag2"));
    assertEquals(3, key.intValue("tag3"));
  }

  private static int nextInt() {
    return INT_COUNTER.getAndIncrement();
  }

  private static long nextLong() {
    return LONG_COUNTER.getAndAdd(100L);
  }

  private static String nextPath() {
    return Paths.get(SystemUtil.tmpDirName() + "test-path-" + PATH_COUNTER.getAndIncrement())
        .toString();
  }

  private static boolean nextBoolean() {
    return RANDOM.nextBoolean();
  }
}
