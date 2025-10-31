package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.UUID;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PropertiesRegistryTest {

  private CountersRegistry countersRegistry;
  private CountersManager countersManager;
  private PropertiesRegistry propertiesRegistry;

  @BeforeEach
  void beforeEach() {
    countersRegistry = CountersRegistry.create();
    countersManager = countersRegistry.countersManager();
    propertiesRegistry = new PropertiesRegistry(countersManager);
  }

  @AfterEach
  void afterEach() {
    CloseHelper.quietCloseAll(countersRegistry);
  }

  @Test
  void stringProperty() {
    final var value = UUID.randomUUID();
    final var name = "string_property";
    propertiesRegistry.put(name, value);
    assertEquals(value.toString(), PropertiesRegistry.getProperty(countersManager, name));
  }

  @Test
  void byteProperty() {
    final String name = "byte_property";
    byte value = 42;
    propertiesRegistry.put(name, value);
    assertEquals(Byte.valueOf(value), PropertiesRegistry.getByteProperty(countersManager, name));
  }

  @Test
  void shortProperty() {
    final String name = "short_property";
    short value = 12345;
    propertiesRegistry.put(name, value);
    assertEquals(Short.valueOf(value), PropertiesRegistry.getShortProperty(countersManager, name));
  }

  @Test
  void intProperty() {
    final String name = "int_property";
    int value = 123456789;
    propertiesRegistry.put(name, value);
    assertEquals(Integer.valueOf(value), PropertiesRegistry.getIntProperty(countersManager, name));
  }

  @Test
  void longProperty() {
    final String name = "long_property";
    long value = 9876543210L;
    propertiesRegistry.put(name, value);
    assertEquals(Long.valueOf(value), PropertiesRegistry.getLongProperty(countersManager, name));
  }

  @Test
  void doubleProperty() {
    final String name = "double_property";
    double value = 3.14159;
    propertiesRegistry.put(name, value);
    assertEquals(
        Double.valueOf(value), PropertiesRegistry.getDoubleProperty(countersManager, name));
  }

  @Test
  void enumProperty() {
    final String name = "enum_property";
    SampleEnum value = SampleEnum.B;
    propertiesRegistry.put(name, value);
    SampleEnum result =
        PropertiesRegistry.getEnumProperty(countersManager, name, SampleEnum::valueOf);
    assertEquals(value, result);
  }

  @Test
  void unknownPropertyReturnsNull() {
    assertNull(PropertiesRegistry.getProperty(countersManager, "nonexistent"));
    assertNull(PropertiesRegistry.getByteProperty(countersManager, "nonexistent"));
    assertNull(PropertiesRegistry.getShortProperty(countersManager, "nonexistent"));
    assertNull(PropertiesRegistry.getIntProperty(countersManager, "nonexistent"));
    assertNull(PropertiesRegistry.getLongProperty(countersManager, "nonexistent"));
    assertNull(PropertiesRegistry.getDoubleProperty(countersManager, "nonexistent"));
    assertNull(
        PropertiesRegistry.getEnumProperty(countersManager, "nonexistent", SampleEnum::valueOf));
  }

  enum SampleEnum {
    A,
    B,
    C
  }
}
