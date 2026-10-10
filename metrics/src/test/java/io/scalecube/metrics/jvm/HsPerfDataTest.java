package io.scalecube.metrics.jvm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HsPerfDataTest {

  @Test
  void parsesLongAndStringEntries() {
    final var buffer =
        new HsPerfDataBuilder()
            .longEntry("sun.rt.safepoints", 42)
            .stringEntry("sun.gc.collector.0.name", "G1 young collection pauses")
            .longVectorEntry("sun.gc.generation.0.agetable.bytes", 1, 2, 3)
            .longEntry("java.threads.live", -1)
            .build();

    final var hsPerfData = HsPerfData.parse(buffer);

    assertEquals(Map.of("sun.rt.safepoints", 42L, "java.threads.live", -1L), hsPerfData.longs());
    assertEquals(
        Map.of("sun.gc.collector.0.name", "G1 young collection pauses"), hsPerfData.strings());
  }

  @Test
  void parsesEmptyFile() {
    final var hsPerfData = HsPerfData.parse(new HsPerfDataBuilder().build());

    assertEquals(Map.of(), hsPerfData.longs());
    assertEquals(Map.of(), hsPerfData.strings());
  }

  @Test
  void notReadyWhenNotAccessible() {
    assertNull(HsPerfData.parse(new HsPerfDataBuilder().accessible(false).build()));
  }

  @Test
  void notReadyWhenPrologueNotWritten() {
    assertNull(HsPerfData.parse(new UnsafeBuffer(new byte[32 * 1024])));
  }

  @Test
  void notReadyWhenShorterThanPrologue() {
    assertNull(HsPerfData.parse(new UnsafeBuffer(new byte[0])));
  }

  @Test
  void rejectsBadMagic() {
    final var buffer = new HsPerfDataBuilder().magic(0xcafebabe).build();
    assertThrows(IllegalArgumentException.class, () -> HsPerfData.parse(buffer));
  }

  @Test
  void rejectsUnsupportedVersion() {
    final var buffer = new HsPerfDataBuilder().majorVersion(1).build();
    assertThrows(IllegalArgumentException.class, () -> HsPerfData.parse(buffer));
  }

  @Test
  void rejectsEntriesBeyondUsed() {
    final var buffer = new HsPerfDataBuilder().longEntry("a", 1).phantomEntries(1).build();
    assertThrows(IllegalArgumentException.class, () -> HsPerfData.parse(buffer));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 19, -8, 1 << 20})
  void rejectsMalformedEntryLength(int entryLength) {
    final var buffer = new HsPerfDataBuilder().entryWithLength(entryLength).build();
    assertThrows(IllegalArgumentException.class, () -> HsPerfData.parse(buffer));
  }
}
