package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KeyEqualityTest {

  @Test
  void testEqualityWithSameTags() {
    Map<String, Object> tags1 = new HashMap<>();
    tags1.put("id", 123);
    tags1.put("name", "alpha");

    Map<String, Object> tags2 = new HashMap<>();
    tags2.put("id", 123);
    tags2.put("name", "alpha");

    Key key1 = new Key(tags1);
    Key key2 = new Key(tags2);

    assertEquals(key1, key2, "Keys with same tag values should be equal");
    assertEquals(key1.hashCode(), key2.hashCode(), "Equal keys must have same hashCode");
  }

  @Test
  void testInequalityWithDifferentTags() {
    Map<String, Object> tags1 = Map.of("id", 123, "name", "alpha");
    Map<String, Object> tags2 = Map.of("id", 123, "name", "beta");

    Key key1 = new Key(tags1);
    Key key2 = new Key(tags2);

    assertNotEquals(key1, key2, "Keys with different tags should not be equal");
  }
}
