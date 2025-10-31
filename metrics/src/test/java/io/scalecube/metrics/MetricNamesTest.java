package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MetricNamesTest {

  @Test
  void testSanitizeName_validName() {
    Assertions.assertEquals("valid_name", MetricNames.sanitizeName("valid_name"));
  }

  @Test
  void testSanitizeName_null() {
    assertEquals("_", MetricNames.sanitizeName(null));
  }

  @Test
  void testSanitizeName_empty() {
    assertEquals("_", MetricNames.sanitizeName(""));
  }

  @Test
  void testSanitizeName_illegalCharacters() {
    assertEquals("a_b_c", MetricNames.sanitizeName("a!b@c"));
  }

  @Test
  void testSanitizeName_upperCase() {
    assertEquals("abc", MetricNames.sanitizeName("ABC"));
  }

  @Test
  void testSanitizeName_startsWithDash() {
    assertEquals("_abc", MetricNames.sanitizeName("-abc"));
  }

  @Test
  void testSanitizeName_startsWithDigit() {
    assertEquals("_123abc", MetricNames.sanitizeName("123abc"));
  }

  @Test
  void testSanitizeName_allInvalid() {
    assertEquals("_", MetricNames.sanitizeName("!!!"));
  }

  @Test
  void testSanitizeName_camelCaseToSnakeCase() {
    assertEquals("cool_market_agent", MetricNames.sanitizeName("CoolMarketAgent"));
  }

  @Test
  void testSanitizeName_acronymPreserved() {
    assertEquals("http_request", MetricNames.sanitizeName("HTTPRequest"));
  }

  @Test
  void testSanitizeName_acronymWithDigit() {
    assertEquals("http2_connection_pool", MetricNames.sanitizeName("HTTP2ConnectionPool"));
  }

  @Test
  void testSanitizeName_mixedAcronymCamelCase() {
    assertEquals("json_to_http_bridge", MetricNames.sanitizeName("JsonToHTTPBridge"));
  }

  @Test
  void testSanitizeName_digitInMiddle() {
    assertEquals("metric2025_value", MetricNames.sanitizeName("Metric2025Value"));
  }

  @Test
  void testSanitizeName_multipleUpperCaseBlocks() {
    assertEquals("ssl_tls_config", MetricNames.sanitizeName("SSL_TLSConfig"));
  }
}
