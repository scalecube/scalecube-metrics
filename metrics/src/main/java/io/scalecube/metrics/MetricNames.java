package io.scalecube.metrics;

/**
 * Utility class for sanitizing metric names. Converts arbitrary strings (including CamelCase
 * identifiers) into snake_case-compliant names suitable for use in systems like Prometheus.
 */
public class MetricNames {

  private MetricNames() {
    // Do not instantiate
  }

  /**
   * Sanitizes a string into a valid metric name.
   *
   * <ul>
   *   <li>Converts CamelCase to snake_case (preserving acronyms)
   *   <li>Replaces invalid characters with underscores
   *   <li>Converts the entire name to lowercase
   *   <li>Collapses multiple consecutive underscores
   *   <li>Ensures the name starts with a letter or underscore
   * </ul>
   *
   * @param value input string to sanitize
   * @return sanitized metric name, defaults to "_" if input is null or empty
   */
  public static String sanitizeName(String value) {
    if (value == null || value.isEmpty()) {
      return "_";
    }

    // Convert CamelCase to snake_case with acronym preservation
    String snake =
        value.replaceAll("([a-z0-9])([A-Z])", "$1_$2").replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2");

    // Replace disallowed characters and lowercase
    var sanitizedValue = snake.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();

    // Collapse consecutive underscores
    sanitizedValue = sanitizedValue.replaceAll("_+", "_");

    // Ensure first character is valid
    if (!Character.isLetter(sanitizedValue.charAt(0)) && sanitizedValue.charAt(0) != '_') {
      sanitizedValue = "_" + sanitizedValue;
    }

    return sanitizedValue;
  }
}
