package io.scalecube.metrics;

import java.util.List;

/**
 * Callback interface for handling counter descriptors. Being used as part of counter processing
 * functionality.
 *
 * @see CountersReaderAgent
 */
public interface CountersHandler {

  /**
   * Callback for handling counter descriptors.
   *
   * @param timestamp timestamp
   * @param counterDescriptors counterDescriptors
   */
  default void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
    // no-op
  }
}
