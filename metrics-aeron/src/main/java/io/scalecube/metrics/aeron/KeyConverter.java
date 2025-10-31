package io.scalecube.metrics.aeron;

import org.agrona.DirectBuffer;

public interface KeyConverter {

  DirectBuffer convert(DirectBuffer keyBuffer, String label);
}
