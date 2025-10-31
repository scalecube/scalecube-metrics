package io.scalecube.metrics;

import io.scalecube.metrics.sbe.HistogramEncoder;
import io.scalecube.metrics.sbe.MessageHeaderEncoder;
import io.scalecube.metrics.sbe.TpsEncoder;
import java.nio.ByteBuffer;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

class MetricsEncoder {

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final HistogramEncoder histogramEncoder = new HistogramEncoder();
  private final TpsEncoder tpsEncoder = new TpsEncoder();

  private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024 * 1024);
  private final ByteBuffer dataBuffer = ByteBuffer.allocate(1024 * 1024);
  private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();

  MutableDirectBuffer buffer() {
    return buffer;
  }

  int encodeHistogram(
      long timestamp,
      DirectBuffer keyBuffer,
      Histogram accumulated,
      Histogram distinct,
      long highestTrackableValue,
      double conversionFactor) {
    histogramEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    histogramEncoder.timestamp(timestamp);
    histogramEncoder.highestTrackableValue(highestTrackableValue);
    histogramEncoder.conversionFactor(conversionFactor);
    histogramEncoder.putKey(keyBuffer, 0, keyBuffer.capacity());

    accumulated.encodeIntoByteBuffer(dataBuffer.clear());
    unsafeBuffer.wrap(dataBuffer.flip(), 0, dataBuffer.limit());
    histogramEncoder.putAccumulated(unsafeBuffer, 0, unsafeBuffer.capacity());

    distinct.encodeIntoByteBuffer(dataBuffer.clear());
    unsafeBuffer.wrap(dataBuffer.flip(), 0, dataBuffer.limit());
    histogramEncoder.putDistinct(unsafeBuffer, 0, unsafeBuffer.capacity());

    return headerEncoder.encodedLength() + histogramEncoder.encodedLength();
  }

  int encodeTps(long timestamp, DirectBuffer keyBuffer, long value) {
    tpsEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
    tpsEncoder.timestamp(timestamp);
    tpsEncoder.value(value);
    tpsEncoder.putKey(keyBuffer, 0, keyBuffer.capacity());

    return headerEncoder.encodedLength() + tpsEncoder.encodedLength();
  }
}
