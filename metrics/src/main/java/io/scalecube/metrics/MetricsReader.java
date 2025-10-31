package io.scalecube.metrics;

import static org.HdrHistogram.Histogram.decodeFromByteBuffer;

import io.scalecube.metrics.sbe.HistogramDecoder;
import io.scalecube.metrics.sbe.MessageHeaderDecoder;
import io.scalecube.metrics.sbe.TpsDecoder;
import java.nio.ByteBuffer;
import org.agrona.BufferUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

/**
 * {@link MessageHandler} implementation that consumes SBE metrics messages (histograms, tps values)
 * from the {@link BroadcastReceiver}, and dispatches them to the supplied {@link MetricsHandler}.
 */
public class MetricsReader implements MessageHandler, AutoCloseable {

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final HistogramDecoder histogramDecoder = new HistogramDecoder();
  private final TpsDecoder tpsDecoder = new TpsDecoder();

  private ByteBuffer scratchBuffer;
  private final CopyBroadcastReceiver broadcastReceiver;
  private MetricsHandler metricsHandler;
  private boolean isClosed = false;

  /**
   * Constructor.
   *
   * @param broadcastBuffer buffer from where to consume SBE metrics messages
   */
  public MetricsReader(AtomicBuffer broadcastBuffer) {
    scratchBuffer = ByteBuffer.allocateDirect(1024 * 1024);
    broadcastReceiver =
        new CopyBroadcastReceiver(
            new BroadcastReceiver(broadcastBuffer), new UnsafeBuffer(scratchBuffer));
  }

  /**
   * Receive one SBE metrics message from the broadcast buffer.
   *
   * @param handler callback handler to be called for each message received
   * @return number of messages that have been received
   */
  public int read(MetricsHandler handler) {
    metricsHandler = handler;
    return broadcastReceiver.receive(this);
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
    headerDecoder.wrap(buffer, index);
    switch (headerDecoder.templateId()) {
      case HistogramDecoder.TEMPLATE_ID:
        onHistogram(histogramDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case TpsDecoder.TEMPLATE_ID:
        onTps(tpsDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      default:
        break;
    }
  }

  private void onHistogram(HistogramDecoder decoder) {
    final var timestamp = decoder.timestamp();
    final var highestTrackableValue = decoder.highestTrackableValue();
    final var conversionFactor = decoder.conversionFactor();

    final var keyBuffer = decoder.buffer();
    final var keyOffset = decoder.limit() + HistogramDecoder.keyHeaderLength();
    final var keyLength = decoder.keyLength();
    decoder.skipKey();

    final var accumulatedBytes = new byte[decoder.accumulatedLength()];
    decoder.getAccumulated(accumulatedBytes, 0, accumulatedBytes.length);
    final var accumulated = decodeFromByteBuffer(ByteBuffer.wrap(accumulatedBytes), 1);

    final var distinctBytes = new byte[decoder.distinctLength()];
    decoder.getDistinct(distinctBytes, 0, distinctBytes.length);
    final var distinct = decodeFromByteBuffer(ByteBuffer.wrap(distinctBytes), 1);

    metricsHandler.onHistogram(
        timestamp,
        keyBuffer,
        keyOffset,
        keyLength,
        accumulated,
        distinct,
        highestTrackableValue,
        conversionFactor);
  }

  private void onTps(TpsDecoder decoder) {
    final var timestamp = decoder.timestamp();
    final var value = decoder.value();

    final var keyBuffer = decoder.buffer();
    final var keyOffset = decoder.limit() + TpsDecoder.keyHeaderLength();
    final var keyLength = decoder.keyLength();
    decoder.skipKey();

    metricsHandler.onTps(timestamp, keyBuffer, keyOffset, keyLength, value);
  }

  @Override
  public void close() {
    if (!isClosed) {
      isClosed = true;
      BufferUtil.free(scratchBuffer);
      scratchBuffer = null;
    }
  }
}
