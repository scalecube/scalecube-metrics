package io.scalecube.metrics.prometheus;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.BufferedOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP handler that serializes registered {@link PrometheusWriter} instances into the Prometheus
 * text exposition format. The handler responds to {@code GET /metrics} requests with the current
 * snapshot of metrics, compressed with GZIP, and encoded in UTF-8. It delegates the actual metric
 * serialization to the provided {@link PrometheusWriter} instances. On error, the handler responds
 * with HTTP 500.
 */
public class PrometheusMetricsHandler implements HttpHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusMetricsHandler.class);

  private final List<PrometheusWriter> prometheusWriters;

  /**
   * Constructor.
   *
   * @param prometheusWriters prometheusWriters
   */
  public PrometheusMetricsHandler(PrometheusWriter... prometheusWriters) {
    this(Arrays.asList(prometheusWriters));
  }

  /**
   * Constructor.
   *
   * @param prometheusWriters prometheusWriters
   */
  public PrometheusMetricsHandler(List<PrometheusWriter> prometheusWriters) {
    this.prometheusWriters = prometheusWriters;
  }

  @Override
  public void handle(HttpExchange exchange) {
    final var responseHeaders = exchange.getResponseHeaders();
    responseHeaders.set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
    responseHeaders.set("Content-Encoding", "gzip");

    try (var writer =
        new OutputStreamWriter(
            new GZIPOutputStream(new BufferedOutputStream(exchange.getResponseBody())),
            StandardCharsets.UTF_8)) {
      exchange.sendResponseHeaders(200, 0);
      for (var prometheusWriter : prometheusWriters) {
        prometheusWriter.write(writer);
      }
      writer.flush();
    } catch (Exception e) {
      LOGGER.warn("Exception occurred", e);
      try {
        exchange.sendResponseHeaders(500, -1);
      } catch (Exception ex) {
        // ignore
      }
    }
  }
}
