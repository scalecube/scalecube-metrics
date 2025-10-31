package io.scalecube.metrics.prometheus;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embedded HTTP server that exposes Prometheus metrics on a configurable address. This server uses
 * the built-in {@link com.sun.net.httpserver.HttpServer} to bind a {@code /metrics} endpoint, which
 * can be scraped by Prometheus. The endpoint is backed by user-provided {@link
 * com.sun.net.httpserver.HttpHandler}, typically an instance of {@link PrometheusMetricsHandler}.
 */
public class PrometheusMetricsServer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusMetricsServer.class);

  private final HttpServer server;

  private PrometheusMetricsServer(HttpServer server) {
    this.server = server;
  }

  /**
   * Launch {@link PrometheusMetricsServer}.
   *
   * @param address address
   * @param metricsHandler metricsHandler
   * @return started {@link PrometheusMetricsServer} instance
   */
  public static PrometheusMetricsServer launch(
      InetSocketAddress address, HttpHandler metricsHandler) {
    HttpServer server = null;
    try {
      server = HttpServer.create(address, 0);
      server.createContext("/metrics", metricsHandler);
      server.setExecutor(
          Executors.newSingleThreadExecutor(
              r -> {
                final var thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("PrometheusMetricsServer");
                return thread;
              }));
      server.start();
      LOGGER.info("Started prometheus metrics server on {}", address);
      return new PrometheusMetricsServer(server);
    } catch (IOException e) {
      if (server != null) {
        server.stop(0);
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    server.stop(0);
    LOGGER.info("Stopped server");
  }
}
