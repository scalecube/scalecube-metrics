package io.scalecube.metrics.mimir;

import io.scalecube.metrics.Delay;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.agrona.LangUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import prometheus.Remote.WriteRequest;

class MimirPublisherAgent implements Agent {

  private static final Logger LOGGER = LoggerFactory.getLogger(MimirPublisherAgent.class);

  public enum State {
    INIT,
    RUNNING,
    CLEANUP,
    CLOSED
  }

  private final String url;
  private final int writeLimit;
  private final ManyToOneConcurrentArrayQueue<WriteRequest> writeQueue;

  private final Delay retryInterval;
  private final Delay publishInterval;
  private ExecutorService executor;
  private HttpClient httpClient;
  private CompletableFuture<HttpResponse<String>> future;
  private State state = State.CLOSED;

  MimirPublisherAgent(
      String url,
      EpochClock epochClock,
      Duration retryInterval,
      Duration publishInterval,
      int writeLimit,
      ManyToOneConcurrentArrayQueue<WriteRequest> writeQueue) {
    this.url = url;
    this.writeLimit = writeLimit;
    this.writeQueue = writeQueue;
    this.retryInterval = new Delay(epochClock, retryInterval.toMillis());
    this.publishInterval = new Delay(epochClock, publishInterval.toMillis());
  }

  @Override
  public String roleName() {
    return "MimirPublisherAgent";
  }

  @Override
  public void onStart() {
    if (state != State.CLOSED) {
      throw new AgentTerminationException("Illegal state: " + state);
    }
    state(State.INIT);
  }

  @Override
  public int doWork() throws Exception {
    try {
      return switch (state) {
        case INIT -> init();
        case RUNNING -> running();
        case CLEANUP -> cleanup();
        default -> throw new AgentTerminationException("Unknown state: " + state);
      };
    } catch (AgentTerminationException e) {
      throw e;
    } catch (Exception e) {
      state(State.CLEANUP);
      throw e;
    }
  }

  private int init() {
    if (retryInterval.isNotOverdue()) {
      return 0;
    }

    executor =
        Executors.newSingleThreadExecutor(
            r -> {
              final var thread = new Thread(r);
              thread.setDaemon(true);
              return thread;
            });
    httpClient = HttpClient.newBuilder().executor(executor).build();
    publishInterval.delay();

    state(State.RUNNING);
    return 1;
  }

  private int running() throws Exception {
    final var fillRate = (double) writeQueue.size() / writeQueue.capacity();

    if (publishInterval.isOverdue() || fillRate > 0.5) {
      publishInterval.delay();
      if (future != null) {
        future.cancel(true);
        future = null;
      }

      final var builder = WriteRequest.newBuilder();
      writeQueue.drain(
          request -> builder.addAllTimeseries(request.getTimeseriesList()), writeLimit);
      final var writeRequest = builder.build();

      if (writeRequest.getTimeseriesCount() > 0) {
        future = send(writeRequest);
      }
    }

    if (future != null) {
      if (future.isDone()) {
        final var response = future.get();
        if (response.statusCode() != 200) {
          LOGGER.warn(
              "Failed to push metrics: HTTP {}, body: {}", response.statusCode(), response.body());
        }
        future = null;
      }
    }

    return 0;
  }

  private CompletableFuture<HttpResponse<String>> send(WriteRequest request) {
    final var payload = request.toByteArray();
    final byte[] compressedPayload;
    try {
      compressedPayload = Snappy.compress(payload);
    } catch (IOException e) {
      LangUtil.rethrowUnchecked(e);
      return null;
    }

    final var httpRequest =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .POST(BodyPublishers.ofByteArray(compressedPayload))
            .build();

    return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString());
  }

  private int cleanup() {
    if (executor != null) {
      executor.shutdownNow();
    }
    // CloseHelper.quietClose(httpClient);
    httpClient = null;
    executor = null;

    if (future != null) {
      future.cancel(true);
      future = null;
    }

    State previous = state;
    if (previous != State.CLOSED) { // when it comes from onClose()
      retryInterval.delay();
      state(State.INIT);
    }
    return 1;
  }

  @Override
  public void onClose() {
    state(State.CLOSED);
    cleanup();
  }

  private void state(State state) {
    LOGGER.debug("[{}][state] {}->{}", roleName(), this.state, state);
    this.state = state;
  }
}
