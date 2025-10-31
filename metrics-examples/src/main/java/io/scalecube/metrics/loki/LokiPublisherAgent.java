package io.scalecube.metrics.loki;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.scalecube.metrics.Delay;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPOutputStream;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LokiPublisherAgent implements Agent {

  private static final Logger LOGGER = LoggerFactory.getLogger(LokiPublisherAgent.class);

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

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
  private HttpClient httpClient;
  private CompletableFuture<HttpResponse<String>> future;
  private State state = State.CLOSED;

  public LokiPublisherAgent(
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
    return "LokiPublisherAgent";
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

    httpClient = HttpClient.newHttpClient();
    publishInterval.delay();

    state(State.RUNNING);
    return 1;
  }

  private int running() throws Exception {
    if (publishInterval.isOverdue()) {
      publishInterval.delay();
      if (future != null) {
        future.cancel(true);
        future = null;
      }

      final var streams = new ArrayList<WriteRequest.Stream>();
      writeQueue.drain(request -> streams.addAll(request.streams()), writeLimit);

      if (!streams.isEmpty()) {
        future = send(new WriteRequest(streams));
      }
    }

    if (future != null) {
      if (future.isDone()) {
        final var response = future.get();
        final var statusCode = response.statusCode();
        if (statusCode != 200 && statusCode != 204) {
          LOGGER.warn("Failed to push metrics: HTTP {}, body: {}", statusCode, response.body());
        }
        future = null;
        return 1;
      }
    }

    return 0;
  }

  private CompletableFuture<HttpResponse<String>> send(WriteRequest request) {
    return httpClient.sendAsync(
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .POST(BodyPublishers.ofByteArray(gzip(request)))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  public static byte[] gzip(WriteRequest request) {
    final var byteStream = new ByteArrayOutputStream();
    try (final var outputStream = new GZIPOutputStream(byteStream)) {
      OBJECT_MAPPER.writeValue(outputStream, request);
    } catch (IOException e) {
      LangUtil.rethrowUnchecked(e);
    }
    return byteStream.toByteArray();
  }

  private int cleanup() {
    CloseHelper.quietClose(httpClient);
    httpClient = null;

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
