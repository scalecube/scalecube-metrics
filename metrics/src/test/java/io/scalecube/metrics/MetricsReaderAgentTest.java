package io.scalecube.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import io.scalecube.metrics.MetricsReaderAgent.State;
import io.scalecube.metrics.MetricsRecorder.Context;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import org.agrona.CloseHelper;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetricsReaderAgentTest {

  private static final Duration RETRY_INTERVAL = Duration.ofSeconds(3);

  private final CachedEpochClock epochClock = new CachedEpochClock();

  @Test
  void testTruncatedMetricsFileIsReportedInsteadOfCrashing(@TempDir File dir) throws IOException {
    metricsFileOfLength(dir, 8);

    final var handler = mock(MetricsHandler.class);
    final var agent = newAgent(dir, handler);

    assertDoesNotThrow(agent::doWork, "a truncated file must not blow up initialisation");
    assertEquals(State.CLEANUP, agent.state(), "a bad file routes to cleanup");
    agent.doWork();
    assertEquals(State.INIT, agent.state(), "and the agent goes back to retrying");
    verifyNoInteractions(handler);
  }

  @Test
  void testMetricsFileShorterThanItsHeaderDeclaresIsReportedInsteadOfCrashing(@TempDir File dir)
      throws IOException {
    final var sourceDir = Files.createTempDirectory("metrics-reader-agent").toFile();
    final var recorder =
        MetricsRecorder.launch(
            new Context().metricsDirectoryName(sourceDir.getPath()).useAgentInvoker(true));
    try {
      copyTruncated(sourceDir, dir, MetricsRecorder.LayoutDescriptor.HEADER_LENGTH + 1024);
    } finally {
      CloseHelper.quietClose(recorder);
    }

    final var handler = mock(MetricsHandler.class);
    final var agent = newAgent(dir, handler);

    assertDoesNotThrow(agent::doWork, "a file shorter than its header declares must not be read");
    assertEquals(State.CLEANUP, agent.state(), "a bad file routes to cleanup");
    agent.doWork();
    assertEquals(State.INIT, agent.state(), "and the agent goes back to retrying");
    verifyNoInteractions(handler);
  }

  private MetricsReaderAgent newAgent(File dir, MetricsHandler handler) {
    final var agent =
        new MetricsReaderAgent(
            "MetricsReaderAgent", dir, false, epochClock, RETRY_INTERVAL, handler);
    agent.onStart();
    return agent;
  }

  private static void metricsFileOfLength(File dir, int length) throws IOException {
    try (var file = new RandomAccessFile(new File(dir, Context.METRICS_FILE), "rw")) {
      file.setLength(length);
    }
  }

  private static void copyTruncated(File srcDir, File dstDir, int length) throws IOException {
    final var target = new File(dstDir, Context.METRICS_FILE);
    Files.copy(
        new File(srcDir, Context.METRICS_FILE).toPath(),
        target.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    try (var file = new RandomAccessFile(target, "rw")) {
      file.setLength(length);
    }
  }
}
