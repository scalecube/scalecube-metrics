package io.scalecube.metrics;

import static io.scalecube.metrics.CounterTags.COUNTER_VISIBILITY;
import static io.scalecube.metrics.CountersRegistry.Context.COUNTERS_FILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.metrics.CountersRegistry.LayoutDescriptor;
import io.scalecube.metrics.CountersStat.CounterFilter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongPredicate;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CountersStatTest {

  private static final CounterFilter NO_FILTER = new CounterFilter(null, null, null);
  private static final LongPredicate ALIVE = pid -> true;
  private static final LongPredicate GONE = pid -> false;

  @TempDir private File countersDir;

  private CountersRegistry countersRegistry;
  private CounterAllocator allocator;

  @BeforeEach
  void beforeEach() {
    countersRegistry =
        CountersRegistry.create(
            new CountersRegistry.Context().countersDirectoryName(countersDir.getPath()));
    allocator = new CounterAllocator(countersRegistry.countersManager());
  }

  @AfterEach
  void afterEach() {
    countersRegistry.close();
  }

  @Test
  void printsTheCounterNameWithItsTagsAndValue() {
    newCounter("duty_cycle_max_time_ns", 1234567, "roleName", "WORKER");

    final var output = printOutput(NO_FILTER);

    assertTrue(
        output.contains("1,234,567 - duty_cycle_max_time_ns{roleName=WORKER}"),
        () -> "value, name and tags on one line, but was:\n" + output);
  }

  @Test
  void printsTheHeaderAndTheTrailingSeparator() {
    newCounter("errors_total", 1, "roleName", "WORKER");

    final var output =
        printOutput(countersDir, NO_FILTER, null /* the real liveness check, on our own pid */);

    assertTrue(
        output.contains(
            " - Counters Stat (" + countersDir + "), pid " + ProcessHandle.current().pid()),
        () -> "header names the directory and the writing process, but was:\n" + output);
    assertFalse(
        output.contains("(not running)"),
        () -> "this test is the writer, so it is running, but was:\n" + output);
    assertTrue(
        output.endsWith("--" + System.lineSeparator()), () -> "trailing separator:\n" + output);
  }

  @Test
  void tellsTheSameNameApartByItsTags() {
    newCounter("resources_total", 3, "action", "ADDED");
    newCounter("resources_total", 2, "action", "REMOVED");

    final var output = printOutput(NO_FILTER);

    assertTrue(output.contains("3 - resources_total{action=ADDED}"), output);
    assertTrue(output.contains("2 - resources_total{action=REMOVED}"), output);
  }

  @Test
  void filtersByName() {
    newCounter("duty_cycle_max_time_ns", 1, "roleName", "WORKER");
    newCounter("errors_total", 2, "roleName", "WORKER");

    final var output = printOutput(new CounterFilter(Pattern.compile("duty_cycle"), null, null));

    assertTrue(output.contains("duty_cycle_max_time_ns"), output);
    assertFalse(output.contains("errors_total"), output);
  }

  @Test
  void filtersByTag() {
    newCounter("errors_total", 1, "roleName", "SENDER");
    newCounter("errors_total", 2, "roleName", "RECEIVER");

    final var output = printOutput(new CounterFilter(null, null, Pattern.compile("SENDER")));

    assertTrue(output.contains("roleName=SENDER"), output);
    assertFalse(output.contains("roleName=RECEIVER"), output);
  }

  @Test
  void filtersByTypeId() {
    allocator.newCounter(
        7, "typed_total", flyweight -> flyweight.tagsCount(1).stringValue("roleName", "WORKER"));
    newCounter("errors_total", 1, "roleName", "WORKER");

    final var output = printOutput(new CounterFilter(null, Pattern.compile("^7$"), null));

    assertTrue(output.contains("typed_total"), output);
    assertFalse(output.contains("errors_total"), output);
  }

  @Test
  void aFilterThatMatchesNothingStillPrintsTheHeaderAndSeparator() {
    newCounter("errors_total", 1, "roleName", "WORKER");

    final var output =
        printOutput(new CounterFilter(Pattern.compile("no_such_counter"), null, null));

    assertTrue(output.contains(" - Counters Stat ("), output);
    assertFalse(output.contains("errors_total"), output);
    assertTrue(output.endsWith("--" + System.lineSeparator()), output);
  }

  @Test
  void aMissingCountersFileReportsTheDirectoryAndTheOverrideProperty() {
    final var output = printOutput(new File(countersDir, "absent"), NO_FILTER, ALIVE);

    assertTrue(output.contains("Counters file not found: "), output);
    assertTrue(
        output.contains("scalecube.metrics.counters.countersDirectoryName"),
        () -> "points at the property that overrides the directory, but was:\n" + output);
  }

  @Test
  void aMissingCountersFileListsTheSiblingDirectoryThatDoesHoldOne() {
    final var output = printOutput(new File(countersDir, "absent"), NO_FILTER, ALIVE);

    assertTrue(
        output.contains("Counters directories found: "),
        () -> "the directory we are running against is a sibling candidate, but was:\n" + output);
    assertTrue(output.contains(countersDir.getPath()), output);
  }

  @Test
  void aTruncatedCountersFileIsReportedInsteadOfBeingRead() throws IOException {
    final var dir = countersFileOfLength(1);

    final var output = printOutput(dir, NO_FILTER, ALIVE);

    assertTrue(output.contains("Counters file is truncated: "), output);
  }

  @Test
  void aCountersFileShorterThanItsHeaderDeclaresIsReportedInsteadOfBeingRead() throws IOException {
    final var dir = copyOfCountersFileTruncatedTo(LayoutDescriptor.HEADER_LENGTH + 1024);

    final var output = printOutput(dir, NO_FILTER, ALIVE);

    assertTrue(
        output.contains("Counters file is shorter than its header declares: "),
        () ->
            "a partially written or truncated file must not be read as counters, but was:\n"
                + output);
  }

  @Test
  void theHeaderMarksAWriterProcessThatIsNoLongerRunning() {
    newCounter("errors_total", 1, "roleName", "WORKER");

    final var output = printOutput(countersDir, NO_FILTER, GONE);

    assertTrue(
        output.contains("(not running)"),
        () -> "a stale counters file must not look healthy, but was:\n" + output);
  }

  @Test
  void valuesAreGroupedInTheRootLocaleSoRedrawsAreComparableAcrossEnvironments() {
    newCounter("errors_total", 1234567, "roleName", "WORKER");

    final var previous = Locale.getDefault();
    Locale.setDefault(Locale.GERMANY);
    try {
      final var output = printOutput(NO_FILTER);
      assertTrue(output.contains("1,234,567 - errors_total"), output);
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void rendersCounterVisibilityByNameAndSortsTagsByTagName() {
    final var tags =
        Map.<String, Object>of(
            "roleName",
            "WORKER",
            "action",
            "ADDED",
            COUNTER_VISIBILITY,
            (byte) CounterVisibility.PRIVATE.value());

    assertEquals(
        "{action=ADDED, counterVisibility=PRIVATE, roleName=WORKER}",
        CountersStat.formatTags(new Key(tags)),
        "sorted by tag name, visibility rendered by name");
  }

  @Test
  void aCounterWithoutTagsRendersNoBraces() {
    assertTrue(CountersStat.formatTags(new Key(Map.of())).isEmpty());
  }

  private void newCounter(String name, long value, String tag, String tagValue) {
    allocator
        .newCounter(name, flyweight -> flyweight.tagsCount(1).stringValue(tag, tagValue))
        .set(value);
  }

  private File countersFileOfLength(int length) throws IOException {
    final var dir = new File(countersDir, "truncated");
    assertTrue(dir.mkdirs(), "mkdirs");
    try (var file = new RandomAccessFile(new File(dir, COUNTERS_FILE), "rw")) {
      file.setLength(length);
    }
    return dir;
  }

  private File copyOfCountersFileTruncatedTo(int length) throws IOException {
    final var dir = new File(countersDir, "short");
    assertTrue(dir.mkdirs(), "mkdirs");
    final var target = new File(dir, COUNTERS_FILE);
    Files.copy(new File(countersDir, COUNTERS_FILE).toPath(), target.toPath());
    try (var file = new RandomAccessFile(target, "rw")) {
      file.setLength(length);
    }
    return dir;
  }

  private String printOutput(CounterFilter counterFilter) {
    return printOutput(countersDir, counterFilter, ALIVE);
  }

  private static String printOutput(
      File dir, CounterFilter counterFilter, LongPredicate processAlive) {
    final var bytes = new ByteArrayOutputStream();
    try (var out = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
      if (processAlive == null) {
        CountersStat.printOutput(out, dir, counterFilter);
      } else {
        CountersStat.printOutput(out, dir, counterFilter, processAlive);
      }
    }
    return bytes.toString(StandardCharsets.UTF_8);
  }
}
