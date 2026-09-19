package io.scalecube.metrics;

import static io.scalecube.metrics.CounterTags.COUNTER_VISIBILITY;
import static io.scalecube.metrics.CountersRegistry.Context.COUNTERS_DIR_NAME_PROP_NAME;
import static io.scalecube.metrics.CountersRegistry.Context.COUNTERS_FILE;
import static io.scalecube.metrics.CountersRegistry.Context.DEFAULT_COUNTERS_DIR_NAME;
import static org.agrona.IoUtil.mapExistingFile;

import io.scalecube.metrics.CountersRegistry.LayoutDescriptor;
import java.io.File;
import java.io.PrintStream;
import java.nio.channels.FileChannel.MapMode;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongPredicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.AsciiEncoding;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.status.CountersReader;

/**
 * Tool for printing out the counters maintained by a process in its memory-mapped counters file.
 * The file is written by {@link CountersRegistry} and lives in shared memory, so the tool reads it
 * from a separate process without touching the one being observed.
 *
 * <p>Each counter prints as its name (the counter label) followed by its tags, for example {@code
 * duty_cycle_max_time_ns{roleName=WORKER}}. A name on its own is not unique - the same name is
 * allocated many times with different tags - so the tags are part of the identity.
 *
 * <p>The counters directory is resolved exactly as {@link CountersRegistry.Context} resolves it, so
 * with no arguments the tool finds the counters of a process running as the same user. Agrona reads
 * the mapped file through {@code jdk.internal.misc.Unsafe}, so the same {@code --add-opens} the
 * observed process runs with is required:
 *
 * <p><code>java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp "app.jar:lib/*"
 * io.scalecube.metrics.CountersStat</code>
 *
 * <p>A non-standard directory is passed the same way the observed process takes it:
 *
 * <p><code>java ... -Dscalecube.metrics.counters.countersDirectoryName=/dev/shm/counters-app -cp
 * ... CountersStat</code>
 *
 * <p>Filters take regex patterns and are applied together:
 *
 * <p><code>java ... -cp ... CountersStat name=duty_cycle tag=WORKER delay=5</code>
 */
public class CountersStat {

  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT).withZone(ZoneId.systemDefault());
  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT)
          .withZone(ZoneId.systemDefault());

  private static final String ANSI_CLS = "[2J";
  private static final String ANSI_HOME = "[H";

  private static final String DELAY = "delay";
  private static final String WATCH = "watch";
  private static final String COUNTER_NAME = "name";
  private static final String COUNTER_TYPE_ID = "type";
  private static final String COUNTER_TAG = "tag";

  private static final LongPredicate PROCESS_ALIVE = CountersStat::isProcessAlive;

  /**
   * Main method for launching the process.
   *
   * @param args passed to the process
   * @throws InterruptedException if the thread sleep delay is interrupted
   */
  public static void main(String[] args) throws InterruptedException {
    var delayMs = 1000L;
    var watch = true;
    Pattern nameFilter = null;
    Pattern typeFilter = null;
    Pattern tagFilter = null;

    if (0 != args.length) {
      checkForHelp(args);

      for (final var arg : args) {
        final var equalsIndex = arg.indexOf('=');
        if (-1 == equalsIndex) {
          System.out.println("Arguments must be in name=pattern format: Invalid '" + arg + "'");
          return;
        }

        final var argName = arg.substring(0, equalsIndex);
        final var argValue = arg.substring(equalsIndex + 1);

        switch (argName) {
          case WATCH:
            watch = Boolean.parseBoolean(argValue);
            break;

          case DELAY:
            delayMs = Long.parseLong(argValue) * 1000L;
            break;

          case COUNTER_NAME:
            nameFilter = Pattern.compile(argValue);
            break;

          case COUNTER_TYPE_ID:
            typeFilter = Pattern.compile(argValue);
            break;

          case COUNTER_TAG:
            tagFilter = Pattern.compile(argValue);
            break;

          default:
            System.out.println("Unrecognised argument: '" + arg + "'");
            return;
        }
      }
    }

    final var countersDir = countersDir();
    final var counterFilter = new CounterFilter(nameFilter, typeFilter, tagFilter);

    if (watch) {
      workLoop(delayMs, () -> printOutput(System.out, countersDir, counterFilter));
    } else {
      printOutput(System.out, countersDir, counterFilter);
    }
  }

  /**
   * Returns the counters directory the observed process writes to. Taken from {@link
   * CountersRegistry.Context} so the property name and the default live in one place.
   *
   * @return counters directory
   */
  public static File countersDir() {
    return new File(new CountersRegistry.Context().countersDirectoryName());
  }

  /**
   * Prints one snapshot of the counters file: a header line, one line per counter that passes the
   * filter, and a trailing separator. The file is mapped and unmapped per call, so a restart of the
   * observed process shows up on the next call instead of leaving a stale view.
   *
   * @param out stream to print to
   * @param countersDir directory holding {@link CountersRegistry.Context#COUNTERS_FILE}
   * @param counterFilter filter applied to every counter
   */
  public static void printOutput(PrintStream out, File countersDir, CounterFilter counterFilter) {
    printOutput(out, countersDir, counterFilter, PROCESS_ALIVE);
  }

  /**
   * Prints one snapshot, taking the process-liveness check as a parameter so it can be driven in
   * tests.
   *
   * @param out stream to print to
   * @param countersDir directory holding {@link CountersRegistry.Context#COUNTERS_FILE}
   * @param counterFilter filter applied to every counter
   * @param processAlive tests whether the pid recorded in the file is still running
   */
  static void printOutput(
      PrintStream out, File countersDir, CounterFilter counterFilter, LongPredicate processAlive) {
    final var countersFile = new File(countersDir, COUNTERS_FILE);
    if (!countersFile.exists()) {
      printCountersFileNotFound(out, countersDir, countersFile);
      return;
    }

    final var fileLength = (int) Math.min(countersFile.length(), Integer.MAX_VALUE);
    if (!LayoutDescriptor.isCountersHeaderLengthSufficient(fileLength)) {
      out.println("Counters file is truncated: " + countersFile);
      return;
    }

    final var countersByteBuffer = mapExistingFile(countersFile, MapMode.READ_ONLY, COUNTERS_FILE);
    try {
      final var headerBuffer = LayoutDescriptor.createHeaderBuffer(countersByteBuffer);
      if (LayoutDescriptor.countersValuesBufferLength(headerBuffer) <= 0) {
        out.println("Counters file is not initialised yet: " + countersFile);
        return;
      }

      if (!LayoutDescriptor.isCountersFileLengthSufficient(headerBuffer, fileLength)) {
        out.println(
            "Counters file is shorter than its header declares: "
                + countersFile
                + " (file "
                + fileLength
                + " bytes, header declares "
                + LayoutDescriptor.countersFileLength(
                    LayoutDescriptor.countersValuesBufferLength(headerBuffer))
                + ")");
        return;
      }

      printHeader(out, countersDir, headerBuffer, processAlive);

      printCounters(
          out,
          new CountersReader(
              LayoutDescriptor.createCountersMetaDataBuffer(countersByteBuffer, headerBuffer),
              LayoutDescriptor.createCountersValuesBuffer(countersByteBuffer, headerBuffer)),
          counterFilter);
    } finally {
      BufferUtil.free(countersByteBuffer);
    }
  }

  /**
   * Renders the tags of a counter as {@code {tag=value, tag=value}}, sorted by tag name so the same
   * counter renders identically on every redraw. Returns an empty string when there are no tags.
   *
   * @param key decoded counter key
   * @return rendered tags
   */
  public static String formatTags(Key key) {
    final var tags = key.tags();
    if (tags.isEmpty()) {
      return "";
    }

    return tags.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> entry.getKey() + "=" + formatTagValue(entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(", ", "{", "}"));
  }

  private static void printHeader(
      PrintStream out, File countersDir, DirectBuffer headerBuffer, LongPredicate processAlive) {
    final var startTimestamp = LayoutDescriptor.startTimestamp(headerBuffer);
    final var pid = LayoutDescriptor.pid(headerBuffer);
    final var now = System.currentTimeMillis();

    out.print(TIME_FORMAT.format(Instant.ofEpochMilli(now)));
    out.println(
        " - Counters Stat ("
            + countersDir
            + "), pid "
            + pid
            + (processAlive.test(pid) ? "" : " (not running)")
            + ", started "
            + TIMESTAMP_FORMAT.format(Instant.ofEpochMilli(startTimestamp))
            + ", up "
            + formatUptime(now - startTimestamp));
    out.println("======================================================================");
  }

  private static void printCounters(
      PrintStream out, CountersReader counters, CounterFilter counterFilter) {
    final var maxIdWidth = AsciiEncoding.digitCount(counters.maxCounterId());
    final var formatString = "%" + maxIdWidth + "d: %,26d - %s%n";
    final var keyCodec = new KeyCodec();

    counters.forEach(
        (counterId, typeId, keyBuffer, label) -> {
          final var tags = decodeTags(keyCodec, keyBuffer);
          if (counterFilter.filter(typeId, label, tags)) {
            out.format(
                Locale.ROOT,
                formatString,
                counterId,
                counters.getCounterValue(counterId),
                label + tags);
          }
        });

    out.println("--");
  }

  private static String decodeTags(KeyCodec keyCodec, DirectBuffer keyBuffer) {
    try {
      return formatTags(keyCodec.decodeKey(keyBuffer, 0));
    } catch (Exception e) {
      return "{<undecodable key>}";
    }
  }

  private static String formatTagValue(String tag, Object value) {
    if (COUNTER_VISIBILITY.equals(tag) && value instanceof Byte byteValue) {
      return CounterVisibility.get(byteValue).name();
    }
    return String.valueOf(value);
  }

  private static String formatUptime(long uptimeMs) {
    final var duration = Duration.ofMillis(Math.max(0, uptimeMs));
    return String.format(
        Locale.ROOT,
        "%d:%02d:%02d",
        duration.toHours(),
        duration.toMinutesPart(),
        duration.toSecondsPart());
  }

  private static boolean isProcessAlive(long pid) {
    try {
      return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    } catch (Exception e) {
      // unable to tell - do not claim the process is gone
      return true;
    }
  }

  private static void printCountersFileNotFound(
      PrintStream out, File countersDir, File countersFile) {
    out.println("Counters file not found: " + countersFile);
    out.println("Set -D" + COUNTERS_DIR_NAME_PROP_NAME + "=<dir> to read another directory.");

    final var candidates = findCountersDirs(countersDir);
    if (!candidates.isEmpty()) {
      out.println("Counters directories found: " + String.join(", ", candidates));
    }
  }

  /**
   * Lists the directories that do hold a counters file, next to the one that was asked for, next to
   * the library default, and in the temp directory. Covers the case where the observed process runs
   * as a different user than the one running the tool.
   *
   * @param countersDir directory that was asked for
   * @return paths of directories holding a counters file
   */
  private static List<String> findCountersDirs(File countersDir) {
    return Stream.of(
            countersDir.getAbsoluteFile().getParentFile(),
            new File(DEFAULT_COUNTERS_DIR_NAME).getAbsoluteFile().getParentFile(),
            new File(SystemUtil.tmpDirName()))
        .filter(Objects::nonNull)
        .map(File::getPath)
        .distinct()
        .map(File::new)
        .map(dir -> dir.listFiles(file -> new File(file, COUNTERS_FILE).exists()))
        .filter(Objects::nonNull)
        .flatMap(Arrays::stream)
        .map(File::getPath)
        .distinct()
        .sorted()
        .toList();
  }

  private static void workLoop(long delayMs, Runnable outputPrinter) throws InterruptedException {
    final var running = new AtomicBoolean(true);
    try (var ignore = new ShutdownSignalBarrier(() -> running.set(false))) {
      do {
        clearScreen();
        outputPrinter.run();
        Thread.sleep(delayMs);
      } while (running.get());
    }
  }

  private static void clearScreen() {
    System.out.print(ANSI_CLS + ANSI_HOME);
  }

  private static void checkForHelp(String[] args) {
    for (final var arg : args) {
      if ("-?".equals(arg) || "-h".equals(arg) || "-help".equals(arg)) {
        System.out.format(
            "Usage: [--add-opens java.base/jdk.internal.misc=ALL-UNNAMED]%n"
                + "       [-D%s=<counters directory>] CountersStat%n"
                + "\t[delay=<seconds between updates>]%n"
                + "\t[watch=<true|false>]%n"
                + "filter by optional regex patterns:%n"
                + "\t[name=<pattern>]%n"
                + "\t[type=<pattern>]%n"
                + "\t[tag=<pattern>]%n",
            COUNTERS_DIR_NAME_PROP_NAME);

        System.exit(0);
      }
    }
  }

  /**
   * Filter applied to every counter before it is printed. Every supplied pattern must match, an
   * absent pattern matches everything, and a pattern matches when it is found anywhere in the
   * value.
   */
  public static class CounterFilter {

    private final Pattern nameFilter;
    private final Pattern typeFilter;
    private final Pattern tagFilter;

    /**
     * Constructor.
     *
     * @param nameFilter pattern matched against the counter name, or null
     * @param typeFilter pattern matched against the counter type id, or null
     * @param tagFilter pattern matched against the rendered tags, or null
     */
    public CounterFilter(Pattern nameFilter, Pattern typeFilter, Pattern tagFilter) {
      this.nameFilter = nameFilter;
      this.typeFilter = typeFilter;
      this.tagFilter = tagFilter;
    }

    /**
     * Tests one counter against all supplied patterns.
     *
     * @param typeId counter type id
     * @param label counter name
     * @param tags rendered tags, as produced by {@link CountersStat#formatTags(Key)}
     * @return true when the counter passes every filter
     */
    public boolean filter(int typeId, String label, String tags) {
      return match(nameFilter, label)
          && match(typeFilter, Integer.toString(typeId))
          && match(tagFilter, tags);
    }

    private static boolean match(Pattern pattern, String value) {
      return pattern == null || (value != null && pattern.matcher(value).find());
    }
  }
}
