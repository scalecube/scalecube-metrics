package io.scalecube.metrics;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Cost of a metric, on the single producer thread that owns it.
 *
 * <p>A metric is written by one thread and drained by {@link MetricsRecorderAgent} on its own
 * thread once per resolution window, so {@code record} is what the application pays per event and
 * {@code drain} is what the agent pays per window. Both are measured here at one thread, which is
 * the contract - a metric shared between producer threads is not a supported shape.
 */
@State(Scope.Thread)
@Threads(1)
@Fork(jvmArgsAppend = "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED")
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 3, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class RecorderBenchmark {

  private static final long HIGHEST_TRACKABLE_VALUE = TimeUnit.SECONDS.toNanos(1);
  private static final long RESOLUTION_MS = 1000;
  private static final int VALUE_COUNT = 1024;
  private static final int VALUE_MASK = VALUE_COUNT - 1;

  private final long[] values = new long[VALUE_COUNT];
  private int cursor;

  private HistogramRecorder histogramRecorder;
  private HistogramAggregate histogramAggregate;
  private TpsRecorder tpsRecorder;
  private TpsAggregate tpsAggregate;

  @Setup(Level.Trial)
  public void setup() {
    final var random = ThreadLocalRandom.current();
    for (int i = 0; i < VALUE_COUNT; i++) {
      values[i] = random.nextLong(1, HIGHEST_TRACKABLE_VALUE + 1);
    }

    final var keyBuffer = new UnsafeBuffer(new byte[8]);
    histogramRecorder =
        new HistogramRecorder(keyBuffer, HIGHEST_TRACKABLE_VALUE, 1.0, RESOLUTION_MS);
    histogramAggregate =
        new HistogramAggregate(keyBuffer, HIGHEST_TRACKABLE_VALUE, 1.0, RESOLUTION_MS, null, null);
    tpsRecorder = new TpsRecorder(keyBuffer);
    tpsAggregate = new TpsAggregate(keyBuffer, null, null);
  }

  /** What the application thread pays to record one histogram value. */
  @Benchmark
  public void histogramRecord() {
    histogramRecorder.record(values[cursor++ & VALUE_MASK]);
  }

  /** What the application thread pays to count one event. */
  @Benchmark
  public void tpsRecord() {
    tpsRecorder.record();
  }

  /** What the agent pays to drain a histogram, once per resolution window. */
  @Benchmark
  public void histogramDrain() {
    histogramRecorder.swapAndUpdate(histogramAggregate);
  }

  /** What the agent pays to drain a tps counter, once per resolution window. */
  @Benchmark
  public void tpsDrain() {
    tpsRecorder.swapAndUpdate(tpsAggregate);
  }
}
