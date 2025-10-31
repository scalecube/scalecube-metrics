package io.scalecube.metrics;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 3, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Group)
public class HistogramBenchmark {

  static final long HIGHEST_TRACKABLE_VALUE = TimeUnit.SECONDS.toNanos(1);
  static final int VALUE_COUNT = 1024;

  volatile Histogram current = new Histogram(1, HIGHEST_TRACKABLE_VALUE, 3);
  volatile Histogram swap = new Histogram(1, HIGHEST_TRACKABLE_VALUE, 3);

  int counter = 0;
  final long[] values = new long[VALUE_COUNT];

  @Setup(Level.Trial)
  public void setup() {
    final var random = ThreadLocalRandom.current();
    for (int i = 0; i < VALUE_COUNT; i++) {
      values[i] = random.nextLong(1, HIGHEST_TRACKABLE_VALUE + 1);
    }
  }

  @Benchmark
  @Group("readWriteGroup")
  @GroupThreads(1)
  public void record() {
    final long value = values[counter++ & (VALUE_COUNT - 1)];
    final var histogram = current;
    histogram.recordValue(Math.min(value, HIGHEST_TRACKABLE_VALUE));
  }

  @Benchmark
  @Group("readWriteGroup")
  @GroupThreads(value = 1)
  public void swapAndUpdate() throws InterruptedException {
    Thread.sleep(1000);
    final var value = current;
    current = swap;
    swap = value;
    value.reset();
  }
}
