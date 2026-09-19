package io.scalecube.metrics;

import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.status.AtomicCounter;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Cost of moving a counter, on the producer thread.
 *
 * <p>{@link ConcurrentCounters#counter(String, java.util.function.Consumer)} encodes the type, name
 * and tags into a scratch buffer and looks the result up in a map on every call, so the difference
 * between {@code lookupThenIncrement} and {@code increment} is what holding on to the returned
 * {@link AtomicCounter} is worth.
 */
@State(Scope.Benchmark)
@Threads(1)
@Fork(jvmArgsAppend = "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED")
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 3, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CountersBenchmark {

  private static final String NAME = "bench_total";
  private static final String TAG = "roleName";
  private static final String TAG_VALUE = "WORKER";

  private CountersRegistry countersRegistry;
  private ConcurrentCounters concurrentCounters;
  private AtomicCounter cachedCounter;

  @Setup(Level.Trial)
  public void setup() {
    countersRegistry =
        CountersRegistry.create(
            new CountersRegistry.Context()
                .countersDirectoryName(CountersRegistry.Context.generateCountersDirectoryName())
                .dirDeleteOnShutdown(true));
    concurrentCounters = new ConcurrentCounters(countersRegistry.countersManager());
    cachedCounter =
        concurrentCounters.counter(NAME, fly -> fly.tagsCount(1).stringValue(TAG, TAG_VALUE));
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    countersRegistry.close();
  }

  /** The caller already holds the counter - just move it. */
  @Benchmark
  public void increment() {
    cachedCounter.increment();
  }

  /** The caller looks the counter up by name and tags on every event. */
  @Benchmark
  public void lookupThenIncrement() {
    concurrentCounters
        .counter(NAME, fly -> fly.tagsCount(1).stringValue(TAG, TAG_VALUE))
        .increment();
  }
}
