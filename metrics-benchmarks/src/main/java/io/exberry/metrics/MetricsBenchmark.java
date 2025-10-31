package io.scalecube.metrics;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Threads(1)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 3, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MetricsBenchmark {

  @Benchmark
  public void nanoTime(Blackhole bh) {
    bh.consume(System.nanoTime());
  }

  @Benchmark
  public void burnCpuMicros() {
    long durationNanos = 20 * 1000;
    long start = System.nanoTime();
    while ((System.nanoTime() - start) < durationNanos) {
      Thread.onSpinWait();
    }
  }
}
