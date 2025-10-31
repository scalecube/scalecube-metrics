package io.scalecube.metrics;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 3, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Group)
public class VolatileReadMostlyBenchmark {

  volatile int vol = 0;

  @Benchmark
  @Group("readWriteGroup")
  @GroupThreads(1)
  public int reader() {
    return vol;
  }

  @Benchmark
  @Group("readWriteGroup")
  @GroupThreads(value = 1)
  public void infrequentWriter() throws InterruptedException {
    Thread.sleep(1000);
    // read-then-write switchover
    int current = vol;
    vol = current + 1;
  }
}
