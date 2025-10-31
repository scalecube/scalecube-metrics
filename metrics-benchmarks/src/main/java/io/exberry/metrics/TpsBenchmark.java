package io.scalecube.metrics;

import java.util.concurrent.TimeUnit;
import org.agrona.collections.MutableLong;
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
public class TpsBenchmark {

  volatile MutableLong current;
  volatile MutableLong swap;

  @Setup(Level.Trial)
  public void setup() {
    current = new MutableLong();
    swap = new MutableLong();
  }

  @Benchmark
  @Group("readWriteGroup")
  @GroupThreads(1)
  public void record() {
    current.increment();
  }

  @Benchmark
  @Group("readWriteGroup")
  @GroupThreads(value = 1)
  public void swapAndUpdate() throws InterruptedException {
    Thread.sleep(1000);
    final var counter = current;
    current = swap;
    swap = counter;
    counter.set(0);
  }
}
