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

@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 3, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(1)
@State(Scope.Thread)
public class VolatileBenchmark {

  int plain = 0;
  volatile int vol = 0;

  @Benchmark
  public int readPlain() {
    return plain;
  }

  @Benchmark
  public int readVolatile() {
    return vol;
  }

  @Benchmark
  public void writePlain() {
    plain++;
  }

  @Benchmark
  public void writeVolatile() {
    vol++;
  }
}
