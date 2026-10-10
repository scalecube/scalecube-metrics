package io.scalecube.metrics.jvm;

import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import io.scalecube.metrics.KeyCodec;
import java.io.File;
import java.time.Duration;
import java.util.List;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;

// Reads JVM metrics of another JVM (e.g. JvmMetricsSourceRunner) from outside: its hsperfdata
// file and its /proc/<pid>/statm. Nothing is attached to the observed JVM.
//
// Usage: JvmMetricsReaderRunner <pid>
public class JvmMetricsReaderRunner {

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: JvmMetricsReaderRunner <pid of the JVM to observe>");
      System.exit(1);
    }
    final var pid = args[0];

    final var agent =
        new JvmMetricsReaderAgent(
            "JvmMetricsReaderAgent",
            new File("/tmp/hsperfdata_" + System.getProperty("user.name"), pid),
            new File("/proc/" + pid + "/statm"),
            true,
            SystemEpochClock.INSTANCE,
            Duration.ofSeconds(3),
            new CountersHandler() {
              private final KeyCodec keyCodec = new KeyCodec();

              @Override
              public void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
                System.out.println(timestamp + "| JVM " + pid + ":");
                for (var counter : counterDescriptors) {
                  final var gc = keyCodec.decodeKey(counter.keyBuffer(), 0).stringValue("gc");
                  System.out.println(
                      "  "
                          + counter.label()
                          + (gc != null ? "{gc=\"" + gc + "\"}" : "")
                          + " "
                          + counter.value());
                }
              }
            });

    AgentRunner.startOnThread(
        new AgentRunner(new BackoffIdleStrategy(), Throwable::printStackTrace, null, agent));
  }
}
