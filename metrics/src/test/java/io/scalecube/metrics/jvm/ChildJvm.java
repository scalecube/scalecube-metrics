package io.scalecube.metrics.jvm;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;

// JVM observed by JvmMetricsReaderAgentJdkTest, driven over stdin/stdout
public class ChildJvm {

  public static void main(String[] args) throws Exception {
    final var threads = ManagementFactory.getThreadMXBean();
    final var runtime = Runtime.getRuntime();
    final var in = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("ready");
    System.out.flush();

    String line;
    while ((line = in.readLine()) != null) {
      switch (line) {
        case "gc" -> {
          System.gc();
          System.out.println("ok");
        }
        case "stats" ->
            System.out.println(
                runtime.totalMemory()
                    + " "
                    + (runtime.totalMemory() - runtime.freeMemory())
                    + " "
                    + threads.getThreadCount()
                    + " "
                    + threads.getDaemonThreadCount()
                    + " "
                    + threads.getPeakThreadCount()
                    + " "
                    + threads.getTotalStartedThreadCount());
        default -> {
          return;
        }
      }
      System.out.flush();
    }
  }
}
