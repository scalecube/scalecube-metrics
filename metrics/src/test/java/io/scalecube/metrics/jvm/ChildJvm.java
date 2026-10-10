package io.scalecube.metrics.jvm;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

// JVM observed by JvmMetricsReaderAgentJdkTest, driven over stdin/stdout
public class ChildJvm {

  static final int OFF_HEAP_SIZE = 32 * 1024 * 1024;

  private static final List<ByteBuffer> retained = new ArrayList<>();

  private static ByteBuffer touch(ByteBuffer buffer) {
    for (int i = 0; i < buffer.capacity(); i += 4096) {
      buffer.put(i, (byte) 1);
    }
    return buffer;
  }

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
        case "direct" -> {
          // off-heap allocation, as Agrona/Aeron do, touched so it is resident
          retained.add(touch(ByteBuffer.allocateDirect(OFF_HEAP_SIZE)));
          System.out.println("ok");
        }
        case "mmap" -> {
          // memory-mapped file in /dev/shm, as Aeron log buffers, touched so it is resident
          final var file = File.createTempFile("child-jvm-", ".dat", new File("/dev/shm"));
          file.deleteOnExit();
          try (var channel = FileChannel.open(file.toPath(), READ, WRITE)) {
            retained.add(touch(channel.map(MapMode.READ_WRITE, 0, OFF_HEAP_SIZE)));
          }
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
