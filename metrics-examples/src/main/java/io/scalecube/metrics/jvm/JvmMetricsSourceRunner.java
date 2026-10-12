package io.scalecube.metrics.jvm;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

// JVM to observe with JvmMetricsReaderRunner. Nothing to set up in it: every HotSpot JVM writes
// its hsperfdata file. Holds some off-heap memory, as a messaging application would (a direct
// buffer, and a memory-mapped file in /dev/shm), and makes garbage so GC metrics move. Run it with
// a small heap (-Xmx64m) to see GCs within seconds; then JvmMetricsReaderRunner <printed pid>.
public class JvmMetricsSourceRunner {

  private static final int OFF_HEAP_SIZE = 64 * 1024 * 1024;

  public static void main(String[] args) throws Exception {
    final var direct = touch(ByteBuffer.allocateDirect(OFF_HEAP_SIZE));

    final var shm = new File("/dev/shm").isDirectory() ? new File("/dev/shm") : null;
    final var file = File.createTempFile("jvm-metrics-example-", ".dat", shm);
    file.deleteOnExit();
    final ByteBuffer mapped;
    try (var channel = FileChannel.open(file.toPath(), CREATE, READ, WRITE)) {
      mapped = touch(channel.map(MapMode.READ_WRITE, 0, OFF_HEAP_SIZE));
    }

    System.out.println("pid: " + ProcessHandle.current().pid());
    System.out.println("direct buffer: " + direct.capacity() + " bytes");
    System.out.println("mapped file: " + file + ", " + mapped.capacity() + " bytes");

    final var garbage = new ArrayList<byte[]>();
    while (true) {
      makeGarbage(garbage);
      Thread.sleep(10);
    }
  }

  private static ByteBuffer touch(ByteBuffer buffer) {
    for (int i = 0; i < buffer.capacity(); i += 4096) {
      buffer.put(i, (byte) 1);
    }
    return buffer;
  }

  private static void makeGarbage(List<byte[]> garbage) {
    garbage.add(new byte[ThreadLocalRandom.current().nextInt(1024, 64 * 1024)]);
    if (garbage.size() > 1000) {
      garbage.clear();
    }
  }
}
