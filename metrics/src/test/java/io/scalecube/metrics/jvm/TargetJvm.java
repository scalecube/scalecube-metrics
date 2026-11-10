package io.scalecube.metrics.jvm;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// Target JVM of JvmMetricsReaderAgentJdkTest, driven over stdin/stdout: one command per line, one
// reply per line. Oracles are the JVM's own management APIs and the JDK's own hsperfdata reader
// (sun.jvmstat, behind jstat), reached by reflection because the build compiles with --release.
// Needs --add-exports jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED and
// --add-exports java.management/sun.management=ALL-UNNAMED.
public class TargetJvm {

  static final int OFF_HEAP_SIZE = 64 * 1024 * 1024;
  static final int CHUNK_SIZE = 256 * 1024;

  private static final List<Object> retained = new ArrayList<>();
  private static final List<Object> releasable = new ArrayList<>();
  private static final CountDownLatch never = new CountDownLatch(1);
  private static volatile Object sink;

  public static void main(String[] args) throws Exception {
    final var in = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("ready");
    System.out.flush();

    String line;
    while ((line = in.readLine()) != null) {
      final var command = line.split(" ");
      final String reply;
      switch (command[0]) {
        case "gc" -> {
          System.gc();
          reply = "ok";
        }
        case "churn" -> {
          churn();
          reply = "ok";
        }
        case "retain" -> {
          final var chunks = Integer.parseInt(command[1]) * 1024 * 1024 / CHUNK_SIZE;
          for (int i = 0; i < chunks; i++) {
            releasable.add(new byte[CHUNK_SIZE]);
          }
          reply = "ok";
        }
        case "release" -> {
          releasable.clear();
          reply = "ok";
        }
        case "classes" -> reply = String.valueOf(defineClasses(Integer.parseInt(command[1])));
        case "threads" -> {
          startThreads(Integer.parseInt(command[1]), Integer.parseInt(command[2]));
          reply = "ok";
        }
        case "direct" -> {
          // off-heap allocation, as Agrona/Aeron do, touched so it is resident
          retained.add(touch(ByteBuffer.allocateDirect(OFF_HEAP_SIZE)));
          reply = "ok";
        }
        case "mmap" -> {
          retained.add(touch(mapShm()));
          reply = "ok";
        }
        case "snapshot" -> {
          snapshot(Path.of(command[1]));
          reply = "ok";
        }
        case "stats" -> reply = format(stats());
        case "metaspace" -> reply = format(metaspaceAfterGc());
        default -> reply = "unknown command: " + line;
      }
      System.out.println(reply);
      System.out.flush();
    }
    System.exit(0); // parked non-daemon threads would keep the JVM alive
  }

  private static ByteBuffer touch(ByteBuffer buffer) {
    for (int i = 0; i < buffer.capacity(); i += 4096) {
      buffer.put(i, (byte) 1);
    }
    return buffer;
  }

  // memory-mapped file in /dev/shm, as Aeron log buffers; unlinked at once, the mapping keeps it
  private static ByteBuffer mapShm() throws Exception {
    final var file = File.createTempFile("target-jvm-", ".dat", new File("/dev/shm"));
    try (var channel = FileChannel.open(file.toPath(), READ, WRITE)) {
      return channel.map(MapMode.READ_WRITE, 0, OFF_HEAP_SIZE);
    } finally {
      Files.delete(file.toPath());
    }
  }

  // Half of the heap stays live, so G1 starts concurrent cycles (initiating occupancy is 45%), and
  // garbage is made until every collector has run; then a full (ZGC: major) collection.
  private static void churn() throws InterruptedException {
    final var live = new ArrayList<byte[]>();
    final var maxLive = Runtime.getRuntime().maxMemory() / 2 / CHUNK_SIZE;
    final var deadline = System.nanoTime() + 20_000_000_000L;
    while (!allCollectorsRan() && System.nanoTime() < deadline) {
      if (live.size() < maxLive) {
        live.add(new byte[CHUNK_SIZE]);
      }
      for (int i = 0; i < 1000; i++) {
        sink = new byte[1024];
      }
    }
    live.clear();
    System.gc();
  }

  // full collectors run on System.gc(), the others on allocation
  private static boolean allCollectorsRan() {
    for (var gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      final var name = gc.getName();
      final var full =
          name.equals("G1 Old Generation")
              || name.equals("PS MarkSweep")
              || name.equals("MarkSweepCompact")
              || name.startsWith("ZGC Major");
      if (!full && gc.getCollectionCount() == 0) {
        return false;
      }
    }
    return true;
  }

  // Each definition of the same bytes as a hidden class is a new class in metaspace
  private static long defineClasses(int count) throws Exception {
    final byte[] bytes;
    try (var in = TargetJvm.class.getResourceAsStream("TargetJvm$Ballast.class")) {
      bytes = in.readAllBytes();
    }
    final var lookup = MethodHandles.lookup();
    final var metaspace = metaspacePool();
    final var before = metaspace.getUsage().getUsed();
    for (int i = 0; i < count; i++) {
      retained.add(lookup.defineHiddenClass(bytes, false).lookupClass());
    }
    return metaspace.getUsage().getUsed() - before;
  }

  // hsperfdata metaspace values are refreshed at GC, so the pool is read right after one
  private static Map<String, Long> metaspaceAfterGc() {
    System.gc();
    final var usage = metaspacePool().getUsage();
    final var metaspace = new LinkedHashMap<String, Long>();
    metaspace.put("metaspaceUsed", usage.getUsed());
    metaspace.put("metaspaceCommitted", usage.getCommitted());
    return metaspace;
  }

  private static MemoryPoolMXBean metaspacePool() {
    for (var pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getName().equals("Metaspace")) {
        return pool;
      }
    }
    throw new IllegalStateException("no Metaspace pool");
  }

  private static void startThreads(int count, int daemons) {
    for (int i = 0; i < count; i++) {
      final var thread =
          new Thread(() -> {
            try {
              never.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
      thread.setDaemon(i < daemons);
      thread.start();
    }
  }

  // Copy of own hsperfdata file, plus its entries as the JDK's own reader sees them
  private static void snapshot(Path target) throws Exception {
    final var source =
        Path.of(
            "/tmp/hsperfdata_" + System.getProperty("user.name"),
            String.valueOf(ProcessHandle.current().pid()));
    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);

    final var vmIdentifierClass = Class.forName("sun.jvmstat.monitor.VmIdentifier");
    final var monitoredHostClass = Class.forName("sun.jvmstat.monitor.MonitoredHost");
    final var monitoredVmClass = Class.forName("sun.jvmstat.monitor.MonitoredVm");
    final var monitorClass = Class.forName("sun.jvmstat.monitor.Monitor");

    final var vmid =
        vmIdentifierClass
            .getConstructor(String.class)
            .newInstance("file:" + target.toAbsolutePath());
    final var host =
        monitoredHostClass.getMethod("getMonitoredHost", vmIdentifierClass).invoke(null, vmid);
    final var vm =
        monitoredHostClass.getMethod("getMonitoredVm", vmIdentifierClass).invoke(host, vmid);
    final var entries = new Properties();
    try {
      final var monitors =
          (List<?>) monitoredVmClass.getMethod("findByPattern", String.class).invoke(vm, ".*");
      for (var monitor : monitors) {
        final var name = (String) monitorClass.getMethod("getName").invoke(monitor);
        final var vector = (boolean) monitorClass.getMethod("isVector").invoke(monitor);
        final var value = monitorClass.getMethod("getValue").invoke(monitor);
        if (value instanceof Long && !vector) {
          entries.setProperty(name, "J" + value);
        } else if (value instanceof String) {
          entries.setProperty(name, "S" + value);
        }
      }
    } finally {
      monitoredVmClass.getMethod("detach").invoke(vm);
    }
    try (var out = new FileOutputStream(target + ".jdk")) {
      entries.store(out, null);
    }
  }

  private static Map<String, Long> stats() throws Exception {
    final var stats = new LinkedHashMap<String, Long>();

    final var hotspotRuntimeClass = Class.forName("sun.management.HotspotRuntimeMBean");
    final var hotspotRuntime =
        Class.forName("sun.management.ManagementFactoryHelper")
            .getMethod("getHotspotRuntimeMBean")
            .invoke(null);
    stats.put(
        "safepoints",
        (Long) hotspotRuntimeClass.getMethod("getSafepointCount").invoke(hotspotRuntime));
    stats.put(
        "safepointTimeMs",
        (Long) hotspotRuntimeClass.getMethod("getTotalSafepointTime").invoke(hotspotRuntime));
    stats.put(
        "safepointSyncTimeMs",
        (Long) hotspotRuntimeClass.getMethod("getSafepointSyncTime").invoke(hotspotRuntime));

    final var threads = ManagementFactory.getThreadMXBean();
    stats.put("threadsLive", (long) threads.getThreadCount());
    stats.put("threadsDaemon", (long) threads.getDaemonThreadCount());
    stats.put("threadsPeak", (long) threads.getPeakThreadCount());
    stats.put("threadsStarted", threads.getTotalStartedThreadCount());

    // Parallel and Serial commit two survivor spaces, their memory pools show one
    long heapCommitted = 0;
    for (var pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == MemoryType.HEAP) {
        final var committed = pool.getUsage().getCommitted();
        heapCommitted += committed;
        if (pool.getName().contains("Survivor") && !pool.getName().startsWith("G1")) {
          heapCommitted += committed;
        }
      }
    }
    stats.put("heapCommitted", heapCommitted);

    for (var gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      stats.put("gc." + gc.getName() + ".count", gc.getCollectionCount());
      stats.put("gc." + gc.getName() + ".timeMs", gc.getCollectionTime());
    }
    return stats;
  }

  private static String format(Map<String, Long> stats) {
    final var sb = new StringBuilder();
    stats.forEach(
        (key, value) -> sb.append(sb.isEmpty() ? "" : "\t").append(key).append('=').append(value));
    return sb.toString();
  }

  // Loaded many times as hidden class to grow metaspace
  static class Ballast {
    long a0;
    long a1;
    long a2;
    long a3;

    long m0(long x) {
      return x * 31 + a0;
    }

    long m1(long x) {
      return x * 37 + a1;
    }

    long m2(long x) {
      return x * 41 + a2;
    }

    long m3(long x) {
      return x * 43 + a3;
    }

    String s0() {
      return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    }

    String s1() {
      return "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    }
  }
}
