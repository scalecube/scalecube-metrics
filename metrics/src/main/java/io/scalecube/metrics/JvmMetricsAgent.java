package io.scalecube.metrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import org.agrona.CloseHelper;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JvmMetricsAgent implements Agent {

  private static final Logger LOGGER = LoggerFactory.getLogger(JvmMetricsAgent.class);

  public static final int JVM_COUNTER_TYPE_ID = 1;

  public enum State {
    INIT,
    RUNNING,
    CLEANUP,
    CLOSED
  }

  private final Object2ObjectHashMap<String, AtomicCounter> gcCounters =
      new Object2ObjectHashMap<>();
  private final Delay readInterval;
  private final ThreadMXBean threadMxBean;
  private AtomicCounter memFree;
  private AtomicCounter memAllocated;
  private AtomicCounter memMax;
  private AtomicCounter threadCount;
  private AtomicCounter peakThreadCount;
  private AtomicCounter daemonThreadCount;
  private final CounterAllocator allocator;
  private State state = State.CLOSED;
  private final boolean gcTelemetryEnabled;

  /**
   * Publishes memory, thread and gc-related basic telemetry as counters. Counters have a source tag
   * of "JVM" and type of "memory", "thread", or "gc", respectively.
   *
   * @param allocator the allocator
   * @param clock the clock, ideally a cached epoch clock
   * @param updatePeriodMs how frequently should telemetry data be updated in the counter files,
   *     given in milliseconds
   * @param enableGcTelemetry whether GC telemetry is enabled or not. NOTE that collecting GC
   *     telemetry data will likely produce garbage.
   */
  public JvmMetricsAgent(
      CounterAllocator allocator,
      EpochClock clock,
      Long updatePeriodMs,
      boolean enableGcTelemetry) {
    this.allocator = allocator;
    this.readInterval = new Delay(clock, updatePeriodMs);
    this.threadMxBean = ManagementFactory.getThreadMXBean();
    this.gcTelemetryEnabled = enableGcTelemetry;
  }

  @Override
  public String roleName() {
    return "JvmMetricsAgent";
  }

  @Override
  public void onStart() {
    if (state != State.CLOSED) {
      throw new AgentTerminationException("Illegal state: " + state);
    }

    memFree =
        allocator.newCounter(
            JVM_COUNTER_TYPE_ID,
            "jvm_memory_free_bytes",
            keyFlyweight -> keyFlyweight.tagsCount(0));

    memAllocated =
        allocator.newCounter(
            JVM_COUNTER_TYPE_ID,
            "jvm_memory_total_bytes",
            keyFlyweight -> keyFlyweight.tagsCount(0));

    memMax =
        allocator.newCounter(
            JVM_COUNTER_TYPE_ID, "jvm_memory_max_bytes", keyFlyweight -> keyFlyweight.tagsCount(0));

    threadCount =
        allocator.newCounter(
            JVM_COUNTER_TYPE_ID, "jvm_threads_current", keyFlyweight -> keyFlyweight.tagsCount(0));

    peakThreadCount =
        allocator.newCounter(
            JVM_COUNTER_TYPE_ID, "jvm_threads_peak", keyFlyweight -> keyFlyweight.tagsCount(0));

    daemonThreadCount =
        allocator.newCounter(
            JVM_COUNTER_TYPE_ID, "jvm_threads_daemon", keyFlyweight -> keyFlyweight.tagsCount(0));

    state(State.INIT);
  }

  @Override
  public int doWork() {
    try {
      return switch (state) {
        case INIT -> init();
        case RUNNING -> running();
        case CLEANUP -> cleanup();
        default -> throw new AgentTerminationException("Unknown state: " + state);
      };
    } catch (AgentTerminationException e) {
      throw e;
    } catch (Exception e) {
      state(State.CLEANUP);
      throw e;
    }
  }

  private int init() {
    readInterval.delay();
    state(State.RUNNING);
    return 1;
  }

  private int running() {
    if (readInterval.isNotOverdue()) {
      return 0;
    }

    readInterval.delay();

    final var runtime = Runtime.getRuntime();
    memFree.set(runtime.freeMemory());
    memAllocated.set(runtime.totalMemory());
    memMax.set(runtime.maxMemory());
    threadCount.set(threadMxBean.getThreadCount());
    peakThreadCount.set(threadMxBean.getPeakThreadCount());
    daemonThreadCount.set(threadMxBean.getDaemonThreadCount());

    if (gcTelemetryEnabled) {
      final var beans = ManagementFactory.getGarbageCollectorMXBeans();
      for (GarbageCollectorMXBean bean : beans) {
        final var gcName = MetricNames.sanitizeName(bean.getName());
        allocateGCCounter("jvm_gc_collections_total", gcName).set(bean.getCollectionCount());
        allocateGCCounter("jvm_gc_ollection_time_ms", gcName).set(bean.getCollectionTime());
      }
    }

    return 1;
  }

  private AtomicCounter allocateGCCounter(String name, String gcName) {
    final var existing = gcCounters.get(name);
    if (existing != null) {
      return existing;
    }

    final var counter =
        allocator.newCounter(
            JVM_COUNTER_TYPE_ID,
            name,
            keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("gcName", gcName));

    gcCounters.put(name, counter);
    return counter;
  }

  private int cleanup() {
    State previous = state;
    if (previous != State.CLOSED) { // when it comes from onClose()
      readInterval.delay();
      state(State.INIT);
    }
    return 1;
  }

  @Override
  public void onClose() {
    state(State.CLOSED);
    cleanup();
    CloseHelper.closeAll(
        memFree, memAllocated, memMax, threadCount, peakThreadCount, daemonThreadCount);
    CloseHelper.closeAll(gcCounters.values());
  }

  private void state(State state) {
    LOGGER.debug("[{}][state] {}->{}", roleName(), this.state, state);
    this.state = state;
  }

  public State state() {
    return state;
  }
}
