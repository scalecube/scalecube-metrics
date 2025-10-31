package io.scalecube.metrics.aeron;

import static io.aeron.AeronCounters.CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID;
import static org.agrona.BitUtil.SIZE_OF_INT;

import io.scalecube.metrics.KeyFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2ObjectHashMap;

public class ClusteredServiceCountersAdapter {

  private final KeyFlyweight keyFlyweight = new KeyFlyweight();

  public static void populate(Int2ObjectHashMap<KeyConverter> map) {
    final var adapter = new ClusteredServiceCountersAdapter();
    map.put(
        CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID, adapter::clusteredServiceMaxCycleTime);
    map.put(
        CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
        adapter::clusteredServiceCycleTimeThresholdExceededCount);
    map.put(
        CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID,
        adapter::clusteredServiceMaxSnapshotDuration);
    map.put(
        CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
        adapter::clusteredServiceSnapshotDurationThresholdExceededCount);
    map.put(CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID, adapter::clusteredServiceErrorCount);
  }

  private DirectBuffer clusteredServiceMaxCycleTime(DirectBuffer keyBuffer, String label) {
    final var clusterId = keyBuffer.getInt(0);
    final var serviceId = keyBuffer.getInt(SIZE_OF_INT);
    return newKey("clustered_service_max_cycle_time_nanos", clusterId, serviceId);
  }

  private DirectBuffer clusteredServiceCycleTimeThresholdExceededCount(
      DirectBuffer keyBuffer, String label) {
    final var clusterId = keyBuffer.getInt(0);
    final var serviceId = keyBuffer.getInt(SIZE_OF_INT);
    return newKey("clustered_service_cycle_time_threshold_exceeded_count", clusterId, serviceId);
  }

  private DirectBuffer clusteredServiceMaxSnapshotDuration(DirectBuffer keyBuffer, String label) {
    final var clusterId = keyBuffer.getInt(0);
    final var serviceId = keyBuffer.getInt(SIZE_OF_INT);
    return newKey("clustered_service_max_snapshot_duration_nanos", clusterId, serviceId);
  }

  private DirectBuffer clusteredServiceSnapshotDurationThresholdExceededCount(
      DirectBuffer keyBuffer, String label) {
    final var clusterId = keyBuffer.getInt(0);
    final var serviceId = keyBuffer.getInt(SIZE_OF_INT);
    return newKey(
        "clustered_service_snapshot_duration_threshold_exceeded_count", clusterId, serviceId);
  }

  private DirectBuffer clusteredServiceErrorCount(DirectBuffer keyBuffer, String label) {
    final var clusterId = keyBuffer.getInt(0);
    final var serviceId = keyBuffer.getInt(SIZE_OF_INT);
    return newKey("clustered_service_error_count", clusterId, serviceId);
  }

  private DirectBuffer newKey(String name, int clusterId, int serviceId) {
    return keyFlyweight
        .wrap(new ExpandableArrayBuffer(), 0)
        .tagsCount(3)
        .stringValue("name", name)
        .intValue("clusterId", clusterId)
        .intValue("serviceId", serviceId)
        .buffer();
  }
}
