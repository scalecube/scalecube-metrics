package io.scalecube.metrics.aeron;

import static io.aeron.AeronCounters.CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_ELECTION_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_NODE_ROLE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.COMMIT_POSITION_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CONSENSUS_MODULE_STATE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID;

import io.scalecube.metrics.KeyFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2ObjectHashMap;

public class ClusterCountersAdapter {

  private final KeyFlyweight keyFlyweight = new KeyFlyweight();

  public static void populate(Int2ObjectHashMap<KeyConverter> map) {
    final var adapter = new ClusterCountersAdapter();
    map.put(CONSENSUS_MODULE_STATE_TYPE_ID, adapter::consensusModuleState);
    map.put(CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID, adapter::consensusModuleErrorCount);
    map.put(ELECTION_STATE_TYPE_ID, adapter::clusterElectionState);
    map.put(CLUSTER_ELECTION_COUNT_TYPE_ID, adapter::clusterElectionCount);
    map.put(CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID, adapter::clusterLeadershipTermId);
    map.put(CLUSTER_NODE_ROLE_TYPE_ID, adapter::clusterNodeRole);
    map.put(COMMIT_POSITION_TYPE_ID, adapter::clusterCommitPosition);
    map.put(CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID, adapter::clusterClientTimeoutCount);
    map.put(CLUSTER_MAX_CYCLE_TIME_TYPE_ID, adapter::clusterMaxCycleTime);
    map.put(
        CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
        adapter::clusterCycleTimeThresholdExceededCount);
    map.put(CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID, adapter::clusterTotalMaxSnapshotDuration);
    map.put(
        CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
        adapter::clusterTotalSnapshotDurationThresholdExceededCount);
  }

  private DirectBuffer consensusModuleState(DirectBuffer keyBuffer, String label) {
    return newKey("consensus_module_state", keyBuffer.getInt(0));
  }

  private DirectBuffer consensusModuleErrorCount(DirectBuffer keyBuffer, String label) {
    return newKey("consensus_module_error_count", keyBuffer.getInt(0));
  }

  private DirectBuffer clusterElectionState(DirectBuffer keyBuffer, String label) {
    return newKey("cluster_election_state", keyBuffer.getInt(0));
  }

  private DirectBuffer clusterElectionCount(DirectBuffer keyBuffer, String label) {
    return newKey("cluster_election_count", keyBuffer.getInt(0));
  }

  private DirectBuffer clusterLeadershipTermId(DirectBuffer keyBuffer, String label) {
    return newKey("cluster_leadership_term_id", keyBuffer.getInt(0));
  }

  private DirectBuffer clusterNodeRole(DirectBuffer keyBuffer, String label) {
    return newKey("cluster_node_role", keyBuffer.getInt(0));
  }

  private DirectBuffer clusterCommitPosition(DirectBuffer keyBuffer, String label) {
    return newKey("cluster_commit_position", keyBuffer.getInt(0));
  }

  private DirectBuffer clusterClientTimeoutCount(DirectBuffer keyBuffer, String label) {
    return newKey("cluster_client_timeout_count", keyBuffer.getInt(0));
  }

  private DirectBuffer clusterMaxCycleTime(DirectBuffer keyBuffer, String label) {
    return newKey("cluster_max_cycle_time_nanos", keyBuffer.getInt(0));
  }

  private DirectBuffer clusterCycleTimeThresholdExceededCount(
      DirectBuffer keyBuffer, String label) {
    return newKey("cluster_cycle_time_threshold_exceeded_count", keyBuffer.getInt(0));
  }

  private DirectBuffer clusterTotalMaxSnapshotDuration(DirectBuffer keyBuffer, String label) {
    return newKey("cluster_total_max_snapshot_duration_nanos", keyBuffer.getInt(0));
  }

  private DirectBuffer clusterTotalSnapshotDurationThresholdExceededCount(
      DirectBuffer keyBuffer, String label) {
    return newKey("cluster_total_snapshot_duration_threshold_exceeded_count", keyBuffer.getInt(0));
  }

  private DirectBuffer newKey(String name, int clusterId) {
    return keyFlyweight
        .wrap(new ExpandableArrayBuffer(), 0)
        .tagsCount(2)
        .stringValue("name", name)
        .intValue("clusterId", clusterId)
        .buffer();
  }
}
