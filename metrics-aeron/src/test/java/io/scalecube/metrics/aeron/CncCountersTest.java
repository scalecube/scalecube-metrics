package io.scalecube.metrics.aeron;

import static io.aeron.AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_ERROR_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_MAX_CYCLE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID;
import static io.aeron.AeronCounters.ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_ELECTION_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_MAX_CYCLE_TIME_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID;
import static io.aeron.CommonContext.AERON_DIR_PROP_DEFAULT;
import static io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_NODE_ROLE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.COMMIT_POSITION_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CONSENSUS_MODULE_STATE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.AeronCluster.Context;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.cluster.service.ClusteredServiceContainer.Configuration;
import io.aeron.driver.MediaDriver;
import io.scalecube.metrics.CounterDescriptor;
import io.scalecube.metrics.CountersHandler;
import io.scalecube.metrics.KeyCodec;
import io.scalecube.metrics.KeyFlyweight;
import io.scalecube.metrics.aeron.CncCountersReaderAgent.State;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CncCountersTest {

  private static final Duration READ_INTERVAL = Duration.ofSeconds(3);
  private static final Duration DRIVER_TIMEOUT = Duration.ofMillis(5000);

  private static final int ARCHIVE_ID = 1;
  private static final int CLUSTER_ID = Configuration.clusterId();
  private static final int SERVICE_ID = Configuration.serviceId();

  private static MediaDriver mediaDriver;
  private static Aeron aeron;
  private static Archive archive;
  private static ConsensusModule consensusModule;
  private static ClusteredServiceContainer serviceContainer;
  private static AeronCluster aeronCluster;

  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final KeyCodec keyCodec = new KeyCodec();
  private CncCountersReaderAgent agent;

  @BeforeAll
  static void beforeAll() {
    mediaDriver =
        MediaDriver.launch(
            new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(true));
    aeron = Aeron.connect();
    archive =
        Archive.launch(
            new Archive.Context()
                .archiveId(ARCHIVE_ID)
                .deleteArchiveOnStart(true)
                .recordingEventsEnabled(false)
                .archiveDirectoryName("target/aeron-archive")
                .controlChannel("aeron:udp?endpoint=localhost:8010")
                .replicationChannel("aeron:udp?endpoint=localhost:0"));
    consensusModule =
        ConsensusModule.launch(
            new ConsensusModule.Context()
                .clusterId(CLUSTER_ID)
                .deleteDirOnStart(true)
                .clusterDirectoryName("target/aeron-cluster")
                .ingressChannel("aeron:udp")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .clusterMemberId(0)
                .clusterMembers(
                    "0,"
                        + "localhost:8005,"
                        + "localhost:8006,"
                        + "localhost:8007,"
                        + "localhost:8008,"
                        + "localhost:8010"));
    serviceContainer =
        ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusterId(CLUSTER_ID)
                .serviceId(SERVICE_ID)
                .clusterDirectoryName("target/aeron-cluster")
                .clusteredService(new ClusteredServiceImpl()));

    aeronCluster =
        AeronCluster.connect(
            new Context()
                .ingressChannel("aeron:udp")
                .ingressEndpoints("0=localhost:8005")
                .isIngressExclusive(true)
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .egressListener(new EgressListenerImpl()));
  }

  @AfterAll
  static void afterAll() {
    CloseHelper.quietCloseAll(
        aeronCluster, consensusModule, serviceContainer, archive, aeron, mediaDriver);
  }

  @AfterEach
  void afterEach() {
    if (agent != null) {
      agent.onClose();
    }
  }

  @Test
  void testCncCounters() {
    final MutableReference<List<CounterDescriptor>> reference = new MutableReference<>();
    final CountersHandler countersHandler =
        new CountersHandler() {
          @Override
          public void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
            reference.set(counterDescriptors);
          }
        };

    agent =
        new CncCountersReaderAgent(
            "CncCountersReaderAgent",
            AERON_DIR_PROP_DEFAULT,
            true,
            epochClock,
            READ_INTERVAL,
            DRIVER_TIMEOUT,
            countersHandler);
    agent.onStart();

    agent.doWork(); // INIT -> RUNNING
    assertEquals(State.RUNNING, agent.state());
    epochClock.advance(READ_INTERVAL.toMillis() + 1);
    agent.doWork(); // RUNNING + countersHandler.accept()
    assertEquals(State.RUNNING, agent.state());

    final var counters = reference.get();
    assertNotNull(counters, "counters");
    assertTrue(counters.size() > 0, "counters.size: " + counters.size());
  }

  @MethodSource("testCncCounterKeyConvertersSource")
  @ParameterizedTest
  void testCncCounterKeyConverters(String test, DirectBuffer expectedKey) {
    final MutableReference<List<CounterDescriptor>> reference = new MutableReference<>();
    final CountersHandler countersHandler =
        new CountersHandler() {
          @Override
          public void accept(long timestamp, List<CounterDescriptor> counterDescriptors) {
            reference.set(counterDescriptors);
          }
        };

    agent =
        new CncCountersReaderAgent(
            "CncCountersReaderAgent",
            AERON_DIR_PROP_DEFAULT,
            true,
            epochClock,
            READ_INTERVAL,
            DRIVER_TIMEOUT,
            countersHandler);
    agent.onStart();

    agent.doWork(); // INIT -> RUNNING
    assertEquals(State.RUNNING, agent.state());
    epochClock.advance(READ_INTERVAL.toMillis() + 1);
    agent.doWork(); // RUNNING + countersHandler.accept()
    assertEquals(State.RUNNING, agent.state());

    final var counters = reference.get();
    assertNotNull(counters, "counters");
    assertTrue(counters.size() > 0, "counters.size: " + counters.size());

    final var actualKeys = counters.stream().map(CounterDescriptor::keyBuffer).toList();
    assertTrue(actualKeys.contains(expectedKey), errorMessage(actualKeys, expectedKey));
  }

  private static Stream<Arguments> testCncCounterKeyConvertersSource() {
    final var builder = Stream.<Arguments>builder();

    // Archive counters

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_ERROR_COUNT_TYPE_ID,
            archiveCounterKey("archive_error_count", 1)));

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_CONTROL_SESSIONS_TYPE_ID,
            archiveCounterKey("archive_control_session_count", ARCHIVE_ID)));

    List.of("archive-conductor", "archive-recorder", "archive-replayer")
        .forEach(
            kind -> {
              builder.add(
                  arguments(
                      "Counter TypeId: " + ARCHIVE_MAX_CYCLE_TIME_TYPE_ID,
                      archiveMaxCycleTime(kind)));
            });

    List.of("archive-conductor", "archive-recorder", "archive-replayer")
        .forEach(
            kind -> {
              builder.add(
                  arguments(
                      "Counter TypeId: " + ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
                      archiveCycleTimeThresholdExceededCount(kind)));
            });

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID,
            archiveCounterKey("archive_recorder_max_write_time_nanos", ARCHIVE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID,
            archiveCounterKey("archive_recorder_total_write_bytes", ARCHIVE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID,
            archiveCounterKey("archive_recorder_total_write_time_nanos", ARCHIVE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID,
            archiveCounterKey("archive_replayer_max_read_time_nanos", ARCHIVE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID,
            archiveCounterKey("archive_replayer_total_read_bytes", ARCHIVE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID,
            archiveCounterKey("archive_replayer_total_read_time_nanos", ARCHIVE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID,
            archiveCounterKey("archive_recording_session_count", ARCHIVE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID,
            archiveCounterKey("archive_replay_session_count", ARCHIVE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + ARCHIVE_RECORDING_POSITION_TYPE_ID, archiveRecordingPosition()));

    // Cluster counters

    builder.add(
        arguments(
            "Counter TypeId: " + CONSENSUS_MODULE_STATE_TYPE_ID,
            clusterCounterKey("consensus_module_state", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID,
            clusterCounterKey("consensus_module_error_count", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + ELECTION_STATE_TYPE_ID,
            clusterCounterKey("cluster_election_state", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_ELECTION_COUNT_TYPE_ID,
            clusterCounterKey("cluster_election_count", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID,
            clusterCounterKey("cluster_leadership_term_id", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_NODE_ROLE_TYPE_ID,
            clusterCounterKey("cluster_node_role", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + COMMIT_POSITION_TYPE_ID,
            clusterCounterKey("cluster_commit_position", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID,
            clusterCounterKey("cluster_client_timeout_count", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_MAX_CYCLE_TIME_TYPE_ID,
            clusterCounterKey("cluster_max_cycle_time_nanos", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
            clusterCounterKey("cluster_cycle_time_threshold_exceeded_count", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID,
            clusterCounterKey("cluster_total_max_snapshot_duration_nanos", CLUSTER_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
            clusterCounterKey(
                "cluster_total_snapshot_duration_threshold_exceeded_count", CLUSTER_ID)));

    // ClusteredService counters

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID,
            clusteredServiceCounterKey(
                "clustered_service_max_cycle_time_nanos", CLUSTER_ID, SERVICE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
            clusteredServiceCounterKey(
                "clustered_service_cycle_time_threshold_exceeded_count", CLUSTER_ID, SERVICE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID,
            clusteredServiceCounterKey(
                "clustered_service_max_snapshot_duration_nanos", CLUSTER_ID, SERVICE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
            clusteredServiceCounterKey(
                "clustered_service_snapshot_duration_threshold_exceeded_count",
                CLUSTER_ID,
                SERVICE_ID)));

    builder.add(
        arguments(
            "Counter TypeId: " + CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID,
            clusteredServiceCounterKey("clustered_service_error_count", CLUSTER_ID, SERVICE_ID)));

    return builder.build();
  }

  // Archive counters

  private static DirectBuffer archiveCounterKey(String name, long archiveId) {
    return new KeyFlyweight()
        .wrap(new ExpandableArrayBuffer(), 0)
        .tagsCount(2)
        .stringValue("name", name)
        .longValue("archiveId", archiveId)
        .buffer();
  }

  private static DirectBuffer archiveMaxCycleTime(String kind) {
    return new KeyFlyweight()
        .wrap(new ExpandableArrayBuffer(), 0)
        .tagsCount(3)
        .stringValue("name", "archive_max_cycle_time_nanos")
        .longValue("archiveId", ARCHIVE_ID)
        .stringValue("kind", kind)
        .buffer();
  }

  private static DirectBuffer archiveCycleTimeThresholdExceededCount(String kind) {
    return new KeyFlyweight()
        .wrap(new ExpandableArrayBuffer(), 0)
        .tagsCount(3)
        .stringValue("name", "archive_cycle_time_threshold_exceeded_count")
        .longValue("archiveId", ARCHIVE_ID)
        .stringValue("kind", kind)
        .buffer();
  }

  private static DirectBuffer archiveRecordingPosition() {
    return new KeyFlyweight()
        .wrap(new ExpandableArrayBuffer(), 0)
        .tagsCount(3)
        .stringValue("name", "archive_recording_position")
        .longValue("archiveId", ARCHIVE_ID)
        .intValue("streamId", ConsensusModule.Configuration.logStreamId())
        .buffer();
  }

  // Cluster counters

  private static DirectBuffer clusterCounterKey(String name, int clusterId) {
    return new KeyFlyweight()
        .wrap(new ExpandableArrayBuffer(), 0)
        .tagsCount(2)
        .stringValue("name", name)
        .intValue("clusterId", clusterId)
        .buffer();
  }

  // ClusteredService counters

  private static DirectBuffer clusteredServiceCounterKey(
      String name, int clusterId, int serviceId) {
    return new KeyFlyweight()
        .wrap(new ExpandableArrayBuffer(), 0)
        .tagsCount(3)
        .stringValue("name", name)
        .intValue("clusterId", clusterId)
        .intValue("serviceId", serviceId)
        .buffer();
  }

  private String errorMessage(List<DirectBuffer> actualKeys, DirectBuffer expectedKey) {
    return "Expected key: "
        + keyCodec.decodeKey(expectedKey, 0)
        + " -- was not found in the list of actual keys: "
        + actualKeys.stream().map(b -> keyCodec.decodeKey(b, 0)).map(Record::toString).toList();
  }
}
