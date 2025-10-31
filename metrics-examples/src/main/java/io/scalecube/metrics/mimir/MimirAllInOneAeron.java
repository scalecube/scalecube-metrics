package io.scalecube.metrics.mimir;

import static io.aeron.Publication.MAX_POSITION_EXCEEDED;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.AeronCluster.Context;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.AdminResponseCode;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.Cluster.Role;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.scalecube.metrics.CountersReaderAgent;
import io.scalecube.metrics.CountersRegistry;
import io.scalecube.metrics.HistogramMetric;
import io.scalecube.metrics.MetricsReaderAgent;
import io.scalecube.metrics.MetricsRecorder;
import io.scalecube.metrics.MetricsTransmitter;
import io.scalecube.metrics.TpsMetric;
import io.scalecube.metrics.aeron.CncCountersReaderAgent;
import java.time.Duration;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.CompositeAgent;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class MimirAllInOneAeron {

  public static void main(String[] args) throws Exception {
    Network network = Network.newNetwork();

    final var mimir =
        new GenericContainer<>("grafana/mimir")
            .withExposedPorts(9009)
            .withNetwork(network)
            .withNetworkAliases("mimir")
            .withCopyFileToContainer(forClasspathResource("mimir.yml"), "/etc/mimir.yml")
            .withCommand("-config.file=/etc/mimir.yml", "-target=all", "-log.level=debug")
            .withLogConsumer(
                outputFrame -> System.err.print("[mimir] " + outputFrame.getUtf8String()));
    mimir.start();

    // Start Grafana container
    GenericContainer<?> grafana =
        new GenericContainer<>("grafana/grafana")
            .withExposedPorts(3000)
            .withNetwork(network)
            .withNetworkAliases("grafana")
            .withEnv("GF_SECURITY_ADMIN_USER", "user")
            .withEnv("GF_SECURITY_ADMIN_PASSWORD", "password")
            .withCopyFileToContainer(
                forClasspathResource("mimir.datasource.yml"),
                "/etc/grafana/provisioning/datasources/datasource.yml");
    grafana.start();

    final var mimirPort = mimir.getMappedPort(9009);
    final var pushUrl = "http://" + mimir.getHost() + ":" + mimirPort + "/api/v1/push";

    String grafanaUrl = "http://" + grafana.getHost() + ":" + grafana.getMappedPort(3000);
    System.out.println("Started Mimir on: " + mimirPort + " | pushUrl: " + pushUrl);
    System.out.println("Grafana is available at: " + grafanaUrl);

    final var metricsRecorder = MetricsRecorder.launch();
    final var metricsTransmitter = MetricsTransmitter.launch();

    final var countersRegistry = CountersRegistry.create();
    final var countersManager = countersRegistry.countersManager();
    final var sessionCounter = countersManager.newCounter("session_count");

    final var mediaDriver =
        MediaDriver.launch(
            new MediaDriver.Context().dirDeleteOnStart(true).dirDeleteOnShutdown(true));
    final var aeron = Aeron.connect();
    final var archive =
        Archive.launch(
            new Archive.Context()
                .recordingEventsEnabled(false)
                .archiveDirectoryName("target/aeron-archive")
                .controlChannel("aeron:udp?endpoint=localhost:8010")
                .replicationChannel("aeron:udp?endpoint=localhost:0"));

    final var consensusModule =
        ConsensusModule.launch(
            new ConsensusModule.Context()
                .ingressChannel("aeron:udp")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .clusterDirectoryName("target/aeron-cluster")
                .clusterMemberId(0)
                .clusterMembers(
                    "0,"
                        + "localhost:8005,"
                        + "localhost:8006,"
                        + "localhost:8007,"
                        + "localhost:8008,"
                        + "localhost:8010"));
    final var serviceContainer =
        ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusterDirectoryName("target/aeron-cluster")
                .clusteredService(new ClusteredServiceImpl(metricsRecorder, sessionCounter)));

    final var aeronCluster =
        AeronCluster.connect(
            new Context()
                .ingressChannel("aeron:udp")
                .ingressEndpoints("0=localhost:8005")
                .isIngressExclusive(true)
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .egressListener(new EgressListenerImpl(metricsRecorder)));

    System.out.println("Started mediaDriver: " + mediaDriver);
    System.out.println("Started aeron: " + aeron);
    System.out.println("Started archive: " + archive);
    System.out.println("Started consensusModule: " + consensusModule);
    System.out.println("Started serviceContainer: " + serviceContainer);
    System.out.println("Connected aeronCluster: " + aeronCluster);

    // Publisher

    final var mimirPublisher = MimirPublisher.launch(new MimirPublisher.Context().url(pushUrl));

    final var compositeAgent =
        new CompositeAgent(
            new CountersReaderAgent(
                "CountersReaderAgent",
                countersRegistry.context().countersDir(),
                true,
                SystemEpochClock.INSTANCE,
                Duration.ofSeconds(1),
                new CountersMimirHandler(null, mimirPublisher.proxy())),
            new CncCountersReaderAgent(
                "CncCountersReaderAgent",
                mediaDriver.aeronDirectoryName(),
                true,
                SystemEpochClock.INSTANCE,
                Duration.ofSeconds(3),
                Duration.ofSeconds(5),
                new CountersMimirHandler(null, mimirPublisher.proxy())),
            new MetricsReaderAgent(
                "MetricsReaderAgent",
                metricsTransmitter.context().broadcastBuffer(),
                SystemEpochClock.INSTANCE,
                Duration.ofSeconds(3),
                new MetricsMimirHandler(null, mimirPublisher.proxy())));

    AgentRunner.startOnThread(
        new AgentRunner(
            new BackoffIdleStrategy(), Throwable::printStackTrace, null, compositeAgent));

    // Receive data

    AgentRunner.startOnThread(
        new AgentRunner(
            new BackoffIdleStrategy(),
            Throwable::printStackTrace,
            null,
            new Agent() {
              @Override
              public int doWork() {
                return aeronCluster.pollEgress();
              }

              @Override
              public String roleName() {
                return "";
              }
            }));

    // Send data

    final var bufferClaim = new BufferClaim();

    while (true) {
      final var claim = aeronCluster.tryClaim(BitUtil.SIZE_OF_LONG, bufferClaim);

      if (claim == Publication.CLOSED
          || claim == MAX_POSITION_EXCEEDED
          || Thread.currentThread().isInterrupted()) {
        throw new RuntimeException("Good bye");
      }

      if (claim > 0) {
        final var buffer = bufferClaim.buffer();
        final var offset = bufferClaim.offset();
        final var i = offset + SESSION_HEADER_LENGTH;
        final var now = System.nanoTime();
        buffer.putLong(i, now);
        bufferClaim.commit();
      }

      Thread.sleep(1);
    }
  }

  private static class ClusteredServiceImpl implements ClusteredService {

    private static final long HIGHEST_TRACKABLE_VALUE = (long) 1e9;
    private static final double CONVERSION_FACTOR = 1e-3;
    private static final long RESOLUTION_MS = 3000;
    private static final int LENGTH = 2 * BitUtil.SIZE_OF_LONG;

    private final MetricsRecorder metricsRecorder;
    private final AtomicCounter sessionCounter;

    private final BufferClaim bufferClaim = new BufferClaim();
    private TpsMetric tpsMetric;
    private HistogramMetric pingMetric;

    private ClusteredServiceImpl(MetricsRecorder metricsRecorder, AtomicCounter sessionCounter) {
      this.metricsRecorder = metricsRecorder;
      this.sessionCounter = sessionCounter;
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
      tpsMetric =
          metricsRecorder.newTps(
              keyFlyweight -> keyFlyweight.tagsCount(1).stringValue("name", "tps"));

      pingMetric =
          metricsRecorder.newHistogram(
              keyFlyweight ->
                  keyFlyweight
                      .tagsCount(2)
                      .stringValue("name", "hft_latency")
                      .stringValue("kind", "ping"),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION_MS);
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
      System.out.println("onSessionOpen: " + session);
      sessionCounter.increment();
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
      System.out.println("onSessionClose: " + session + ", closeReason=" + closeReason);
      sessionCounter.decrement();
    }

    @Override
    public void onSessionMessage(
        ClientSession session,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header) {
      tpsMetric.record();

      final var ping = buffer.getLong(offset);
      final var pong = System.nanoTime();
      final var delta = pong - ping;

      pingMetric.record(delta);

      if (session.tryClaim(LENGTH, bufferClaim) > 0) {
        final var buf = bufferClaim.buffer();
        final var index = bufferClaim.offset() + SESSION_HEADER_LENGTH;
        buf.putLong(index, ping);
        buf.putLong(index + BitUtil.SIZE_OF_LONG, pong);
        bufferClaim.commit();
      }
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {}

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {}

    @Override
    public void onRoleChange(Role newRole) {}

    @Override
    public void onTerminate(Cluster cluster) {}
  }

  private static class EgressListenerImpl implements EgressListener {

    private static final long HIGHEST_TRACKABLE_VALUE = (long) 1e9;
    private static final double CONVERSION_FACTOR = 1e-3;
    private static final long RESOLUTION_MS = 3000;

    private final HistogramMetric pongMetric;
    private final HistogramMetric rttMetric;

    private EgressListenerImpl(MetricsRecorder metricsRecorder) {
      pongMetric =
          metricsRecorder.newHistogram(
              keyFlyweight ->
                  keyFlyweight
                      .tagsCount(2)
                      .stringValue("name", "hft_latency")
                      .stringValue("kind", "pong"),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION_MS);
      rttMetric =
          metricsRecorder.newHistogram(
              keyFlyweight ->
                  keyFlyweight
                      .tagsCount(2)
                      .stringValue("name", "hft_latency")
                      .stringValue("kind", "rtt"),
              HIGHEST_TRACKABLE_VALUE,
              CONVERSION_FACTOR,
              RESOLUTION_MS);
    }

    @Override
    public void onMessage(
        long clusterSessionId,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header) {
      final var now = System.nanoTime();
      final var ping = buffer.getLong(offset);
      final var pong = buffer.getLong(offset + BitUtil.SIZE_OF_LONG);
      pongMetric.record(now - pong);
      rttMetric.record(now - ping);
    }

    @Override
    public void onSessionEvent(
        long correlationId,
        long clusterSessionId,
        long leadershipTermId,
        int leaderMemberId,
        EventCode code,
        String detail) {
      System.out.println(
          "onSessionEvent: clusterSessionId="
              + clusterSessionId
              + ", leadershipTermId="
              + leadershipTermId
              + ", leaderMemberId="
              + leaderMemberId
              + ", code="
              + code
              + ", detail="
              + detail);
    }

    @Override
    public void onNewLeader(
        long clusterSessionId, long leadershipTermId, int leaderMemberId, String ingressEndpoints) {
      System.out.println(
          "onNewLeader: leadershipTermId="
              + leadershipTermId
              + ", leaderMemberId="
              + leaderMemberId);
    }

    @Override
    public void onAdminResponse(
        long clusterSessionId,
        long correlationId,
        AdminRequestType requestType,
        AdminResponseCode responseCode,
        String message,
        DirectBuffer payload,
        int payloadOffset,
        int payloadLength) {
      System.out.println(
          "onAdminResponse: requestType=" + requestType + ", responseCode=" + responseCode);
    }
  }
}
