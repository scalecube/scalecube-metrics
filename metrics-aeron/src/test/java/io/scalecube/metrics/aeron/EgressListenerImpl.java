package io.scalecube.metrics.aeron;

import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.AdminResponseCode;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

public class EgressListenerImpl implements EgressListener {

  @Override
  public void onMessage(
      long clusterSessionId,
      long timestamp,
      DirectBuffer buffer,
      int offset,
      int length,
      Header header) {}

  @Override
  public void onSessionEvent(
      long correlationId,
      long clusterSessionId,
      long leadershipTermId,
      int leaderMemberId,
      EventCode code,
      String detail) {}

  @Override
  public void onNewLeader(
      long clusterSessionId, long leadershipTermId, int leaderMemberId, String ingressEndpoints) {}

  @Override
  public void onAdminResponse(
      long clusterSessionId,
      long correlationId,
      AdminRequestType requestType,
      AdminResponseCode responseCode,
      String message,
      DirectBuffer payload,
      int payloadOffset,
      int payloadLength) {}
}
