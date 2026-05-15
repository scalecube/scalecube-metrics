package io.scalecube.metrics.aeron;

import static io.aeron.CncFileDescriptor.CNC_FILE;

import io.aeron.Aeron;
import io.aeron.RethrowingErrorHandler;
import io.scalecube.metrics.CounterDescriptor;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to read and snapshot counters produced by aeron. */
public class CncCountersSnapshot implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CncCountersSnapshot.class);

  private final String roleName;
  private final String aeronDirectoryName;
  private final boolean warnIfCncNotExists;
  private final Duration driverTimeout;

  private Aeron aeron;
  private CountersReader countersReader;
  private final Int2ObjectHashMap<KeyConverter> keyConverters = new Int2ObjectHashMap<>();
  private final boolean copyKeyBuffer;
  private AgentInvoker conductorAgentInvoker;

  /**
   * Constructor.
   *
   * @param roleName roleName
   * @param aeronDirectoryName aeronDirectoryName
   * @param warnIfCncNotExists whether to log warning if counters file does not exist
   * @param driverTimeout media-driver timeout (see {@link Aeron.Context#driverTimeoutMs(long)})
   * @param copyKeyBuffer whether to allocate new buffers for the counters
   */
  public CncCountersSnapshot(
      String roleName,
      String aeronDirectoryName,
      boolean warnIfCncNotExists,
      Duration driverTimeout,
      boolean copyKeyBuffer) {
    this.roleName = roleName;
    this.aeronDirectoryName = aeronDirectoryName;
    this.warnIfCncNotExists = warnIfCncNotExists;
    this.driverTimeout = driverTimeout;
    this.copyKeyBuffer = copyKeyBuffer;
    ArchiveCountersAdapter.populate(keyConverters);
    ClusterCountersAdapter.populate(keyConverters);
    ClusteredServiceCountersAdapter.populate(keyConverters);
  }

  public boolean attach() {
    final var cncFile = new File(aeronDirectoryName, CNC_FILE);
    if (!cncFile.exists()) {
      if (warnIfCncNotExists) {
        LOGGER.warn("[{}] {} not exists", roleName, cncFile);
      }
      return false;
    }

    aeron =
        Aeron.connect(
            new Aeron.Context()
                .useConductorAgentInvoker(true)
                .aeronDirectoryName(aeronDirectoryName)
                .errorHandler(RethrowingErrorHandler.INSTANCE)
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE)
                .driverTimeoutMs(driverTimeout.toMillis()));
    conductorAgentInvoker = aeron.conductorAgentInvoker();
    countersReader = aeron.countersReader();

    return true;
  }

  public AgentInvoker conductorAgentInvoker() {
    return conductorAgentInvoker;
  }

  public List<CounterDescriptor> snapshot() {

    final var counterDescriptors = new ArrayList<CounterDescriptor>();
    countersReader.forEach(
        (counterId, typeId, keyBuffer, label) -> {
          DirectBuffer keyBufferCopy = keyBuffer;
          if (copyKeyBuffer) {
            final var tmpBuffer = new UnsafeBuffer(new byte[keyBuffer.capacity()]);
            keyBuffer.getBytes(0, tmpBuffer, 0, tmpBuffer.capacity());
            keyBufferCopy = tmpBuffer;
          }

          final var keyConverter = keyConverters.get(typeId);
          if (keyConverter != null) {
            counterDescriptors.add(
                new CounterDescriptor(
                    counterId,
                    typeId,
                    countersReader.getCounterValue(counterId),
                    keyConverter.convert(keyBufferCopy, label),
                    null));
          }
        });

    return counterDescriptors;
  }

  @Override
  public void close() {
    CloseHelper.quietCloseAll(aeron);
    aeron = null;
    conductorAgentInvoker = null;
    countersReader = null;
  }
}
