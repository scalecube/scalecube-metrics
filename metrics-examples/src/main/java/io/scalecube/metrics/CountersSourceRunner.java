package io.scalecube.metrics;

import io.aeron.archive.Archive;
import io.aeron.driver.MediaDriver;

public class CountersSourceRunner {

  public static void main(String[] args) {
    final var mediaDriver = MediaDriver.launch();
    final var archive =
        Archive.launch(
            new Archive.Context()
                .archiveDirectoryName("target/aeron-archive")
                .controlChannel("aeron:udp?endpoint=localhost:8010")
                .replicationChannel("aeron:udp?endpoint=localhost:0"));

    final var countersRegistry = CountersRegistry.create();
    final var sessionCounter = countersRegistry.countersManager().newCounter("session_count");
    sessionCounter.set(42);

    System.out.println(mediaDriver.aeronDirectoryName());
    System.out.println(countersRegistry.context().countersDirectoryName());
  }
}
