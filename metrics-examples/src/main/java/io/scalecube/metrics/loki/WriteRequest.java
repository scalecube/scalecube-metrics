package io.scalecube.metrics.loki;

import java.util.List;
import java.util.Map;

public class WriteRequest {

  private List<Stream> streams;

  public WriteRequest() {}

  public WriteRequest(List<Stream> streams) {
    this.streams = streams;
  }

  public List<Stream> streams() {
    return streams;
  }

  public static class Stream {

    private Map<String, String> stream; // e.g., {job="test", level="info"}
    private List<String[]> values; // each entry: [timestamp, log line]

    public Stream() {}

    public Stream(Map<String, String> stream, List<String[]> values) {
      this.stream = stream;
      this.values = values;
    }

    public Map<String, String> stream() {
      return stream;
    }

    public List<String[]> values() {
      return values;
    }
  }
}
