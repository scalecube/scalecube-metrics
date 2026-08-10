# scalecube-metrics

High-performance KPI and telemetry library for Java, designed for ultra-low-latency and precise
metrics. Built on Agrona Counters and HdrHistograms, it supports both real-time monitoring and
historical data captures. The library is ideal for systems that require fine-grained performance
insights, high-frequency metrics, and minimal runtime overhead. Lightweight and thread-safe, it can
be integrated into performance-critical applications such as trading platforms, messaging systems,
or any low-latency environment.

## Configuration

`Context` settings come from a `java.util.Properties` object; the no-arg constructor reads
`System.getProperties()`. Any property set to `@null` is treated as not set (default applies), so a
layered or templated config can unset an inherited value.
