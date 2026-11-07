#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace swss {

// OtlpSink converts ComponentStats counter snapshots into OTLP/gRPC metric
// exports destined for a local OpenTelemetry Collector.
//
// This is the OTLP half of the dual-sink design described in
// doc/component-stats/component-stats-hld.md (sonic-net/SONiC#2312). Phase 1
// (sonic-net/sonic-swss-common#1180 and sonic-net/sonic-swss#4516) provides
// the in-memory counters and the COUNTERS_DB sink. Phase 2 plugs this class
// into the ComponentStats writer thread so the same snapshot is also
// exported via OTLP.
//
// Design notes:
//   * Construction does not connect; the underlying gRPC channel is created
//     lazily on the first exportBatch() call.
//   * Failures are non-throwing: exportBatch() returns false and writes one
//     log line, so a dead Collector cannot stall the ComponentStats writer
//     thread or affect the DB sink (HLD requirement R9).
//   * The class uses the PIMPL idiom so that callers (notably
//     ComponentStats) do not transitively include any OpenTelemetry C++ SDK
//     headers.
//   * The class is move-only by design; copying a sink would imply two
//     gRPC channels exporting the same counters.
class OtlpSink
{
public:
    struct Config
    {
        // gRPC endpoint, e.g. "localhost:4317". Plaintext on loopback by
        // design — TLS and authentication live in the local OTel Collector,
        // not in the producer.
        std::string endpoint = "localhost:4317";

        // OTel resource attributes applied to every batch.
        //   componentName     → service.name, sonic.component
        //   serviceInstanceId → service.instance.id
        std::string componentName;
        std::string serviceInstanceId;

        // Per-Export() deadline. Short by design: the writer thread runs
        // every intervalSec (default 1 s) and must not block the next tick.
        std::chrono::milliseconds exportTimeout{500};

        // Wall-clock time at which this sink was constructed, used as the
        // start_time of the very first delta export per (entity, metric).
        // Subsequent exports use the previous export's end_time as their
        // start_time, which is the OTLP-defined contract for delta
        // temporality.
        uint64_t startTimeUnixNano = 0;
    };

    // One counter sample contributed by the ComponentStats writer thread.
    //
    //   Final OTel metric name is "sonic.<componentName>.<metric>".
    //   The entity is exported as a data-point attribute, never as part of
    //   the metric name, so dashboards can pivot freely.
    //
    //   value is always the *cumulative* in-memory counter from
    //   ComponentStats. The sink converts cumulative → delta internally
    //   (Geneva mdm only accepts AGGREGATION_TEMPORALITY_DELTA), so callers
    //   never have to track per-sample state.
    struct DataPoint
    {
        std::string entity;
        std::string metric;
        uint64_t    value       = 0;
        bool        isMonotonic = true;  // false ⇒ exported as Gauge
    };

    explicit OtlpSink(Config config);
    ~OtlpSink();

    OtlpSink(const OtlpSink&)            = delete;
    OtlpSink& operator=(const OtlpSink&) = delete;
    OtlpSink(OtlpSink&&) noexcept;
    OtlpSink& operator=(OtlpSink&&) noexcept;

    // Convert one snapshot to OTLP and ship it. Returns true on a
    // successful Export() RPC. Never throws. Safe to call from the
    // ComponentStats writer thread.
    //
    // An empty batch is a no-op and returns true.
    bool exportBatch(const std::vector<DataPoint>& points);

    // Flush in-flight batches and tear down the gRPC channel. Idempotent;
    // exportBatch() after shutdown() is a no-op that returns false.
    void shutdown();

private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;
};

} // namespace swss
