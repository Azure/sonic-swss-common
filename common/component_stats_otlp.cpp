#include "common/component_stats_otlp.h"

#include "common/logger.h"

#include <chrono>
#include <utility>
#include <unordered_map>

#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_options.h>
#include <opentelemetry/sdk/common/exporter_utils.h>
#include <opentelemetry/sdk/instrumentationscope/instrumentation_scope.h>
#include <opentelemetry/sdk/metrics/data/metric_data.h>
#include <opentelemetry/sdk/metrics/data/point_data.h>
#include <opentelemetry/sdk/metrics/export/metric_producer.h>
#include <opentelemetry/sdk/metrics/instruments.h>
#include <opentelemetry/sdk/metrics/push_metric_exporter.h>
#include <opentelemetry/sdk/resource/resource.h>

namespace otlp_exporter = opentelemetry::exporter::otlp;
namespace sdk_common    = opentelemetry::sdk::common;
namespace sdk_metrics   = opentelemetry::sdk::metrics;
namespace sdk_resource  = opentelemetry::sdk::resource;
namespace sdk_scope     = opentelemetry::sdk::instrumentationscope;

namespace swss {

namespace {

// One InstrumentationScope per process is plenty; the SDK does not require
// it to be unique per sink.
std::unique_ptr<sdk_scope::InstrumentationScope> makeScope()
{
    return sdk_scope::InstrumentationScope::Create(
        "swss.component_stats", "1.0", "https://opentelemetry.io/schemas/1.30.0");
}

opentelemetry::common::SystemTimestamp toSystemTimestamp(uint64_t unixNano)
{
    return opentelemetry::common::SystemTimestamp{
        std::chrono::system_clock::time_point{std::chrono::nanoseconds{unixNano}}};
}

} // namespace

struct OtlpSink::Impl
{
    // Per-series state needed to convert cumulative counters into the delta
    // points that Geneva mdm requires. Keyed by "<entity>\x1f<metric>".
    struct SeriesState
    {
        uint64_t                              lastValue = 0;
        opentelemetry::common::SystemTimestamp lastEndTs;
        bool                                  hasLastEndTs = false;
    };

    Config                                           config;
    sdk_resource::Resource                           resource;
    std::unique_ptr<sdk_scope::InstrumentationScope> scope;
    std::unique_ptr<sdk_metrics::PushMetricExporter> exporter;
    std::unordered_map<std::string, SeriesState>     series;
    opentelemetry::common::SystemTimestamp           creationTs;
    bool                                             stopped = false;

    explicit Impl(Config c)
        : config(std::move(c)),
          resource(sdk_resource::Resource::Create({
              {"service.name",        config.componentName},
              {"service.instance.id", config.serviceInstanceId},
              {"sonic.component",     config.componentName},
          })),
          scope(makeScope()),
          creationTs(toSystemTimestamp(config.startTimeUnixNano))
    {
        otlp_exporter::OtlpGrpcMetricExporterOptions opts;
        opts.endpoint            = config.endpoint;
        opts.use_ssl_credentials = false;
        opts.timeout             = std::chrono::duration_cast<std::chrono::system_clock::duration>(
            config.exportTimeout);
        exporter = otlp_exporter::OtlpGrpcMetricExporterFactory::Create(opts);
    }

    bool exportBatch(const std::vector<DataPoint>& points)
    {
        if (stopped)
        {
            return false;
        }
        if (points.empty())
        {
            return true;
        }

        const auto endTs = opentelemetry::common::SystemTimestamp{std::chrono::system_clock::now()};

        // Group data points by full metric name. Each metric carries one
        // PointDataAttributes per entity, which keeps "entity" as a label
        // rather than as part of the metric name.
        //
        // For Sum points we emit DELTA temporality (Geneva mdm rejects
        // CUMULATIVE), so we maintain a per-series cache of the last
        // cumulative value and the last end_ts. delta = current - last; on
        // counter reset (current < last) we treat current as the delta and
        // restart the window at creationTs.
        std::unordered_map<std::string, sdk_metrics::MetricData> byMetric;

        for (const auto& dp : points)
        {
            const std::string fullName = "sonic." + config.componentName + "." + dp.metric;
            const std::string seriesKey = dp.entity + "\x1f" + dp.metric;
            auto& state = series[seriesKey];

            auto it = byMetric.find(fullName);
            if (it == byMetric.end())
            {
                sdk_metrics::MetricData md;
                md.instrument_descriptor.name_        = fullName;
                md.instrument_descriptor.description_ = "";
                md.instrument_descriptor.unit_        = "1";
                md.instrument_descriptor.type_        = dp.isMonotonic
                    ? sdk_metrics::InstrumentType::kCounter
                    : sdk_metrics::InstrumentType::kGauge;
                md.instrument_descriptor.value_type_  = sdk_metrics::InstrumentValueType::kLong;
                // Gauge ignores temporality; Sum requires DELTA for mdm.
                md.aggregation_temporality            = dp.isMonotonic
                    ? sdk_metrics::AggregationTemporality::kDelta
                    : sdk_metrics::AggregationTemporality::kUnspecified;
                md.start_ts                           = state.hasLastEndTs ? state.lastEndTs : creationTs;
                md.end_ts                             = endTs;
                it = byMetric.emplace(fullName, std::move(md)).first;
            }

            sdk_metrics::PointDataAttributes pda;
            pda.attributes.SetAttribute("entity", dp.entity);

            if (dp.isMonotonic)
            {
                const uint64_t delta = (dp.value >= state.lastValue)
                    ? (dp.value - state.lastValue)
                    : dp.value;  // counter reset: treat current as delta

                sdk_metrics::SumPointData spd;
                spd.value_        = static_cast<int64_t>(delta);
                spd.is_monotonic_ = true;
                pda.point_data    = std::move(spd);
            }
            else
            {
                sdk_metrics::LastValuePointData lvd;
                lvd.value_                = static_cast<int64_t>(dp.value);
                lvd.is_lastvalue_valid_   = true;
                lvd.sample_ts_            = endTs;
                pda.point_data            = std::move(lvd);
            }

            it->second.point_data_attr_.push_back(std::move(pda));

            state.lastValue    = dp.value;
            state.lastEndTs    = endTs;
            state.hasLastEndTs = true;
        }

        sdk_metrics::ScopeMetrics scopeMetrics;
        scopeMetrics.scope_ = scope.get();
        scopeMetrics.metric_data_.reserve(byMetric.size());
        for (auto& kv : byMetric)
        {
            scopeMetrics.metric_data_.push_back(std::move(kv.second));
        }

        sdk_metrics::ResourceMetrics rm;
        rm.resource_ = &resource;
        rm.scope_metric_data_.push_back(std::move(scopeMetrics));

        try
        {
            const auto result = exporter->Export(rm);
            if (result != sdk_common::ExportResult::kSuccess)
            {
                SWSS_LOG_WARN("OtlpSink: Export to %s returned %d",
                              config.endpoint.c_str(), static_cast<int>(result));
                return false;
            }
        }
        catch (const std::exception& e)
        {
            SWSS_LOG_WARN("OtlpSink: Export to %s threw: %s",
                          config.endpoint.c_str(), e.what());
            return false;
        }
        return true;
    }

    void shutdown()
    {
        if (stopped)
        {
            return;
        }
        stopped = true;

        if (!exporter)
        {
            return;
        }

        try
        {
            exporter->ForceFlush(config.exportTimeout);
            exporter->Shutdown(config.exportTimeout);
        }
        catch (...)
        {
            // shutdown() is contractually noexcept from the caller's view.
        }
    }
};

OtlpSink::OtlpSink(Config config)
    : m_impl(std::make_unique<Impl>(std::move(config)))
{
}

OtlpSink::~OtlpSink()
{
    if (m_impl)
    {
        m_impl->shutdown();
    }
}

OtlpSink::OtlpSink(OtlpSink&&) noexcept            = default;
OtlpSink& OtlpSink::operator=(OtlpSink&&) noexcept = default;

bool OtlpSink::exportBatch(const std::vector<DataPoint>& points)
{
    return m_impl->exportBatch(points);
}

void OtlpSink::shutdown()
{
    m_impl->shutdown();
}

} // namespace swss
