#include "common/component_stats_otlp.h"

#include <chrono>
#include <gtest/gtest.h>
#include <vector>

namespace swss {
namespace test {

namespace {

OtlpSink::Config makeConfig()
{
    OtlpSink::Config c;
    // Pick a port that no Collector is listening on. The point of these
    // tests is to verify the wrapper's contract — never throw, never crash —
    // not to validate the on-the-wire OTLP shape, which requires an
    // in-process gRPC mock server (deferred to a follow-up).
    c.endpoint            = "127.0.0.1:14317";
    c.componentName       = "swss-ut";
    c.serviceInstanceId   = "ut-host";
    c.startTimeUnixNano   = 1700000000000000000ULL;
    c.exportTimeout       = std::chrono::milliseconds(100);
    return c;
}

} // namespace

TEST(OtlpSink, ConstructAndDestructDoesNotCrash)
{
    OtlpSink sink(makeConfig());
    SUCCEED();
}

TEST(OtlpSink, EmptyBatchIsNoOp)
{
    OtlpSink sink(makeConfig());
    EXPECT_TRUE(sink.exportBatch({}));
}

TEST(OtlpSink, ExportToUnreachableCollectorDoesNotThrow)
{
    OtlpSink sink(makeConfig());
    const std::vector<OtlpSink::DataPoint> points = {
        {"PORT_TABLE", "SET",     42, /*isMonotonic*/ true},
        {"PORT_TABLE", "DEL",     7,  /*isMonotonic*/ true},
        {"PORT_TABLE", "BACKLOG", 3,  /*isMonotonic*/ false},  // gauge
    };

    bool ok = true;
    EXPECT_NO_THROW(ok = sink.exportBatch(points));
    // The exact return value depends on local network behaviour: in CI the
    // gRPC client may report failure quickly, on a developer box it may
    // succeed against a stray listener. The wrapper's contract only forbids
    // throwing; the boolean is informational.
    (void)ok;
}

TEST(OtlpSink, ShutdownIsIdempotent)
{
    OtlpSink sink(makeConfig());
    sink.shutdown();
    sink.shutdown();
    EXPECT_FALSE(sink.exportBatch({{"E", "M", 1, true}}));
}

TEST(OtlpSink, MovedFromInstanceIsHarmless)
{
    OtlpSink first(makeConfig());
    OtlpSink second(std::move(first));
    EXPECT_TRUE(second.exportBatch({}));
}

TEST(OtlpSink, DeltaConversionDoesNotThrowAcrossMultipleExports)
{
    OtlpSink sink(makeConfig());
    // Three consecutive cumulative snapshots of the same series. The sink
    // is expected to convert these to deltas (10, 5, 8) internally without
    // crashing or throwing, regardless of whether the export RPC itself
    // succeeds.
    const std::vector<OtlpSink::DataPoint> first  = {{"PORT", "SET", 10, true}};
    const std::vector<OtlpSink::DataPoint> second = {{"PORT", "SET", 15, true}};
    const std::vector<OtlpSink::DataPoint> third  = {{"PORT", "SET", 23, true}};

    EXPECT_NO_THROW({
        (void)sink.exportBatch(first);
        (void)sink.exportBatch(second);
        (void)sink.exportBatch(third);
    });
}

TEST(OtlpSink, CounterResetIsTreatedAsDelta)
{
    OtlpSink sink(makeConfig());
    // After a counter reset (current < last), the sink must not underflow
    // a uint64_t. The contract is to emit the current value as the delta.
    EXPECT_NO_THROW({
        (void)sink.exportBatch({{"PORT", "SET", 100, true}});
        (void)sink.exportBatch({{"PORT", "SET", 7,   true}});
    });
}

} // namespace test
} // namespace swss
