#include "gtest/gtest.h"
#include "common/componentstats.h"
#include "common/dbconnector.h"
#include "common/table.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

using namespace swss;

namespace
{

// Poll Redis up to timeoutMs waiting for the writer thread to flush.
bool waitForFlush(Table& tbl,
                  const std::string& key,
                  const std::string& field,
                  const std::string& expected,
                  int timeoutMs = 5000)
{
    using namespace std::chrono;
    auto deadline = steady_clock::now() + milliseconds(timeoutMs);
    std::string value;
    while (steady_clock::now() < deadline)
    {
        if (tbl.hget(key, field, value) && value == expected)
        {
            return true;
        }
        std::this_thread::sleep_for(milliseconds(50));
    }
    return false;
}

} // namespace

// These tests focus on the in-process behavior (no Redis required).
// The writer thread's DB connect attempts will fail and be retried silently;
// that does not affect the getters below, which read from the in-memory map.

TEST(ComponentStats, IncrementBasic)
{
    auto s = ComponentStats::create("UT1");
    s->increment("entity_a", "SET");
    s->increment("entity_a", "SET");
    s->increment("entity_a", "DEL");

    EXPECT_EQ(2U, s->get("entity_a", "SET"));
    EXPECT_EQ(1U, s->get("entity_a", "DEL"));
    EXPECT_EQ(0U, s->get("entity_a", "UNKNOWN"));
    EXPECT_EQ(0U, s->get("unknown_entity", "SET"));
    s->stop();
}

TEST(ComponentStats, IncrementByN)
{
    auto s = ComponentStats::create("UT2");
    s->increment("e", "COMPLETE", 5);
    s->increment("e", "COMPLETE", 3);
    EXPECT_EQ(8U, s->get("e", "COMPLETE"));

    // n=0 is a no-op
    s->increment("e", "COMPLETE", 0);
    EXPECT_EQ(8U, s->get("e", "COMPLETE"));
    s->stop();
}

TEST(ComponentStats, SetValueOverwrites)
{
    auto s = ComponentStats::create("UT3");
    s->increment("e", "GAUGE", 10);
    s->setValue("e", "GAUGE", 3);
    EXPECT_EQ(3U, s->get("e", "GAUGE"));
    s->stop();
}

TEST(ComponentStats, GetAllReturnsAllMetrics)
{
    auto s = ComponentStats::create("UT4");
    s->increment("e", "A", 1);
    s->increment("e", "B", 2);
    s->increment("e", "C", 3);

    auto all = s->getAll("e");
    EXPECT_EQ(3U, all.size());
    EXPECT_EQ(1U, all["A"]);
    EXPECT_EQ(2U, all["B"]);
    EXPECT_EQ(3U, all["C"]);

    EXPECT_TRUE(s->getAll("missing").empty());
    s->stop();
}

TEST(ComponentStats, MultipleEntitiesIndependent)
{
    auto s = ComponentStats::create("UT5");
    s->increment("e1", "X", 10);
    s->increment("e2", "X", 20);
    EXPECT_EQ(10U, s->get("e1", "X"));
    EXPECT_EQ(20U, s->get("e2", "X"));
    s->stop();
}

TEST(ComponentStats, DisabledIsNoOp)
{
    auto s = ComponentStats::create("UT6");
    s->increment("e", "X", 5);
    s->setEnabled(false);
    s->increment("e", "X", 100);      // ignored
    s->setValue("e", "X", 999);       // ignored
    EXPECT_EQ(5U, s->get("e", "X"));
    s->setEnabled(true);
    s->increment("e", "X", 1);
    EXPECT_EQ(6U, s->get("e", "X"));
    s->stop();
}

TEST(ComponentStats, SameComponentReturnsSameInstance)
{
    auto a = ComponentStats::create("UT_SINGLETON");
    auto b = ComponentStats::create("ut_singleton");  // case-insensitive
    EXPECT_EQ(a.get(), b.get());
    a->increment("e", "X", 7);
    EXPECT_EQ(7U, b->get("e", "X"));
    a->stop();
}

TEST(ComponentStats, ConcurrentIncrementsAreExact)
{
    auto s = ComponentStats::create("UT_CONC");
    constexpr int kThreads = 8;
    constexpr int kOps = 1000;

    std::vector<std::thread> ts;
    for (int i = 0; i < kThreads; ++i)
    {
        ts.emplace_back([s]{
            for (int j = 0; j < kOps; ++j)
            {
                s->increment("hot", "SET");
            }
        });
    }
    for (auto& t : ts) t.join();

    EXPECT_EQ(static_cast<uint64_t>(kThreads) * kOps, s->get("hot", "SET"));
    s->stop();
}

TEST(ComponentStats, ConcurrentIncrementAndSetValueDoNotCrash)
{
    // Mixing increment() and setValue() on the same metric from multiple
    // threads must be safe (setValue now takes an exclusive lock so the
    // store + version bump are observed atomically; increment() keeps its
    // shared-lock fast path). This test pounds both code paths at once.
    auto s = ComponentStats::create("UT_MIXED");
    std::atomic<bool> stop{false};

    std::thread incr([&]{
        while (!stop.load()) s->increment("e", "M");
    });
    std::thread setv([&]{
        for (int i = 0; i < 1000 && !stop.load(); ++i)
            s->setValue("e", "M", i);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    stop = true;
    incr.join();
    setv.join();

    // Value is non-deterministic (race between setValue and increment) but
    // must be readable without UB.
    (void)s->get("e", "M");
    s->stop();
}

TEST(ComponentStats, StopIsIdempotent)
{
    auto s = ComponentStats::create("UT_STOP");
    s->stop();
    s->stop();  // should not crash or hang
}

// --- Writer-thread / Redis integration tests ---------------------------------
// These use the local Redis instance that the swss-common test harness brings
// up (the same one used by redis_ut.cpp). They cover the writer-thread paths
// that the in-memory tests above cannot reach.

TEST(ComponentStats, WriterFlushesToRedis)
{
    auto s = ComponentStats::create("UT_FLUSH", "TEST_DB", 1);
    s->increment("e1", "SET", 3);
    s->increment("e1", "DEL", 1);
    s->setValue("e1", "GAUGE", 42);

    DBConnector db("TEST_DB", 0, true);
    Table tbl(&db, "UT_FLUSH_STATS");

    EXPECT_TRUE(waitForFlush(tbl, "e1", "SET", "3"));
    EXPECT_TRUE(waitForFlush(tbl, "e1", "DEL", "1"));
    EXPECT_TRUE(waitForFlush(tbl, "e1", "GAUGE", "42"));

    // A second round of changes should be picked up by the dirty-tracking
    // path on the next flush.
    s->increment("e1", "SET", 7);
    EXPECT_TRUE(waitForFlush(tbl, "e1", "SET", "10"));

    s->stop();
    tbl.del("e1");
}

TEST(ComponentStats, WriterSkipsUnchangedEntities)
{
    // Exercise the "no change since last flush" branch in writerThread.
    auto s = ComponentStats::create("UT_NOCHANGE", "TEST_DB", 1);
    s->increment("e", "X", 1);

    DBConnector db("TEST_DB", 0, true);
    Table tbl(&db, "UT_NOCHANGE_STATS");
    ASSERT_TRUE(waitForFlush(tbl, "e", "X", "1"));

    // Overwrite Redis directly; if the writer thread re-flushed an unchanged
    // entity it would clobber our value back to "1".
    std::vector<FieldValueTuple> sentinel = {{"X", "999"}};
    tbl.set("e", sentinel);

    // Dirty a *different* entity and wait for it to land in Redis. This
    // proves the writer thread is alive and went through at least one flush
    // cycle after we planted the sentinel -- otherwise a hung writer would
    // also leave "999" intact and the test would falsely pass.
    s->increment("other", "Y", 5);
    ASSERT_TRUE(waitForFlush(tbl, "other", "Y", "5"));

    // The unchanged entity "e" must not have been re-written by the writer.
    std::string value;
    ASSERT_TRUE(tbl.hget("e", "X", value));
    EXPECT_EQ("999", value);

    s->stop();
    tbl.del("e");
    tbl.del("other");
}

TEST(ComponentStats, FinalFlushOnShutdown)
{
    // Updates landing between the last periodic flush and stop() must NOT
    // be silently dropped: writerThread runs one final flush pass on
    // shutdown. Use a long interval so a periodic flush cannot race the
    // stop() call.
    auto s = ComponentStats::create("UT_FINAL", "TEST_DB", 60);

    DBConnector db("TEST_DB", 0, true);
    Table tbl(&db, "UT_FINAL_STATS");
    tbl.del("e");

    s->increment("e", "LATE", 7);
    s->setValue("e", "GAUGE_LATE", 99);

    // Sanity-check Redis is empty before stop() — we should NOT have seen a
    // periodic flush land yet given the 60s interval.
    std::string value;
    EXPECT_FALSE(tbl.hget("e", "LATE", value));

    // stop() triggers the final flush in writerThread before join.
    s->stop();

    ASSERT_TRUE(tbl.hget("e", "LATE", value));
    EXPECT_EQ("7", value);
    ASSERT_TRUE(tbl.hget("e", "GAUGE_LATE", value));
    EXPECT_EQ("99", value);

    tbl.del("e");
}

TEST(ComponentStats, DestructorStopsThread)
{
    // Let the shared_ptr go out of scope: the destructor must call stop()
    // and join the writer thread cleanly.
    {
        auto s = ComponentStats::create("UT_DTOR", "TEST_DB", 1);
        s->increment("e", "X", 1);
    }
    // If the destructor failed to join, the test process would hang or
    // crash on later teardown; reaching this line is the assertion.
    SUCCEED();
}

TEST(ComponentStats, RegistryPrunesExpiredInstances)
{
    // Creating and releasing many uniquely-named instances must not leak
    // the static registry (every entry becomes an expired weak_ptr; the
    // next create() prunes them).
    //
    // We can't observe the registry size directly, and we cannot compare
    // raw pointers across re-creations either: the allocator is free to
    // recycle the same address for the second instance after the first
    // one's storage is freed (which is what actually happens in practice
    // on glibc). Instead, verify the *singleton* property end-to-end:
    // after the original UT_PRUNE shared_ptr is released and a churn of
    // unrelated names triggers a prune cycle, re-creating UT_PRUNE must
    // start over from a zero counter -- if the old in-memory state had
    // survived (e.g. because the registry never pruned and somehow kept
    // the instance alive), the counter increment below would observe a
    // non-zero starting value.
    {
        auto s1 = ComponentStats::create("UT_PRUNE");
        s1->increment("e", "X", 7);
        EXPECT_EQ(7U, s1->get("e", "X"));
        s1->stop();
    } // s1 released here; registry slot becomes expired weak_ptr.

    // Create some unrelated instances to drive create() through the prune
    // path. Each call to pruneExpiredRegistryLocked() inside create()
    // should reap the now-expired UT_PRUNE slot.
    for (int i = 0; i < 10; ++i)
    {
        auto tmp = ComponentStats::create("UT_PRUNE_TMP_" + std::to_string(i));
        tmp->stop();
    }

    // Re-create UT_PRUNE. Because the old instance was destroyed, the
    // new one must start with empty in-memory state.
    auto s2 = ComponentStats::create("UT_PRUNE");
    EXPECT_EQ(0U, s2->get("e", "X"));
    s2->stop();
}

TEST(ComponentStats, ConnectRetryOnBadDb)
{
    // Use a database name that does not exist in the test config so the
    // writer thread's DBConnector construction throws, exercising the
    // catch + retry path.
    auto s = ComponentStats::create("UT_BADDB", "DOES_NOT_EXIST_DB", 1);
    s->increment("e", "X", 1);

    // Give the writer thread time to attempt at least one connect.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // In-memory state is still readable even though Redis is unreachable.
    EXPECT_EQ(1U, s->get("e", "X"));

    // stop() must wake the cv_wait inside the retry loop and return
    // promptly without hanging.
    auto start = std::chrono::steady_clock::now();
    s->stop();
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::seconds(2));
}
