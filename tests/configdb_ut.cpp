#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <unistd.h>
#include "gtest/gtest.h"
#include "common/configdb.h"
#include "common/dbconnector.h"
#include "common/logger.h"

using namespace std;
using namespace swss;

namespace {

const char *CONFIG_DB_NAME = "CONFIG_DB";

// Upper bound on any single wait in these tests. Far above the longest
// expected wait (3 s) so it only trips when the waiter is genuinely stuck.
const int WAIT_TIMEOUT_SEC = 10;

// Connect to CONFIG_DB and clear the init indicator so a subsequent
// wait_for_init=true connect actually blocks.
class ConfigDBWaitForInit : public ::testing::Test
{
protected:
    void SetUp() override
    {
        db.db_connect(CONFIG_DB_NAME, /*wait_for_init=*/false, /*retry_on=*/false);
        client = &db.get_redis_client(CONFIG_DB_NAME);
        client->del(ConfigDBConnector_Native::INIT_INDICATOR);
    }

    void TearDown() override
    {
        // Other tests connect with wait_for_init=true; leave the indicator set.
        set_indicator();
    }

    void set_indicator()
    {
        client->set(ConfigDBConnector_Native::INIT_INDICATOR, "1");
    }

    // Set the indicator from another thread after delay_sec seconds, the
    // same way the restart_waiter and redis_table_waiter tests unblock waits.
    // indicator_set is raised just before the write so tests can check the
    // waiter did not return early, independent of wall-clock timing.
    thread set_indicator_after(int delay_sec)
    {
        indicator_set = false;
        return thread([this, delay_sec]() {
            sleep(static_cast<unsigned int>(delay_sec));
            indicator_set = true;
            set_indicator();
        });
    }

    // Run a blocking wait on a helper thread and fail the test if it has
    // not returned within WAIT_TIMEOUT_SEC. On timeout, set the indicator so
    // the stuck waiter unblocks and can be joined instead of hanging CI.
    // Exceptions thrown by wait_fn are rethrown to the caller.
    template <typename Fn>
    void expect_returns_in_time(Fn wait_fn)
    {
        auto done = async(launch::async, wait_fn);
        bool in_time = done.wait_for(chrono::seconds(WAIT_TIMEOUT_SEC)) == future_status::ready;
        if (!in_time)
        {
            set_indicator();
        }
        done.get();
        EXPECT_TRUE(in_time) << "wait did not return within " << WAIT_TIMEOUT_SEC << " s";
    }

    ConfigDBConnector_Native db;
    DBConnector *client = nullptr;
    atomic<bool> indicator_set{false};
};

TEST_F(ConfigDBWaitForInit, ReturnsImmediatelyWhenAlreadyInitialized)
{
    // Given: the indicator is already set.
    set_indicator();

    // When: waiting for init.
    auto start = chrono::steady_clock::now();
    expect_returns_in_time([&]() { db.wait_for_init_indicator(); });
    auto elapsed = chrono::steady_clock::now() - start;

    // Then: the wait returns without blocking.
    EXPECT_LT(elapsed, chrono::seconds(1));
}

TEST_F(ConfigDBWaitForInit, BlocksUntilIndicatorSet)
{
    // Given: the indicator is unset and will be set 1 second from now.
    auto start = chrono::steady_clock::now();
    thread t = set_indicator_after(1);

    // When: waiting for init with the default schedule.
    expect_returns_in_time([&]() { db.wait_for_init_indicator(); });
    auto elapsed = chrono::steady_clock::now() - start;
    t.join();

    // Then: the wait only returned once the indicator was set.
    EXPECT_TRUE(indicator_set) << "wait returned before the indicator was set";
    EXPECT_GE(elapsed, chrono::seconds(1));
}

TEST_F(ConfigDBWaitForInit, WarnsThenEscalatesWhileBlocked)
{
    // Given: a compressed schedule so the first warning, a repeat warning
    // and the escalated error all fire within a 3 second wait, an indicator
    // that will be set 3 seconds from now, and the logger captured via stderr.
    ConfigDBConnector_Native::WaitForInitSchedule schedule{
        /*first_warn_sec=*/1, /*warn_interval_sec=*/1,
        /*escalate_sec=*/2, /*tty_interval_sec=*/1};
    auto start = chrono::steady_clock::now();
    thread t = set_indicator_after(3);
    Logger::swssOutputNotify("", "STDERR");  // capture syslog output below
    testing::internal::CaptureStderr();

    // When: waiting for init with that schedule.
    expect_returns_in_time([&]() { db.wait_for_init_indicator(schedule); });
    auto elapsed = chrono::steady_clock::now() - start;
    t.join();
    string logs = testing::internal::GetCapturedStderr();
    Logger::swssOutputNotify("", "SYSLOG");  // back to Logger's default output

    // Then: the wait spanned every warning stage, logged a warning and then
    // the escalated error, and returned once the indicator was set.
    EXPECT_TRUE(indicator_set) << "wait returned before the indicator was set";
    EXPECT_GE(elapsed, chrono::seconds(3));
    EXPECT_NE(logs.find("WARN"), string::npos) << logs;
    EXPECT_NE(logs.find("Still waiting for CONFIG_DB_INITIALIZED"), string::npos) << logs;
    EXPECT_NE(logs.find("ERROR"), string::npos) << logs;
    EXPECT_NE(logs.find("Blocked for"), string::npos) << logs;
}

TEST_F(ConfigDBWaitForInit, DbConnectWaitsForInit)
{
    // Given: the indicator is unset and will be set 1 second from now.
    thread t = set_indicator_after(1);

    // When: connecting through the production entry point with wait_for_init.
    ConfigDBConnector_Native waiter;
    expect_returns_in_time([&]() { waiter.db_connect(CONFIG_DB_NAME, /*wait_for_init=*/true, /*retry_on=*/false); });
    t.join();

    // Then: db_connect blocked until the indicator was set and is connected.
    EXPECT_TRUE(indicator_set) << "db_connect returned before the indicator was set";
    EXPECT_EQ(waiter.getDbName(), CONFIG_DB_NAME);
}

} // namespace
