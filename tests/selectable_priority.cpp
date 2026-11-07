#include "common/dbconnector.h"
#include "common/consumertable.h"
#include "common/notificationconsumer.h"
#include "common/select.h"
#include "common/selectableevent.h"
#include "common/selectabletimer.h"
#include "common/subscriberstatetable.h"
#include "common/netmsg.h"
#include "common/netlink.h"
#include "common/logger.h"
#include "gtest/gtest.h"

#include <stdexcept>
#include <string>


using namespace std;
using namespace swss;


#define DEFAULT_POP_BATCH_SIZE (10)


TEST(Priority, default_pri_values)
{
    std::string tableName = "tableName";

    DBConnector db("TEST_DB", 0, true);

    timespec interval = { .tv_sec = 1, .tv_nsec = 0 };

    NetLink nl;
    ConsumerStateTable cst(&db, tableName);
    ConsumerTable ct(&db, tableName);
    NotificationConsumer nc(&db, tableName);
    RedisSelect rs;
    SelectableEvent se;
    SelectableTimer st(interval);
    SubscriberStateTable sst(&db, tableName);

    EXPECT_EQ(nl.getPri(), 0);
    EXPECT_EQ(cst.getPri(), 0);
    EXPECT_EQ(ct.getPri(), 0);
    EXPECT_EQ(nc.getPri(), 100);
    EXPECT_EQ(rs.getPri(), 0);
    EXPECT_EQ(se.getPri(), 0);
    EXPECT_EQ(st.getPri(), 50);
    EXPECT_EQ(sst.getPri(), 0);
}

TEST(Priority, set_pri_values)
{
    std::string tableName = "tableName";

    DBConnector db("TEST_DB", 0, true);

    timespec interval = { .tv_sec = 1, .tv_nsec = 0 };

    NetLink nl(101);
    ConsumerStateTable cst(&db, tableName, DEFAULT_POP_BATCH_SIZE, 102);
    ConsumerTable ct(&db, tableName, DEFAULT_POP_BATCH_SIZE, 103);
    NotificationConsumer nc(&db, tableName, 104);
    RedisSelect rs(105);
    SelectableEvent se(106);
    SelectableTimer st(interval, 107);
    SubscriberStateTable sst(&db, tableName, DEFAULT_POP_BATCH_SIZE, 108);

    EXPECT_EQ(nl.getPri(), 101);
    EXPECT_EQ(cst.getPri(), 102);
    EXPECT_EQ(ct.getPri(), 103);
    EXPECT_EQ(nc.getPri(), 104);
    EXPECT_EQ(rs.getPri(), 105);
    EXPECT_EQ(se.getPri(), 106);
    EXPECT_EQ(st.getPri(), 107);
    EXPECT_EQ(sst.getPri(), 108);
}

TEST(Priority, priority_select_1)
{
    Select cs;
    Selectable *selectcs;

    SelectableEvent s1(100);
    SelectableEvent s2(1000);
    SelectableEvent s3(10000);

    cs.addSelectable(&s1);
    cs.addSelectable(&s2);
    cs.addSelectable(&s3);

    s1.notify();
    s2.notify();
    s3.notify();

    int ret;
    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_EQ(selectcs, &s3);

    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_EQ(selectcs, &s2);

    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_EQ(selectcs, &s1);
}

TEST(Priority, priority_select_2)
{
    Select cs;
    Selectable *selectcs;

    SelectableEvent s1(100);
    SelectableEvent s2(1000);
    SelectableEvent s3(10000);

    s1.notify();
    s2.notify();
    s3.notify();

    cs.addSelectable(&s1);
    cs.addSelectable(&s2);
    cs.addSelectable(&s3);

    int ret;
    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_EQ(selectcs, &s3);

    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_EQ(selectcs, &s2);

    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_EQ(selectcs, &s1);
}

TEST(Priority, priority_select_3)
{
    Select cs;
    Selectable *selectcs;

    SelectableEvent s1(10);
    SelectableEvent s2(10);
    SelectableEvent s3(10000);

    cs.addSelectable(&s1);
    cs.addSelectable(&s2);
    cs.addSelectable(&s3);

    s1.notify();
    s2.notify();
    s3.notify();

    int ret;
    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_EQ(selectcs, &s3);

    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_TRUE(selectcs==&s1 || selectcs==&s2);

    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_TRUE(selectcs==&s1 || selectcs==&s2);
}

TEST(Priority, priority_select_4)
{
    Select cs;
    Selectable *selectcs;

    SelectableEvent s1(10);
    SelectableEvent s2(10000);
    SelectableEvent s3(10000);

    cs.addSelectable(&s1);
    cs.addSelectable(&s2);
    cs.addSelectable(&s3);

    s1.notify();
    s2.notify();
    s3.notify();

    int ret;
    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_TRUE(selectcs==&s2 || selectcs==&s3);

    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_TRUE(selectcs==&s2 || selectcs==&s3);

    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_EQ(selectcs, &s1);
}

TEST(Priority, priority_select_5)
{
    Select cs;
    Selectable *selectcs;

    SelectableEvent s1(150);
    SelectableEvent s2(1000);

    cs.addSelectable(&s1);
    cs.addSelectable(&s2);

    s1.notify();
    s2.notify();

    int ret;
    ret = cs.select(&selectcs);
    EXPECT_EQ(ret, Select::OBJECT);
    EXPECT_EQ(selectcs, &s2);

    cs.removeSelectable(&s1);

    ret = cs.select(&selectcs, 1000);
    EXPECT_EQ(ret, Select::TIMEOUT);
}

TEST(Priority, priority_select_6)
{
    Select cs;
    Selectable *selectcs1;
    Selectable *selectcs2;

    SelectableEvent s1(1000);
    SelectableEvent s2(1000);

    cs.addSelectable(&s1);
    cs.addSelectable(&s2);

    s1.notify();
    s2.notify();

    int ret;
    ret = cs.select(&selectcs1);
    EXPECT_EQ(ret, Select::OBJECT);

    s1.notify();
    s2.notify();

    ret = cs.select(&selectcs2);
    EXPECT_EQ(ret, Select::OBJECT);
    // we gave fair scheduler. we've read different selectables on the second read
    EXPECT_NE(selectcs1, selectcs2);
}

namespace {

// Mirrors kMaxConsecutiveErrorLogs in common/select.cpp.
constexpr size_t kMaxErrorLogs = 10;

// Throws on every read while failing, standing in for a selectable backed by an
// unreachable redis.
class FailingSelectable : public SelectableEvent
{
public:
    FailingSelectable() { notify(); }

    uint64_t readData() override
    {
        if (m_failing) throw runtime_error("Unable to read redis reply");
        return SelectableEvent::readData();
    }

    void setFailing(bool failing) { m_failing = failing; }

private:
    bool m_failing = true;
};

// Stays readable forever, standing in for a healthy selectable that keeps
// producing data while another one is broken.
class HealthySelectable : public SelectableEvent
{
public:
    HealthySelectable() { notify(); }

    uint64_t readData() override
    {
        uint64_t ret = SelectableEvent::readData();
        notify();
        return ret;
    }
};

class SelectErrorLogging : public ::testing::Test
{
protected:
    Select s;

    // Log suppression state is per-thread, so it outlives an individual test and
    // only a successful read re-arms it. Tests read cleanly before asserting.
    void readCleanly()
    {
        Selectable *sel = nullptr;
        ASSERT_EQ(s.select(&sel, 0), Select::OBJECT);
    }

    // Poll n times, returning how many readData error lines were logged.
    size_t pollAndCountLogs(int n)
    {
        Logger::swssOutputNotify("", "STDERR");
        testing::internal::CaptureStderr();

        for (int i = 0; i < n; i++)
        {
            Selectable *sel = nullptr;
            s.select(&sel, 0);
        }

        string out = testing::internal::GetCapturedStderr();
        Logger::swssOutputNotify("", "SYSLOG");

        size_t logs = 0;
        for (size_t p = out.find("readData error"); p != string::npos;
             p = out.find("readData error", p + 1))
        {
            logs++;
        }
        return logs;
    }
};

}

// A healthy selectable read in the same poll must not re-arm logging for one that
// keeps failing. It is added first so epoll tends to read it before the failing one.
TEST_F(SelectErrorLogging, capped_alongside_a_healthy_selectable)
{
    HealthySelectable good;
    FailingSelectable bad;
    s.addSelectable(&good);
    s.addSelectable(&bad);

    bad.setFailing(false);
    readCleanly();

    bad.setFailing(true);
    bad.notify();
    EXPECT_EQ(pollAndCountLogs(1000), kMaxErrorLogs);
}

// A tight caller loop against a permanently failing selectable logs a bounded
// number of lines instead of filling /var/log, and a later successful read re-arms
// logging so a subsequent outage is still reported.
TEST_F(SelectErrorLogging, capped_then_rearmed_by_a_successful_read)
{
    FailingSelectable bad;
    s.addSelectable(&bad);

    bad.setFailing(false);
    readCleanly();

    bad.setFailing(true);
    bad.notify();
    EXPECT_EQ(pollAndCountLogs(100), kMaxErrorLogs);

    bad.setFailing(false);
    readCleanly();

    bad.setFailing(true);
    bad.notify();
    EXPECT_EQ(pollAndCountLogs(100), kMaxErrorLogs);
}

// A different fd failing gets its own log budget instead of inheriting the
// saturated counter from the fd that was already failing.
TEST_F(SelectErrorLogging, resets_for_a_different_fd)
{
    FailingSelectable first;
    FailingSelectable second;
    s.addSelectable(&first);
    s.addSelectable(&second);

    first.setFailing(false);
    second.setFailing(false);
    readCleanly();

    first.setFailing(true);
    first.notify();
    EXPECT_EQ(pollAndCountLogs(100), kMaxErrorLogs);

    first.setFailing(false);
    second.setFailing(true);
    first.notify();
    second.notify();
    EXPECT_EQ(pollAndCountLogs(100), kMaxErrorLogs);
}
