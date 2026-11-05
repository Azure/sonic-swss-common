#include "gtest/gtest.h"

#include "common/notificationconsumer.h"
#include "common/select.h"

using namespace swss;

TEST(NotificationConsumerInProcess, SendAllowListAndPop)
{
    NotificationConsumer consumer("NOTIFICATIONS",
                                  NotificationQueuePolicy::Fifo,
                                  100,
                                  64);
    ASSERT_TRUE(consumer.isInProcessDelivery());

    consumer.setOpAllowList({"fdb_event"});

    consumer.send("fdb_event", "payload", {});
    consumer.send("other_op", "drop", {});

    EXPECT_TRUE(consumer.hasData());

    std::deque<KeyOpFieldsValuesTuple> entries;
    consumer.pops(entries);

    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(kfvOp(entries[0]), "fdb_event");
    EXPECT_EQ(kfvKey(entries[0]), "payload");

    auto stats = consumer.getStats();
    EXPECT_EQ(stats.received, 2u);
    EXPECT_EQ(stats.dropped_allowlist, 1u);
}

TEST(NotificationConsumerInProcess, SelectWakesOnSend)
{
    NotificationConsumer consumer("NOTIFICATIONS",
                                  NotificationQueuePolicy::Fifo,
                                  100,
                                  64);
    consumer.setOpAllowList({"ping"});

    Select s;
    s.addSelectable(&consumer);

    // Same thread: callback threads use send(); here we only verify eventfd wake.
    consumer.send("ping", "data", {});

    Selectable *sel = nullptr;
    const int rc = s.select(&sel, 1000);
    ASSERT_EQ(rc, Select::OBJECT);
    ASSERT_EQ(sel, &consumer);

    std::string op;
    std::string data;
    std::vector<FieldValueTuple> values;
    consumer.pop(op, data, values);
    EXPECT_EQ(op, "ping");
    EXPECT_EQ(data, "data");
}

TEST(NotificationConsumerInProcess, NotifyPendingWhenQueued)
{
    NotificationConsumer consumer("NOTIFICATIONS",
                                  NotificationQueuePolicy::Fifo,
                                  100,
                                  64);
    consumer.setOpAllowList({"x"});

    consumer.send("x", "1", {});

    Select s;
    s.addSelectable(&consumer);
    consumer.readData();

    consumer.notifyPending();

    Selectable *sel = nullptr;
    const int rc = s.select(&sel, 1000);
    EXPECT_EQ(rc, Select::OBJECT);
    EXPECT_EQ(sel, &consumer);
}

TEST(NotificationConsumerInProcess, LruDedupViaSend)
{
    NotificationConsumer consumer("NOTIFICATIONS",
                                  NotificationQueuePolicy::LruDedup,
                                  100,
                                  64);
    consumer.setOpAllowList({"fdb_event"});

    consumer.send("fdb_event", "same", {});
    consumer.send("fdb_event", "same", {});

    std::deque<KeyOpFieldsValuesTuple> entries;
    consumer.pops(entries);
    EXPECT_EQ(entries.size(), 1u);

    auto *lru = consumer.getLruDedupQueue();
    ASSERT_NE(lru, nullptr);
    auto qs = lru->getStats();
    EXPECT_EQ(qs.pushed, 2u);
    EXPECT_EQ(qs.dedup_hits, 1u);
}
