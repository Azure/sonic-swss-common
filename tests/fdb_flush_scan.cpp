#include "gtest/gtest.h"

#include "common/dbconnector.h"
#include "common/producertable.h"
#include "common/consumertable.h"
#include "common/redisapi.h"
#include "common/rediscommand.h"
#include "common/redisreply.h"

#include <inttypes.h>
#include <stdlib.h>

#include <string>
#include <system_error>
#include <vector>

using namespace std;
using namespace swss;

// Tests for common/fdb_flush_scan.lua.
//
// The script is invoked without KEYS, all parameters are ARGV:
//   ARGV[1] cursor, ARGV[2] COUNT hint, ARGV[3] MATCH pattern,
//   ARGV[4] bridge port oid ("" == any), ARGV[5] "0" dynamic only / "1" all.
// It returns two strings: { nextCursor, deletedCount } and the caller has to
// invoke it again until nextCursor is "0".

static const char *tableName = "ASIC_STATE";

// Every entry created by populate() / insertMany() lives below this prefix, so
// a plain "<prefix>*" MATCH pattern is enough for the "flush everything" case.
static const char *fdbPattern = "ASIC_STATE:SAI_OBJECT_TYPE_FDB_ENTRY:*";

// Bridge port used by the port filter tests.
static const uint64_t portA = 0x13a000000000001;
static const uint64_t portB = 0x13a000000000002;

static void clear()
{
    DBConnector db("TEST_DB", 0, true);

    RedisReply r(&db, "FLUSHALL", REDIS_REPLY_STATUS);

    r.checkStatusOK();
}

static std::string sOid(uint64_t oid)
{
    char buffer[100];

    sprintf(buffer, "oid:0x%" PRIx64, oid);

    return buffer;
}

static std::string macKey(unsigned char mac)
{
    char buffer[100];

    sprintf(buffer, "*SAI_OBJECT_TYPE_FDB_ENTRY:*\"mac\":\"00:00:00:00:00:%02X\"*", mac);

    return buffer;
}

static void insert(
        uint64_t switchId,
        uint64_t bvId,
        uint64_t portId,
        unsigned char mac,
        bool isStatic)
{
    DBConnector db("TEST_DB", 0, true);

    ProducerTable producer(&db, tableName);
    ConsumerTable consumer(&db, tableName);

    char buffer[4000];

    sprintf(buffer, "SAI_OBJECT_TYPE_FDB_ENTRY:{\"bvid\":\"oid:0x%" PRIx64 "\",\"mac\":\"00:00:00:00:00:%02X\",\"switch_id\":\"oid:0x%" PRIx64 "\"}", bvId, mac, switchId);

    std::string key = buffer;

    std::vector<FieldValueTuple> values;

    values.emplace_back("SAI_FDB_ENTRY_ATTR_PACKET_ACTION", "SAI_PACKET_ACTION_FORWARD");
    values.emplace_back("SAI_FDB_ENTRY_ATTR_TYPE", (isStatic ? "SAI_FDB_ENTRY_TYPE_STATIC" : "SAI_FDB_ENTRY_TYPE_DYNAMIC"));
    values.emplace_back("SAI_FDB_ENTRY_ATTR_BRIDGE_PORT_ID", sOid(portId));

    producer.set(key, values, "set");

    std::string op;
    std::vector<FieldValueTuple> fvs;

    consumer.pop(key, op, fvs);
}

static size_t count()
{
    DBConnector db("TEST_DB", 0, true);

    auto keys = db.keys("ASIC_STATE:*");

    return keys.size();
}

static void mac(unsigned char m, bool is)
{
    DBConnector db("TEST_DB", 0, true);

    auto keys = db.keys(macKey(m));

    if (is)
    {
        EXPECT_EQ(keys.size(), 1u) << "mac 00:00:00:00:00:" << std::hex << (int)m << " is expected to survive";
        return;
    }

    EXPECT_EQ(keys.size(), 0u) << "mac 00:00:00:00:00:" << std::hex << (int)m << " is expected to be flushed";
}

// 8 entries, 4 static and 4 dynamic, spread over two switches, two bridge
// ports (portA, portB) and four bridge/port combinations.
static void populate()
{
    clear();

    // switch A, bvid A1, portA
    insert(0x21000000000000, 0x26000000000001, portA, 1, true);
    insert(0x21000000000000, 0x26000000000001, portA, 2, false);

    // switch A, bvid A2, portB
    insert(0x21000000000000, 0x26000000000002, portB, 3, true);
    insert(0x21000000000000, 0x26000000000002, portB, 4, false);

    // switch B, bvid B1, portA
    insert(0x121000000000001, 0x126000000000003, portA, 5, true);
    insert(0x121000000000001, 0x126000000000003, portA, 6, false);

    // switch B, bvid B2, portB
    insert(0x121000000000001, 0x126000000000004, portB, 7, true);
    insert(0x121000000000001, 0x126000000000004, portB, 8, false);
}

struct FlushBatch
{
    std::string cursor;
    size_t deleted = 0;
};

struct FlushStats
{
    size_t deleted = 0;
    size_t batches = 0;
    bool terminated = false;
};

// One EVALSHA invocation of the script.
static FlushBatch evalScan(
        DBConnector &db,
        const std::string &sha,
        const std::vector<std::string> &argv)
{
    std::vector<std::string> command = { "EVALSHA", sha, "0" };

    command.insert(command.end(), argv.begin(), argv.end());

    swss::RedisCommand redisCommand;
    redisCommand.format(command);

    swss::RedisReply r(&db, redisCommand, REDIS_REPLY_ARRAY);

    redisReply *reply = r.getContext();

    EXPECT_EQ(reply->elements, 2u);

    FlushBatch batch;
    batch.cursor = std::string(reply->element[0]->str, reply->element[0]->len);
    batch.deleted = (size_t)strtoul(reply->element[1]->str, NULL, 10);

    return batch;
}

static FlushBatch evalScan(
        DBConnector &db,
        const std::string &sha,
        const std::string &cursor,
        const std::string &countHint,
        const std::string &pattern,
        const std::string &port,
        const std::string &flushType)
{
    return evalScan(db, sha, { cursor, countHint, pattern, port, flushType });
}

// Drive the script the way syncd is supposed to: keep invoking it until the
// returned cursor is "0" again.
static FlushStats flush(
        DBConnector &db,
        const std::string &sha,
        const std::string &countHint,
        const std::string &pattern,
        const std::string &port,
        const std::string &flushType,
        size_t maxBatches = 100000)
{
    FlushStats stats;

    std::string cursor = "0";

    do
    {
        FlushBatch batch = evalScan(db, sha, cursor, countHint, pattern, port, flushType);

        stats.deleted += batch.deleted;
        stats.batches++;
        cursor = batch.cursor;
    }
    while (cursor != "0" && stats.batches < maxBatches);

    stats.terminated = (cursor == "0");

    return stats;
}

static std::string loadScanScript(DBConnector &db)
{
    auto script = swss::readTextFile("./common/fdb_flush_scan.lua");

    return swss::loadRedisScript(&db, script);
}

TEST(FdbFlushScan, flushDynamicOnly)
{
    DBConnector db("TEST_DB", 0, true);

    populate();

    auto sha = loadScanScript(db);

    auto stats = flush(db, sha, "100", fdbPattern, "", "0");

    EXPECT_TRUE(stats.terminated);
    EXPECT_EQ(stats.deleted, 4u);
    EXPECT_EQ(count(), 4u);

    // static entries are kept
    mac(1, true);
    mac(3, true);
    mac(5, true);
    mac(7, true);

    // dynamic entries are gone
    mac(2, false);
    mac(4, false);
    mac(6, false);
    mac(8, false);
}

TEST(FdbFlushScan, flushEveryEntry)
{
    DBConnector db("TEST_DB", 0, true);

    populate();

    auto sha = loadScanScript(db);

    auto stats = flush(db, sha, "100", fdbPattern, "", "1");

    EXPECT_TRUE(stats.terminated);
    EXPECT_EQ(stats.deleted, 8u);
    EXPECT_EQ(count(), 0u);
}

TEST(FdbFlushScan, flushDynamicByPort)
{
    DBConnector db("TEST_DB", 0, true);

    populate();

    auto sha = loadScanScript(db);

    auto stats = flush(db, sha, "100", fdbPattern, sOid(portA), "0");

    EXPECT_TRUE(stats.terminated);
    EXPECT_EQ(stats.deleted, 2u);
    EXPECT_EQ(count(), 6u);

    mac(2, false);
    mac(6, false);

    mac(1, true);
    mac(5, true);
}

TEST(FdbFlushScan, flushStaticByPort)
{
    DBConnector db("TEST_DB", 0, true);

    populate();

    auto sha = loadScanScript(db);

    auto stats = flush(db, sha, "100", fdbPattern, sOid(portB), "1");

    EXPECT_TRUE(stats.terminated);
    EXPECT_EQ(stats.deleted, 4u);
    EXPECT_EQ(count(), 4u);

    mac(3, false);
    mac(4, false);
    mac(7, false);
    mac(8, false);

    mac(1, true);
    mac(2, true);
}

TEST(FdbFlushScan, flushByBridgePortPattern)
{
    DBConnector db("TEST_DB", 0, true);

    populate();

    auto sha = loadScanScript(db);

    // restrict the MATCH pattern to one bridge port, like syncd does for a
    // per bridge-port flush
    std::string pattern = "*SAI_OBJECT_TYPE_FDB_ENTRY:*\"bvid\":\"oid:0x26000000000001\"*";

    auto stats = flush(db, sha, "100", pattern, "", "0");

    EXPECT_TRUE(stats.terminated);
    EXPECT_EQ(stats.deleted, 1u);
    EXPECT_EQ(count(), 7u);

    mac(2, false);
    mac(1, true);
}

TEST(FdbFlushScan, flushIsIdempotent)
{
    DBConnector db("TEST_DB", 0, true);

    populate();

    auto sha = loadScanScript(db);

    auto first = flush(db, sha, "100", fdbPattern, "", "0");
    EXPECT_TRUE(first.terminated);
    EXPECT_EQ(first.deleted, 4u);

    auto second = flush(db, sha, "100", fdbPattern, "", "0");
    EXPECT_TRUE(second.terminated);
    EXPECT_EQ(second.deleted, 0u);
    EXPECT_EQ(count(), 4u);
}

TEST(FdbFlushScan, flushInSmallBatches)
{
    DBConnector db("TEST_DB", 0, true);

    clear();

    // 100 dynamic entries on portA and 100 static entries on portB
    for (unsigned char i = 0; i < 100; i++)
    {
        insert(0x21000000000000, 0x26000000000001, portA, i, false);
        insert(0x21000000000000, 0x26000000000002, portB, (unsigned char)(100 + i), true);
    }

    EXPECT_EQ(count(), 200u);

    auto sha = loadScanScript(db);

    // a COUNT hint far below the number of keys forces several SCAN pages
    auto stats = flush(db, sha, "7", fdbPattern, "", "0");

    EXPECT_TRUE(stats.terminated);
    EXPECT_GT(stats.batches, 1u);
    EXPECT_EQ(stats.deleted, 100u);
    EXPECT_EQ(count(), 100u);

    mac(0, false);
    mac(99, false);
    mac(100, true);
    mac(199, true);
}

TEST(FdbFlushScan, flushWithOidWildcardPort)
{
    DBConnector db("TEST_DB", 0, true);

    populate();

    auto sha = loadScanScript(db);

    // "oid:0x0" means "any port", the same convention as fdb_flush.v2.lua
    auto stats = flush(db, sha, "100", fdbPattern, "oid:0x0", "0");

    EXPECT_TRUE(stats.terminated);
    EXPECT_EQ(stats.deleted, 4u);
    EXPECT_EQ(count(), 4u);
}

TEST(FdbFlushScan, defaultsForOptionalArguments)
{
    DBConnector db("TEST_DB", 0, true);

    populate();

    auto sha = loadScanScript(db);

    // only cursor, COUNT and MATCH are passed: port defaults to "" (any port)
    // and the flush type defaults to "0" (dynamic only)
    auto batch = evalScan(db, sha, { "0", "100", fdbPattern });

    EXPECT_EQ(batch.cursor, "0");
    EXPECT_EQ(batch.deleted, 4u);
    EXPECT_EQ(count(), 4u);
}

TEST(FdbFlushScan, rejectsInvalidArguments)
{
    DBConnector db("TEST_DB", 0, true);

    populate();

    auto sha = loadScanScript(db);

    // a cursor which is not a decimal number
    try
    {
        evalScan(db, sha, "not-a-cursor", "100", fdbPattern, "", "0");
        FAIL() << "invalid cursor was accepted";
    }
    catch (const std::system_error &e)
    {
        EXPECT_NE(std::string(e.what()).find("fdb_flush_scan"), std::string::npos);
    }

    // a COUNT hint below 1
    try
    {
        evalScan(db, sha, "0", "0", fdbPattern, "", "0");
        FAIL() << "invalid count was accepted";
    }
    catch (const std::system_error &e)
    {
        EXPECT_NE(std::string(e.what()).find("fdb_flush_scan"), std::string::npos);
    }

    // an unknown flush type
    try
    {
        evalScan(db, sha, "0", "100", fdbPattern, "", "2");
        FAIL() << "invalid flush type was accepted";
    }
    catch (const std::system_error &e)
    {
        EXPECT_NE(std::string(e.what()).find("fdb_flush_scan"), std::string::npos);
    }

    // an empty MATCH pattern
    try
    {
        evalScan(db, sha, "0", "100", "", "", "0");
        FAIL() << "empty pattern was accepted";
    }
    catch (const std::system_error &e)
    {
        EXPECT_NE(std::string(e.what()).find("fdb_flush_scan"), std::string::npos);
    }

    // nothing was deleted by the rejected calls
    EXPECT_EQ(count(), 8u);
}
