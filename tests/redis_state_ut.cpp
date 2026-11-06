#include <iostream>
#include <memory>
#include <thread>
#include <algorithm>
#include <deque>
#include "gtest/gtest.h"
#include "common/dbconnector.h"
#include "common/notificationconsumer.h"
#include "common/notificationproducer.h"
#include "common/select.h"
#include "common/selectableevent.h"
#include "common/table.h"
#include "common/producerstatetable.h"
#include "common/consumerstatetable.h"

using namespace std;
using namespace swss;

#define TEST_DB           "APPL_DB" // Need to test against a DB which uses a colon table name separator due to hardcoding in consumer_table_pops.lua
#define NUMBER_OF_THREADS    (64) // Spawning more than 256 threads causes libc++ to except
#define NUMBER_OF_OPS      (1000)
#define MAX_FIELDS_DIV       (30) // Testing up to 30 fields objects
#define PRINT_SKIP           (10) // Print + for Producer and - for Consumer for every 100 ops

static inline int getMaxFields(int i)
{
    return (i/MAX_FIELDS_DIV) + 1;
}

static inline string key(int i)
{
    return string("key ") + to_string(i);
}

static inline string field(int i)
{
    return string("field ") + to_string(i);
}

static inline string value(int i)
{
    if (i == 0) return string(); // empty
    return string("value ") + to_string(i);
}

static inline bool IsDigit(char ch)
{
    return (ch >= '0') && (ch <= '9');
}

static inline int readNumberAtEOL(const string& str)
{
    if (str.empty()) return 0;
    auto pos = find_if(str.begin(), str.end(), IsDigit);
    istringstream is(str.substr(pos - str.begin()));
    int ret;
    is >> ret;
    EXPECT_TRUE((bool)is);
    return ret;
}

static inline void validateFields(const string& key, const vector<FieldValueTuple>& f)
{
    unsigned int maxNumOfFields = getMaxFields(readNumberAtEOL(key));
    int i = 0;
    EXPECT_EQ(maxNumOfFields, f.size());

    for (auto fv : f)
    {
        EXPECT_EQ(i, readNumberAtEOL(fvField(fv)));
        EXPECT_EQ(i, readNumberAtEOL(fvValue(fv)));
        i++;
    }
}

static void producerWorker(int index)
{
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);

    for (int i = 0; i < NUMBER_OF_OPS; i++)
    {
        vector<FieldValueTuple> fields;
        int maxNumOfFields = getMaxFields(i);
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        if ((i % 100) == 0)
            cout << "+" << flush;

        p.set(key(i), fields);
    }

    for (int i = 0; i < NUMBER_OF_OPS; i++)
    {
        p.del(key(i));
    }
}

static void consumerWorker(int index)
{
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ConsumerStateTable c(&db, tableName);
    Select cs;
    Selectable *selectcs;
    int numberOfKeysSet = 0;
    int numberOfKeyDeleted = 0;
    int ret, i = 0;
    KeyOpFieldsValuesTuple kco;

    cs.addSelectable(&c);
    while ((ret = cs.select(&selectcs)) == Select::OBJECT)
    {
        c.pop(kco);
        if (kfvOp(kco) == "SET")
        {
            numberOfKeysSet++;
            validateFields(kfvKey(kco), kfvFieldsValues(kco));

            for (auto fv : kfvFieldsValues(kco))
            {
                string val = *db.hget(tableName + ":" + kfvKey(kco), fvField(fv));
                EXPECT_EQ(val, fvValue(fv));
            }
        } else if (kfvOp(kco) == "DEL")
        {
            numberOfKeyDeleted++;
            auto keys = db.keys(tableName + ":" + kfvKey(kco));
            EXPECT_EQ(0UL, keys.size());
        }

        if ((i++ % 100) == 0)
            cout << "-" << flush;

        if (numberOfKeyDeleted == NUMBER_OF_OPS)
            break;
    }

    EXPECT_LE(numberOfKeysSet, numberOfKeyDeleted);
    EXPECT_EQ(ret, Select::OBJECT);
}

static inline void clearDB()
{
    DBConnector db(TEST_DB, 0, true);
    RedisReply r(&db, "FLUSHALL", REDIS_REPLY_STATUS);
    r.checkStatusOK();
}

TEST(ConsumerStateTable, double_set)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    EXPECT_EQ(db.getDbName(), TEST_DB);
    EXPECT_EQ(SonicDBConfig::getSeparator(db.getDbName()), ":");
    ProducerStateTable p(&db, tableName);
    string key = "TheKey";
    int maxNumOfFields = 2;

    /* First set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Second set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields * 2; j += 2)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Prepare consumer */
    ConsumerStateTable c(&db, tableName);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);

    /* First pop operation */
    {
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), "SET");

        auto fvs = kfvFieldsValues(kco);
        EXPECT_EQ(fvs.size(), (unsigned int)(maxNumOfFields + maxNumOfFields/2));

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields; j++)
        {
            EXPECT_EQ(mm[field(j)], value(j));
        }
        for (int j = 0; j < maxNumOfFields * 2; j += 2)
        {
            EXPECT_EQ(mm[field(j)], value(j));
        }
    }

    /* Second select operation */
    {
        int ret = cs.select(&selectcs, 1000);
        EXPECT_EQ(ret, Select::TIMEOUT);
    }

    /* State Queue should be empty */
    RedisCommand keys;
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r(&db, keys, REDIS_REPLY_ARRAY);
    auto qlen = r.getContext()->elements;
    EXPECT_EQ(qlen, 0U);
}

TEST(ConsumerStateTable, set_del)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);
    string key = "TheKey";
    int maxNumOfFields = 2;

    /* Set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Del operation */
    p.del(key);

    /* Prepare consumer */
    ConsumerStateTable c(&db, tableName);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);

    /* First pop operation */
    {
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), "DEL");

        auto fvs = kfvFieldsValues(kco);
        EXPECT_EQ(fvs.size(), 0U);
    }

    /* Second select operation */
    {
        int ret = cs.select(&selectcs, 1000);
        EXPECT_EQ(ret, Select::TIMEOUT);
    }

    /* State Queue should be empty */
    RedisCommand keys;
    keys.format("KEYS %s*", (p.getStateHashPrefix() + tableName).c_str());
    RedisReply r(&db, keys, REDIS_REPLY_ARRAY);
    auto qlen = r.getContext()->elements;
    EXPECT_EQ(qlen, 0U);
}

TEST(ConsumerStateTable, set_del_set)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);
    string key = "TheKey";
    int maxNumOfFields = 2;

    /* First set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Del operation */
    p.del(key);

    /* Second set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields * 2; j += 2)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Prepare consumer */
    ConsumerStateTable c(&db, tableName);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);

    /* First pop operation */
    {
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        std::deque<KeyOpFieldsValuesTuple> vkco;
        c.pops(vkco);
        ASSERT_EQ(vkco.size(), 2);
        EXPECT_EQ(kfvKey(vkco[0]), key);
        EXPECT_EQ(kfvOp(vkco[0]), "DEL");
        auto fvs = kfvFieldsValues(vkco[0]);
        EXPECT_EQ(fvs.size(), 0);
        EXPECT_EQ(kfvKey(vkco[1]), key);
        EXPECT_EQ(kfvOp(vkco[1]), "SET");
        fvs = kfvFieldsValues(vkco[1]);
        EXPECT_EQ(fvs.size(), (unsigned int)maxNumOfFields);

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields * 2; j += 2)
        {
            EXPECT_EQ(mm[field(j)], value(j));
        }
    }

    /* Second select operation */
    {
        int ret = cs.select(&selectcs, 1000);
        EXPECT_EQ(ret, Select::TIMEOUT);
    }

    /* State Queue should be empty */
    RedisCommand keys;
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r(&db, keys, REDIS_REPLY_ARRAY);
    auto qlen = r.getContext()->elements;
    EXPECT_EQ(qlen, 0U);
}

TEST(ConsumerStateTable, set_pop_del_set_pop_get)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);
    string key = "TheKey";
    int maxNumOfFields = 2;

    /* First set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Prepare consumer */
    ConsumerStateTable c(&db, tableName);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);

    /* First pop operation */
    {
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), "SET");

        auto fvs = kfvFieldsValues(kco);
        EXPECT_EQ(fvs.size(), (unsigned int)maxNumOfFields);

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields; j ++)
        {
            EXPECT_EQ(mm[field(j)], value(j));
        }
    }

    /* Del operation */
    p.del(key);

    /* Second set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields * 2; j += 2)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Second pop operation */
    {
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        std::deque<KeyOpFieldsValuesTuple> vkco;
        c.pops(vkco);
        ASSERT_EQ(vkco.size(), 2);
        EXPECT_EQ(kfvKey(vkco[0]), key);
        EXPECT_EQ(kfvOp(vkco[0]), "DEL");
        auto fvs = kfvFieldsValues(vkco[0]);
        EXPECT_EQ(fvs.size(), 0);
        EXPECT_EQ(kfvKey(vkco[1]), key);
        EXPECT_EQ(kfvOp(vkco[1]), "SET");
        fvs = kfvFieldsValues(vkco[1]);
        EXPECT_EQ(fvs.size(), (unsigned int)maxNumOfFields);

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields * 2; j += 2)
        {
            EXPECT_EQ(mm[field(j)], value(j));
        }
    }

    /* Get data directly from table in redis DB*/
    Table t(&db, tableName);
    vector<FieldValueTuple> values;
    t.get(key, values);
    /* size of values should be maxNumOfFields, no "field 1" left from first set */
    EXPECT_EQ(values.size(), (unsigned int)maxNumOfFields);

    /*
     * Third pop operation, consumer received two consecutive signals.
     * data depleted upon first one
     */
    /*
    {
        int ret = cs.select(&selectcs, 1000);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_EQ(kfvKey(kco), "");
    }
    */

    /*
     * With the optimization in ProducerStateTable set/del operation,
     * if there is pending operations on the same key, producer won't publish
     * redundant notification again.
     * So the check above was commented out.
     */

    /* Third select operation */
    {
        int ret = cs.select(&selectcs, 1000);
        EXPECT_EQ(ret, Select::TIMEOUT);
    }

    /* State Queue should be empty */
    RedisCommand keys;
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r(&db, keys, REDIS_REPLY_ARRAY);
    auto qlen = r.getContext()->elements;
    EXPECT_EQ(qlen, 0U);
}

TEST(ConsumerStateTable, view_switch)
{
    clearDB();

    // Prepare producer
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);
    Table table(&db, tableName);

    int numOfKeys = 20;
    int numOfKeysToDel = 10;
    int maxNumOfFields = 2;

    p.create_temp_view();
    for (int i=0; i<numOfKeys; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }
    // Set operations should go into temp view
    EXPECT_EQ(p.count(), 0);
    for (int i=0; i<numOfKeysToDel; ++i)
    {
        p.del(key(i));
    }
    EXPECT_EQ(p.count(), 0);
    p.apply_temp_view();
    // After apply_temp_view(), minimal amount of SET operation should be created
    EXPECT_EQ(p.count(), numOfKeys - numOfKeysToDel);

    // Test temp view with large amount of objects
    clearDB();
    numOfKeys = 20000;
    p.create_temp_view();
    for (int i=0; i<numOfKeys; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }
    p.apply_temp_view();
    EXPECT_EQ(p.count(), numOfKeys);

    // Test object deletion
    clearDB();
    numOfKeys = 20;
    for (int i=0; i<numOfKeys; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        table.set(key(i), fields);
    }
    p.create_temp_view();
    p.apply_temp_view();
    EXPECT_EQ(p.count(), numOfKeys);
    RedisCommand cmdDelCount;
    cmdDelCount.format("SCARD %s", p.getDelKeySetName().c_str());
    RedisReply r(&db, cmdDelCount, REDIS_REPLY_INTEGER);
    EXPECT_EQ(r.getReply<long long int>(), (long long int) numOfKeys);

    // When there is less field. objects need to be deleted and recreated
    clearDB();
    int maxNumOfFieldsNew = 1;
    p.create_temp_view();
    for (int i=0; i<numOfKeys; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        table.set(key(i), fields);
        fields.clear();
        for (int j = 0; j < maxNumOfFieldsNew; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }
    p.apply_temp_view();
    EXPECT_EQ(p.count(), numOfKeys);
    RedisReply r2(&db, cmdDelCount, REDIS_REPLY_INTEGER);
    EXPECT_EQ(r2.getReply<long long int>(), (long long int) numOfKeys);

    // When there is more field. objects does not need to be deleted
    clearDB();
    maxNumOfFieldsNew = 3;
    p.create_temp_view();
    for (int i=0; i<numOfKeys; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        table.set(key(i), fields);
        fields.clear();
        for (int j = 0; j < maxNumOfFieldsNew; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }
    p.apply_temp_view();
    EXPECT_EQ(p.count(), numOfKeys);
    RedisReply r3(&db, cmdDelCount, REDIS_REPLY_INTEGER);
    EXPECT_EQ(r3.getReply<long long int>(), (long long int) 0);
}

TEST(ConsumerStateTable, view_switch_abnormal_sequence)
{
    clearDB();

    // Prepare producer
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);
    Table table(&db, tableName);

    int numOfKeys = 20;
    int numOfKeysNew = 10;
    int maxNumOfFields = 2;

    // Double create - only the content of second view should be applied
    p.create_temp_view();
    for (int i=0; i<numOfKeys; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }
    p.create_temp_view();
    for (int i=0; i<numOfKeysNew; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }
    p.apply_temp_view();
    EXPECT_EQ(p.count(), numOfKeysNew);

    // Double apply - should throw exception
    clearDB();
    p.create_temp_view();
    p.apply_temp_view();
    EXPECT_ANY_THROW({
        p.apply_temp_view();
    });
}

TEST(ConsumerStateTable, view_switch_with_consumer)
{
    clearDB();
    int numOfKeys = 20;
    int numOfKeysNew = 5;
    int maxNumOfFields = 2;
    int maxNumOfFieldsNew = 1;

    // Prepare producer
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);
    Table table(&db, tableName);

    // Set up previous view
    for (int i=0; i<numOfKeys; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }

    // Set up consumer to sync state
    ConsumerStateTable c(&db, tableName);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);
    int ret;
    do
    {
        ret = cs.select(&selectcs, 1000);
        if (ret == Select::OBJECT)
        {
            KeyOpFieldsValuesTuple kco;
            c.pop(kco);
        }
    }
    while (ret != Select::TIMEOUT);

    // State Queue should be empty
    RedisCommand keys;
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r.getContext()->elements, (size_t) 0);
    // Verify number of objects
    keys.format("KEYS %s:*", tableName.c_str());
    RedisReply r2(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r2.getContext()->elements, (size_t) numOfKeys);
    // Verify number of fields
    RedisCommand hlen;
    hlen.format("HLEN %s", (c.getKeyName(key(0))).c_str());
    RedisReply r3(&db, hlen, REDIS_REPLY_INTEGER);
    EXPECT_EQ(r3.getReply<long long int>(), (long long int) maxNumOfFields);

    // Apply temp view with different number of objects and different number of fields
    p.create_temp_view();
    for (int i=0; i<numOfKeysNew; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFieldsNew; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }
    p.apply_temp_view();

    do
    {
        ret = cs.select(&selectcs, 1000);
        if (ret == Select::OBJECT)
        {
            KeyOpFieldsValuesTuple kco;
            c.pop(kco);
        }
    }
    while (ret != Select::TIMEOUT);

    // State Queue should be empty
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r4(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r4.getContext()->elements, (size_t) 0);
    // Verify number of objects
    keys.format("KEYS %s:*", tableName.c_str());
    RedisReply r5(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r5.getContext()->elements, (size_t) numOfKeysNew);
    // Verify number of fields
    hlen.format("HLEN %s", (c.getKeyName(key(0))).c_str());
    RedisReply r6(&db, hlen, REDIS_REPLY_INTEGER);
    EXPECT_EQ(r6.getReply<long long int>(), (long long int) maxNumOfFieldsNew);
}

TEST(ConsumerStateTable, view_switch_delete_with_consumer)
{
    clearDB();
    int numOfKeys = 20;
    int maxNumOfFields = 2;

    // Prepare producer
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);
    Table table(&db, tableName);

    // Set up previous view
    for (int i=0; i<numOfKeys; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }

    // Set up consumer to sync state
    ConsumerStateTable c(&db, tableName);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);
    int ret;
    do
    {
        ret = cs.select(&selectcs, 1000);
        if (ret == Select::OBJECT)
        {
            KeyOpFieldsValuesTuple kco;
            c.pop(kco);
        }
    }
    while (ret != Select::TIMEOUT);

    // State Queue should be empty
    RedisCommand keys;
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r.getContext()->elements, (size_t) 0);
    // Verify number of objects
    keys.format("KEYS %s:*", tableName.c_str());
    RedisReply r2(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r2.getContext()->elements, (size_t) numOfKeys);
    // Verify number of fields
    RedisCommand hlen;
    hlen.format("HLEN %s", (c.getKeyName(key(0))).c_str());
    RedisReply r3(&db, hlen, REDIS_REPLY_INTEGER);
    EXPECT_EQ(r3.getReply<long long int>(), (long long int) maxNumOfFields);

    // Apply empty temp view
    p.create_temp_view();
    p.apply_temp_view();

    do
    {
        ret = cs.select(&selectcs, 1000);
        if (ret == Select::OBJECT)
        {
            KeyOpFieldsValuesTuple kco;
            c.pop(kco);
        }
    }
    while (ret != Select::TIMEOUT);

    // State Queue should be empty
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r4(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r4.getContext()->elements, (size_t) 0);
    // Table should be empty
    keys.format("KEYS %s:*", tableName.c_str());
    RedisReply r5(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r5.getContext()->elements, (size_t) 0);
}

TEST(ConsumerStateTable, view_switch_delete_with_consumer_2)
{
    clearDB();
    int numOfKeys = 20;
    int maxNumOfFields = 2;

    // Prepare producer
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);
    Table table(&db, tableName);

    // Set up consumer to sync state
    ConsumerStateTable c(&db, tableName);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);
    int ret;

    // Set up initial view again
    for (int i=0; i<numOfKeys; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }
    do
    {
        ret = cs.select(&selectcs, 1000);
        if (ret == Select::OBJECT)
        {
            KeyOpFieldsValuesTuple kco;
            c.pop(kco);
        }
    }
    while (ret != Select::TIMEOUT);
    // State Queue should be empty
    RedisCommand keys;
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r6(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r6.getContext()->elements, (size_t) 0);

    p.create_temp_view();
    // set and del in temp view
    for (int i=0; i<numOfKeys; ++i)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; ++j)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        p.set(key(i), fields);
    }
    for (int i=0; i<numOfKeys; ++i)
    {
        p.del(key(i));
    }
    p.apply_temp_view();

    do
    {
        ret = cs.select(&selectcs, 1000);
        if (ret == Select::OBJECT)
        {
            KeyOpFieldsValuesTuple kco;
            c.pop(kco);
        }
    }
    while (ret != Select::TIMEOUT);

    // State Queue should be empty
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r7(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r7.getContext()->elements, (size_t) 0);
    // Table should be empty
    keys.format("KEYS %s:*", tableName.c_str());
    RedisReply r8(&db, keys, REDIS_REPLY_ARRAY);
    EXPECT_EQ(r8.getContext()->elements, (size_t) 0);
}

TEST(ConsumerStateTable, singlethread)
{
    clearDB();

    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);

    for (int i = 0; i < NUMBER_OF_OPS; i++)
    {
        vector<FieldValueTuple> fields;
        int maxNumOfFields = getMaxFields(i);
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        if ((i % 100) == 0)
            cout << "+" << flush;

        p.set(key(i), fields);
    }

    ConsumerStateTable c(&db, tableName);
    Select cs;
    Selectable *selectcs;
    int ret, i = 0;
    KeyOpFieldsValuesTuple kco;

    cs.addSelectable(&c);
    int numberOfKeysSet = 0;
    while ((ret = cs.select(&selectcs)) == Select::OBJECT)
    {
        c.pop(kco);
        EXPECT_EQ(kfvOp(kco), "SET");
        numberOfKeysSet++;
        validateFields(kfvKey(kco), kfvFieldsValues(kco));

        if ((i++ % 100) == 0)
            cout << "-" << flush;

        if (numberOfKeysSet == NUMBER_OF_OPS)
            break;
    }

    for (i = 0; i < NUMBER_OF_OPS; i++)
    {
        p.del(key(i));
        if ((i % 100) == 0)
            cout << "+" << flush;
    }

    int numberOfKeyDeleted = 0;
    while ((ret = cs.select(&selectcs)) == Select::OBJECT)
    {
        c.pop(kco);
        EXPECT_EQ(kfvOp(kco), "DEL");
        numberOfKeyDeleted++;

        if ((i++ % 100) == 0)
            cout << "-" << flush;

        if (numberOfKeyDeleted == NUMBER_OF_OPS)
            break;
    }

    EXPECT_LE(numberOfKeysSet, numberOfKeyDeleted);
    EXPECT_EQ(ret, Select::OBJECT);

    cout << "Done. Waiting for all job to finish " << NUMBER_OF_OPS << " jobs." << endl;

    /* State Queue should be empty */
    RedisCommand keys;
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r(&db, keys, REDIS_REPLY_ARRAY);
    auto qlen = r.getContext()->elements;
    EXPECT_EQ(qlen, 0U);

    cout << endl << "Done." << endl;
}

TEST(ConsumerStateTable, batched)
{
    clearDB();

    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    ProducerStateTable p(&db, tableName);
    vector<KeyOpFieldsValuesTuple> set_requests;

    for (int i = 0; i < NUMBER_OF_OPS; i++)
    {
        vector<FieldValueTuple> fields;
        int maxNumOfFields = getMaxFields(i);
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(j), value(j));
            fields.push_back(t);
        }
        if ((i % 100) == 0)
            cout << "+" << flush;
        set_requests.push_back(KeyOpFieldsValuesTuple({key(i), SET_COMMAND, fields}));
    }
    p.set(set_requests);

    // Set the batch size to be NUMBER_OF_OPS, so that we can pop all requests
    // in one call.
    ConsumerStateTable c(&db, tableName, NUMBER_OF_OPS);
    Select cs;
    Selectable *selectcs;
    int ret, i = 0;
    deque<KeyOpFieldsValuesTuple> vkco;

    cs.addSelectable(&c);
    ret = cs.select(&selectcs);
    c.pops(vkco);
    int numberOfKeysSet = 0;
    for (auto kco : vkco)
    {
        EXPECT_EQ(kfvOp(kco), "SET");
        numberOfKeysSet++;
        validateFields(kfvKey(kco), kfvFieldsValues(kco));

        if ((i++ % 100) == 0)
            cout << "-" << flush;
    }

    EXPECT_LE(numberOfKeysSet, NUMBER_OF_OPS);
    EXPECT_EQ(ret, Select::OBJECT);

    vector<string> del_requests;
    for (i = 0; i < NUMBER_OF_OPS; i++)
    {
        del_requests.push_back(key(i));
        if ((i % 100) == 0)
            cout << "+" << flush;
    }
    p.del(del_requests);

    ret = cs.select(&selectcs);
    vkco.clear();
    c.pops(vkco);
    int numberOfKeyDeleted = 0;
    for (auto kco : vkco)
    {
        EXPECT_EQ(kfvOp(kco), "DEL");
        numberOfKeyDeleted++;

        if ((i++ % 100) == 0)
            cout << "-" << flush;
    }

    EXPECT_LE(numberOfKeysSet, numberOfKeyDeleted);
    EXPECT_EQ(ret, Select::OBJECT);

    cout << "Done. Waiting for all job to finish " << NUMBER_OF_OPS << " jobs." << endl;

    /* State Queue should be empty */
    RedisCommand keys;
    keys.format("KEYS %s*", (c.getStateHashPrefix() + tableName).c_str());
    RedisReply r(&db, keys, REDIS_REPLY_ARRAY);
    auto qlen = r.getContext()->elements;
    EXPECT_EQ(qlen, 0U);

    cout << endl << "Done." << endl;
}

TEST(ConsumerStateTable, test)
{
    thread *producerThreads[NUMBER_OF_THREADS];
    thread *consumerThreads[NUMBER_OF_THREADS];

    clearDB();

    cout << "Starting " << NUMBER_OF_THREADS*2 << " producers and consumers on redis" << endl;
    /* Starting the consumer before the producer */
    for (int i = 0; i < NUMBER_OF_THREADS; i++)
    {
        consumerThreads[i] = new thread(consumerWorker, i);
        producerThreads[i] = new thread(producerWorker, i);
    }

    cout << "Done. Waiting for all job to finish " << NUMBER_OF_OPS << " jobs." << endl;

    for (int i = 0; i < NUMBER_OF_THREADS; i++)
    {
        producerThreads[i]->join();
        delete producerThreads[i];
        consumerThreads[i]->join();
        delete consumerThreads[i];
    }

    cout << endl << "Done." << endl;
}

TEST(ConsumerStateTable, multitable)
{
    DBConnector db(TEST_DB, 0, true);
    ConsumerStateTable *consumers[NUMBER_OF_THREADS];
    vector<string> tablenames(NUMBER_OF_THREADS);
    thread *producerThreads[NUMBER_OF_THREADS];
    KeyOpFieldsValuesTuple kco;
    Select cs;
    int numberOfKeysSet = 0;
    int numberOfKeyDeleted = 0;
    int ret = 0, i;

    clearDB();

    cout << "Starting " << NUMBER_OF_THREADS*2 << " producers and consumers on redis, using single thread for consumers and thread per producer" << endl;

    /* Starting the consumer before the producer */
    for (i = 0; i < NUMBER_OF_THREADS; i++)
    {
        tablenames[i] = string("UT_REDIS_THREAD_") + to_string(i);
        consumers[i] = new ConsumerStateTable(&db, tablenames[i]);
        producerThreads[i] = new thread(producerWorker, i);
    }

    for (i = 0; i < NUMBER_OF_THREADS; i++)
        cs.addSelectable(consumers[i]);

    while (1)
    {
        Selectable *is;

        ret = cs.select(&is);
        EXPECT_EQ(ret, Select::OBJECT);

        ((ConsumerStateTable *)is)->pop(kco);
        if (kfvOp(kco) == "SET")
        {
            numberOfKeysSet++;
            validateFields(kfvKey(kco), kfvFieldsValues(kco));
        } else if (kfvOp(kco) == "DEL")
        {
            numberOfKeyDeleted++;
            if ((numberOfKeyDeleted % 100) == 0)
                cout << "-" << flush;
        }

        if (numberOfKeyDeleted == NUMBER_OF_OPS * NUMBER_OF_THREADS)
            break;
    }

    EXPECT_LE(numberOfKeysSet, numberOfKeyDeleted);

    /* Making sure threads stops execution */
    for (i = 0; i < NUMBER_OF_THREADS; i++)
    {
        producerThreads[i]->join();
        delete consumers[i];
        delete producerThreads[i];
    }


    /* State Queue should be empty */
    RedisCommand keys;
    keys.format("KEYS %s*", (consumers[0]->getStateHashPrefix() + tablenames[0]).c_str());
    RedisReply r(&db, keys, REDIS_REPLY_ARRAY);
    auto qlen = r.getContext()->elements;
    EXPECT_EQ(qlen, 0U);

    cout << endl << "Done." << endl;
}
