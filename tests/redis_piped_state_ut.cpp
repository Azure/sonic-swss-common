#include <iostream>
#include <memory>
#include <thread>
#include <algorithm>
#include <memory>
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
    return string("key") + to_string(i);
}

static inline string field(int i)
{
    return string("field") + to_string(i);
}

static inline string value(int i)
{
    if (i == 0) return string(); // empty
    return string("value") + to_string(i);
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
    RedisPipeline pipeline(&db);
    ProducerStateTable p(&pipeline, tableName, true);

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
        } else if (kfvOp(kco) == "DEL")
        {
            numberOfKeyDeleted++;
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

TEST(ConsumerStateTable, async_double_set)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    RedisPipeline pipeline(&db);
    ProducerStateTable p(&pipeline, tableName, true);
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
    p.flush();

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
}

TEST(ConsumerStateTable, async_set_del)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    RedisPipeline pipeline(&db);
    ProducerStateTable p(&pipeline, tableName, true);
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
    p.flush();

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
}

TEST(ConsumerStateTable, async_set_del_set)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    RedisPipeline pipeline(&db);
    ProducerStateTable p(&pipeline, tableName, true);
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
    p.flush();

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

        c.pop(kco);
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), "SET");

        fvs = kfvFieldsValues(kco);
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
}

TEST(ConsumerStateTable, async_singlethread)
{
    clearDB();

    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    RedisPipeline pipeline(&db);
    ProducerStateTable p(&pipeline, tableName, true);

    // Do pending data clear test first.
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
    p.clear();
    EXPECT_EQ(p.count(), 0);
    string queryCommand = "KEYS " + p.getStateHashPrefix() + tableName + p.getTableNameSeparator() + "*";
    RedisReply r(&db, queryCommand.c_str(), REDIS_REPLY_ARRAY);
    EXPECT_EQ(r.getContext()->elements, (size_t)0);

    ConsumerStateTable c(&db, tableName);
    Select cs;
    Selectable *selectcs;
    int ret, i = 0;
    KeyOpFieldsValuesTuple kco;

    cs.addSelectable(&c);
    ret = cs.select(&selectcs, 1000);
    EXPECT_EQ(ret, Select::TIMEOUT);

    for (i = 0; i < NUMBER_OF_OPS; i++)
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
    p.flush();
    // KeySet of the ProducerStateTable has data to be picked up by ConsumerStateTable
    EXPECT_EQ(p.count(), NUMBER_OF_OPS);

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
    // KeySet of the ProducerStateTable has been emptied by ConsumerStateTable
    EXPECT_EQ(p.count(), 0);

    for (i = 0; i < NUMBER_OF_OPS; i++)
    {
        p.del(key(i));
        if ((i % 100) == 0)
            cout << "+" << flush;
    }

    EXPECT_EQ(p.count(), NUMBER_OF_OPS);
    p.flush();

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
    EXPECT_EQ(p.count(), 0);

    EXPECT_LE(numberOfKeysSet, numberOfKeyDeleted);
    EXPECT_EQ(ret, Select::OBJECT);

    // clear test with consumer already listening
    for (i = 0; i < NUMBER_OF_OPS; i++)
    {
        p.del(key(i));
        if ((i % 100) == 0)
            cout << "+" << flush;
    }

    EXPECT_EQ(p.count(), NUMBER_OF_OPS);
    p.flush();
    p.clear();
    EXPECT_EQ(p.count(), 0);
    RedisReply r2(&db, queryCommand.c_str(), REDIS_REPLY_ARRAY);
    EXPECT_EQ(r2.getContext()->elements, (size_t)0);

    int numberOfNotification = 0;
    while ((ret = cs.select(&selectcs, 1000)) == Select::OBJECT)
    {
        c.pop(kco);
        // keys have been dropped, expecting empty kco.
        EXPECT_EQ(kfvOp(kco), "");
        numberOfNotification++;

        if ((i++ % 100) == 0)
            cout << "-" << flush;
    }
    EXPECT_EQ(p.count(), 0);

    // ConsumerStateTable got all the notifications though no real data available.
    EXPECT_EQ(NUMBER_OF_OPS, numberOfNotification);
    EXPECT_EQ(ret, Select::TIMEOUT);

    cout << "Done. Waiting for all job to finish " << NUMBER_OF_OPS << " jobs." << endl;

    cout << endl << "Done." << endl;
}

TEST(ConsumerStateTable, async_batched)
{
    clearDB();

    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    RedisPipeline pipeline(&db);
    ProducerStateTable p(&pipeline, tableName, true);
    vector<KeyOpFieldsValuesTuple> set_requests;

    // Do pending data clear test first.
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
    p.clear();
    EXPECT_EQ(p.count(), 0);
    string queryCommand = "KEYS " + p.getStateHashPrefix() + tableName + p.getTableNameSeparator() + "*";
    RedisReply r(&db, queryCommand.c_str(), REDIS_REPLY_ARRAY);
    EXPECT_EQ(r.getContext()->elements, (size_t)0);

    // Set the batch size to be NUMBER_OF_OPS, so that we can pop all requests
    // in one call.
    ConsumerStateTable c(&db, tableName, NUMBER_OF_OPS);
    Select cs;
    Selectable *selectcs;
    int ret, i = 0;
    deque<KeyOpFieldsValuesTuple> vkco;

    cs.addSelectable(&c);
    ret = cs.select(&selectcs, 1000);
    EXPECT_EQ(ret, Select::TIMEOUT);

    set_requests.clear();
    for (i = 0; i < NUMBER_OF_OPS; i++)
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
    p.flush();
    // KeySet of the ProducerStateTable has data to be picked up by ConsumerStateTable
    EXPECT_EQ(p.count(), NUMBER_OF_OPS);

    ret = cs.select(&selectcs, 1000);
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
    // KeySet of the ProducerStateTable has been emptied by ConsumerStateTable
    EXPECT_EQ(p.count(), 0);

    vector<string> del_requests;
    for (i = 0; i < NUMBER_OF_OPS; i++)
    {
        del_requests.push_back(key(i));
        if ((i % 100) == 0)
            cout << "+" << flush;
    }
    p.del(del_requests);

    EXPECT_EQ(p.count(), NUMBER_OF_OPS);
    p.flush();

    ret = cs.select(&selectcs, 1000);
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
    EXPECT_EQ(p.count(), 0);

    EXPECT_LE(numberOfKeysSet, numberOfKeyDeleted);
    EXPECT_EQ(ret, Select::OBJECT);

    // clear test with consumer already listening
    del_requests.clear();
    for (i = 0; i < NUMBER_OF_OPS; i++)
    {
        del_requests.push_back(key(i));
        if ((i % 100) == 0)
            cout << "+" << flush;
    }
    p.del(del_requests);

    EXPECT_EQ(p.count(), NUMBER_OF_OPS);
    p.flush();
    p.clear();
    EXPECT_EQ(p.count(), 0);
    RedisReply r2(&db, queryCommand.c_str(), REDIS_REPLY_ARRAY);
    EXPECT_EQ(r2.getContext()->elements, (size_t)0);

    int numberOfNotification = 0;
    while ((ret = cs.select(&selectcs, 1000)) == Select::OBJECT)
    {
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        // keys have been dropped, expecting empty kco.
        EXPECT_EQ(kfvOp(kco), "");
        numberOfNotification++;

        if ((i++ % 100) == 0)
            cout << "-" << flush;
    }
    EXPECT_EQ(p.count(), 0);

    // There is only 1 notification in batched request.
    EXPECT_EQ(1, numberOfNotification);
    EXPECT_EQ(ret, Select::TIMEOUT);

    cout << "Done. Waiting for all job to finish " << NUMBER_OF_OPS << " jobs." << endl;

    cout << endl << "Done." << endl;
}

TEST(ConsumerStateTable, async_test)
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

TEST(ConsumerStateTable, async_multitable)
{
    DBConnector db(TEST_DB, 0, true);
    ConsumerStateTable *consumers[NUMBER_OF_THREADS];
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
        consumers[i] = new ConsumerStateTable(&db, string("UT_REDIS_THREAD_") +
                                         to_string(i));
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

    cout << endl << "Done." << endl;
}

TEST(ConsumerStateTable, flushPub)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    string tableName = "UT_REDIS_THREAD_" + to_string(index);
    DBConnector db(TEST_DB, 0, true);
    RedisPipeline pipeline(&db);
    ProducerStateTable p(&pipeline, tableName, false, true);
    p.setBuffered(true);

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
    p.flush();

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
}