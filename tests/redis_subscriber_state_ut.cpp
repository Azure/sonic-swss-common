#include <iostream>
#include <memory>
#include <thread>
#include <algorithm>
#include "gtest/gtest.h"
#include "common/dbconnector.h"
#include "common/select.h"
#include "common/selectableevent.h"
#include "common/table.h"
#include "common/subscriberstatetable.h"

using namespace std;
using namespace swss;

#define NUMBER_OF_THREADS   (64) // Spawning more than 256 threads causes libc++ to except
#define NUMBER_OF_OPS     (1000)
#define MAX_FIELDS_DIV      (30) // Testing up to 30 fields objects
#define PRINT_SKIP          (10) // Print + for Producer and - for Subscriber for every 100 ops

static const string testTableName = "UT_REDIS_TABLE";
static const string testTableName2 = "UT_REDIS_TABLE2";

static inline int getMaxFields(int i)
{
    return (i/MAX_FIELDS_DIV) + 1;
}

static inline string key(int index, int keyid)
{
    return string("key_") + to_string(index) + ":" + to_string(keyid);
}

static inline string field(int index, int keyid)
{
    return string("field ") + to_string(index) + ":" + to_string(keyid);
}

static inline string value(int index, int keyid)
{
    if (keyid == 0)
    {
        return string(); // empty
    }

    return string("value ") + to_string(index) + ":" + to_string(keyid);
}

static inline int readNumberAtEOL(const string& str)
{
    if (str.empty())
    {
        return 0;
    }

    auto pos = str.find(":");
    if (pos == str.npos)
    {
        return 0;
    }

    istringstream is(str.substr(pos + 1));

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

static inline void clearDB()
{
    DBConnector db("TEST_DB", 0, true);
    RedisReply r(&db, "FLUSHALL", REDIS_REPLY_STATUS);
    r.checkStatusOK();
}

static void producerWorker(int index)
{
    DBConnector db("TEST_DB", 0, true);
    Table p(&db, testTableName);

    for (int i = 0; i < NUMBER_OF_OPS; i++)
    {
        vector<FieldValueTuple> fields;
        int maxNumOfFields = getMaxFields(i);

        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }

        if ((i % 100) == 0)
        {
            cout << "+" << flush;
        }

        p.set(key(index, i), fields);
    }

    for (int i = 0; i < NUMBER_OF_OPS; i++)
    {
        p.del(key(index, i));
    }
}

static void subscriberWorker(int index, int *status)
{
    DBConnector db("TEST_DB", 0, true);
    SubscriberStateTable c(&db, testTableName);
    Select cs;
    Selectable *selectcs;
    int numberOfKeysSet = 0;
    int numberOfKeyDeleted = 0;
    int ret, i = 0;
    KeyOpFieldsValuesTuple kco;

    cs.addSelectable(&c);

    status[index] = 1;

    while ((ret = cs.select(&selectcs, 10000)) == Select::OBJECT)
    {
        c.pop(kco);
        if (kfvOp(kco) == "SET")
        {
            numberOfKeysSet++;
            validateFields(kfvKey(kco), kfvFieldsValues(kco));
        }
        else if (kfvOp(kco) == "DEL")
        {
            numberOfKeyDeleted++;
        }

        if ((i++ % 100) == 0)
        {
            cout << "-" << flush;
        }

        if (numberOfKeyDeleted == NUMBER_OF_OPS)
        {
            break;
        }

    }

    EXPECT_LE(numberOfKeysSet, numberOfKeyDeleted);

    /* Verify that all data are read */
    {
        ret = cs.select(&selectcs, 1000);
        EXPECT_EQ(ret, Select::TIMEOUT);
    }
}

TEST(SubscriberStateTable, set)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    DBConnector db("TEST_DB", 0, true);
    Table p(&db, testTableName);
    string key = "TheKey";
    int maxNumOfFields = 2;

    /* Prepare subscriber */
    SubscriberStateTable c(&db, testTableName);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);

    /* Set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Pop operation */
    {
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), "SET");

        auto fvs = kfvFieldsValues(kco);
        EXPECT_EQ(fvs.size(), (unsigned int)(maxNumOfFields));

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields; j++)
        {
            EXPECT_EQ(mm[field(index, j)], value(index, j));
        }
    }
}

TEST(SubscriberStateTable, set2_pop1_set1_pop1)
{
    clearDB();

    /* Prepare producer */
    DBConnector db("TEST_DB", 0, true);
    Table p(&db, testTableName);
    int maxNumOfFields = 2;

    /* Prepare subscriber */
    SubscriberStateTable c(&db, testTableName);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);

    /* Set 1st, 2nd */
    int index;
    for (index = 0; index < 2; index++)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }
        p.set(key(index, 0), fields);
    }

    /* Pop 1st */
    index = 0;
    {
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);

        EXPECT_EQ(kfvKey(kco), key(index, 0));
        EXPECT_EQ(kfvOp(kco), "SET");

        auto fvs = kfvFieldsValues(kco);
        EXPECT_EQ(fvs.size(), (unsigned int)(maxNumOfFields));

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields; j++)
        {
            EXPECT_EQ(mm[field(index, j)], value(index, j));
        }
    }

    /* Set 3rd */
    for (index = 2; index < 3; index++)
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }
        p.set(key(index, 0), fields);
    }

    /* Pop 2nd */
    index = 1;
    {
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_EQ(kfvKey(kco), key(index, 0));
        EXPECT_EQ(kfvOp(kco), "SET");

        auto fvs = kfvFieldsValues(kco);
        EXPECT_EQ(fvs.size(), (unsigned int)(maxNumOfFields));

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields; j++)
        {
            EXPECT_EQ(mm[field(index, j)], value(index, j));
        }
    }

    /* Pop 3rd */
    index = 2;
    {
        // Note: the code before this commit will hang at below select()
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_EQ(kfvKey(kco), key(index, 0));
        EXPECT_EQ(kfvOp(kco), "SET");

        auto fvs = kfvFieldsValues(kco);
        EXPECT_EQ(fvs.size(), (unsigned int)(maxNumOfFields));

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields; j++)
        {
            EXPECT_EQ(mm[field(index, j)], value(index, j));
        }
    }
}

TEST(SubscriberStateTable, pops_intial)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    DBConnector db("TEST_DB", 0, true);
    Table p(&db, testTableName);
    string key = "TheKey";
    int maxNumOfFields = 2;

    /* Set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Prepare subscriber */
    SubscriberStateTable c(&db, testTableName);
    Select cs;
    cs.addSelectable(&c);
    std::deque<KeyOpFieldsValuesTuple> entries;

    /* Pop all the initial data */
    {
        c.pops(entries);
        ASSERT_EQ(entries.size(), 1U);
        KeyOpFieldsValuesTuple t = entries[0];
        EXPECT_EQ(kfvKey(t), key);
        auto fvs = kfvFieldsValues(t);
        ASSERT_EQ(fvs.size(), (size_t)maxNumOfFields);

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields; j++)
        {
            EXPECT_EQ(mm[field(index, j)], value(index, j));
        }
    }

    /* Continue pop and get nothing */
    {
        c.pops(entries);
        EXPECT_EQ(entries.size(), 0U);
    }
}

TEST(SubscriberStateTable, del)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    DBConnector db("TEST_DB", 0, true);
    Table p(&db, testTableName);
    string key = "TheKey";
    int maxNumOfFields = 2;

    /* Prepare subscriber */
    SubscriberStateTable c(&db, testTableName);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);

    /* Set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Pop operation for set */
    {
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), "SET");
    }

    p.del(key);

    /* Pop operation for del */
    {
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), "DEL");
    }
}

TEST(SubscriberStateTable, table_state)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    DBConnector db("TEST_DB", 0, true);
    Table p(&db, testTableName);

    for (int i = 0; i < NUMBER_OF_OPS; i++)
    {
       vector<FieldValueTuple> fields;
       int maxNumOfFields = getMaxFields(i);
       for (int j = 0; j < maxNumOfFields; j++)
       {
           FieldValueTuple t(field(index, j), value(index, j));
           fields.push_back(t);
       }

       if ((i % 100) == 0)
       {
           cout << "+" << flush;
       }

       p.set(key(index, i), fields);
    }

    /* Prepare subscriber */
    SubscriberStateTable c(&db, testTableName);
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
       {
           cout << "-" << flush;
       }

       if (numberOfKeysSet == NUMBER_OF_OPS)
           break;
    }

    /* Verify that all data are read */
    {
        ret = cs.select(&selectcs, 1000);
        EXPECT_EQ(ret, Select::TIMEOUT);
    }
}

TEST(SubscriberStateTable, one_producer_multiple_subscriber)
{
    thread *subscriberThreads[NUMBER_OF_THREADS];

    clearDB();

    cout << "Starting " << NUMBER_OF_THREADS << " subscribers on redis" << endl;

    int status[NUMBER_OF_THREADS] = { 0 };

    /* Starting the subscribers before the producer */
    for (int i = 0; i < NUMBER_OF_THREADS; i++)
    {
        subscriberThreads[i] = new thread(subscriberWorker, i, status);
    }

    int i = 0;
    /* Wait for subscribers initialization */
    while (i < NUMBER_OF_THREADS)
    {
        if (status[i])
        {
            ++i;
        }
        else
        {
            sleep(1);
        }
    }

    producerWorker(0);

    for (i = 0; i < NUMBER_OF_THREADS; i++)
    {
        subscriberThreads[i]->join();
        delete subscriberThreads[i];
    }
    cout << endl << "Done." << endl;
}

TEST(SubscriberStateTable, cachedData)
{
    clearDB();

    /* Prepare init data */
    int index = 0;
    int maxNumOfFields = 2;

    DBConnector db("TEST_DB", 0, true);
    Table p(&db, testTableName);
    string key1 = "TheKey1";
    /* Set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }
        p.set(key1, fields);
    }

    Table p2(&db, testTableName2);
    string key2 = "TheKey2";
    /* Set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }
        p2.set(key2, fields);
    }

    /* Prepare subscriber */
    SubscriberStateTable c1(&db, testTableName);
    SubscriberStateTable c2(&db, testTableName2);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c1);
    cs.addSelectable(&c2);

    /* Pop operation and check CachedSelectable */
    {
        string key = key1;
        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        if (selectcs == &c1)
        {
            c1.pop(kco);
        }
        else
        {
            c2.pop(kco);
            key = key2;
        }

        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), "SET");

        /* There is one cached selectable left */
        bool r = cs.isQueueEmpty();
        EXPECT_FALSE(r);

        ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        if (key == key1)
        {
            EXPECT_TRUE(selectcs == &c2);
            key = key2;
            c2.pop(kco);
        }
        else
        {
            EXPECT_TRUE(selectcs == &c1);
            key = key1;
            c1.pop(kco);
        }

        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), "SET");
        /* No cached selectable left */
        r = cs.isQueueEmpty();
        EXPECT_TRUE(r);
    }
}

TEST(SubscriberStateTable, updateOnly)
{
    clearDB();

    /* Prepare init data */
    int index = 0;
    int maxNumOfFields = 2;

    DBConnector db("TEST_DB", 0, true);
    Table p(&db, testTableName);
    string key = "TheKey";
    /* Set operation */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }
        p.set(key, fields);
    }

    /* Prepare subscriber */
    SubscriberStateTable c(&db, testTableName, TableConsumable::DEFAULT_POP_BATCH_SIZE, /*pri=*/0, /*update_only=*/true);
    Select cs;
    Selectable *selectcs;
    cs.addSelectable(&c);
    std::deque<KeyOpFieldsValuesTuple> entries;

    /* Pop all the initial data */
    {
        c.pops(entries);
        ASSERT_EQ(entries.size(), 1U);
        KeyOpFieldsValuesTuple t = entries[0];
        EXPECT_EQ(kfvKey(t), key);
        EXPECT_EQ(kfvOp(t), SET_COMMAND);
        auto fvs = kfvFieldsValues(t);
        ASSERT_EQ(fvs.size(), (size_t)maxNumOfFields);

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields; j++)
        {
            EXPECT_EQ(mm[field(index, j)], value(index, j));
        }
        entries.clear();
    }

    /* Add new attribute */
    {
        vector<FieldValueTuple> fields;
        FieldValueTuple t(field(index, maxNumOfFields), value(index, maxNumOfFields));
        fields.push_back(t);
        p.set(key, fields);

        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        EXPECT_EQ(selectcs, &c);
        c.pops(entries);
        ASSERT_EQ(entries.size(), 1U);
        KeyOpFieldsValuesTuple kco = entries[0];
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), SET_COMMAND);
        auto fvs = kfvFieldsValues(kco);
        ASSERT_EQ(fvs.size(), (size_t)maxNumOfFields + 1);

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j <= maxNumOfFields; j++)
        {
            EXPECT_EQ(mm[field(index, j)], value(index, j));
        }
        entries.clear();
    }

    /* Update attribute */
    {
        vector<FieldValueTuple> fields;
        FieldValueTuple t(field(index, 0), "new_value");
        fields.push_back(t);
        p.set(key, fields);

        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        EXPECT_EQ(selectcs, &c);
        c.pops(entries);
        ASSERT_EQ(entries.size(), 1U);
        KeyOpFieldsValuesTuple kco = entries[0];
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), SET_COMMAND);
        auto fvs = kfvFieldsValues(kco);
        ASSERT_EQ(fvs.size(), (size_t)maxNumOfFields + 1);

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 1; j <= maxNumOfFields; j++)
        {
            EXPECT_EQ(mm[field(index, j)], value(index, j));
        }
        EXPECT_EQ(mm[field(index, 0)], "new_value");
        entries.clear();
    }

    /* Delete key */
    {
        p.del(key);

        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        EXPECT_EQ(selectcs, &c);
        c.pops(entries);
        ASSERT_EQ(entries.size(), 1U);
        KeyOpFieldsValuesTuple kco = entries[0];
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), DEL_COMMAND);
        auto fvs = kfvFieldsValues(kco);
        ASSERT_EQ(fvs.size(), 0);
        entries.clear();
    }

    /* Add new attribute */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }
        p.set(key, fields);

        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        EXPECT_EQ(selectcs, &c);
        c.pops(entries);
        ASSERT_EQ(entries.size(), 1U);
        KeyOpFieldsValuesTuple kco = entries[0];
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), SET_COMMAND);
        auto fvs = kfvFieldsValues(kco);
        ASSERT_EQ(fvs.size(), (size_t)maxNumOfFields);

        map<string, string> mm;
        for (auto fv: fvs)
        {
            mm[fvField(fv)] = fvValue(fv);
        }

        for (int j = 0; j < maxNumOfFields; j++)
        {
            EXPECT_EQ(mm[field(index, j)], value(index, j));
        }
        entries.clear();
    }

    /* Update attribute with no change */
    {
        vector<FieldValueTuple> fields;
        for (int j = 0; j < maxNumOfFields; j++)
        {
            FieldValueTuple t(field(index, j), value(index, j));
            fields.push_back(t);
        }
        p.set(key, fields);

        int ret = cs.select(&selectcs);
        EXPECT_EQ(ret, Select::OBJECT);
        EXPECT_EQ(selectcs, &c);
        c.pops(entries);
        ASSERT_EQ(entries.size(), 1U);
        KeyOpFieldsValuesTuple kco = entries[0];
        EXPECT_EQ(kfvKey(kco), key);
        EXPECT_EQ(kfvOp(kco), SET_COMMAND);
        auto fvs = kfvFieldsValues(kco);
        ASSERT_EQ(fvs.size(), 0);
        entries.clear();
    }
}