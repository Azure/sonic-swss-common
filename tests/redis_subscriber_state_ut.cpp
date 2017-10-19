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

#define TEST_VIEW            (7)
#define NUMBER_OF_THREADS   (64) // Spawning more than 256 threads causes libc++ to except
#define NUMBER_OF_OPS     (1000)
#define MAX_FIELDS_DIV      (30) // Testing up to 30 fields objects
#define PRINT_SKIP          (10) // Print + for Producer and - for Consumer for every 100 ops

static const string dbhost = "localhost";
static const int dbport = 6379;
static const string testTableName = "UT_REDIS_TABLE";

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
        return string(); // emtpy
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

    EXPECT_TRUE(is);

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
    DBConnector db(TEST_VIEW, dbhost, dbport, 0);
    RedisReply r(&db, "FLUSHALL", REDIS_REPLY_STATUS);
    r.checkStatusOK();
}

static void producerWorker(int index)
{
    DBConnector db(TEST_VIEW, dbhost, dbport, 0);
    Table p(&db, testTableName, CONFIGDB_TABLE_NAME_SEPARATOR);

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

static void consumerWorker(int index, int *status)
{
    DBConnector db(TEST_VIEW, dbhost, dbport, 0);
    SubscriberStateTable c(&db, testTableName);
    Select cs;
    Selectable *selectcs;
    int tmpfd;
    int numberOfKeysSet = 0;
    int numberOfKeyDeleted = 0;
    int ret, i = 0;
    KeyOpFieldsValuesTuple kco;

    cs.addSelectable(&c);

    status[index] = 1;

    while ((ret = cs.select(&selectcs, &tmpfd, 10000)) == Select::OBJECT)
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

    EXPECT_TRUE(numberOfKeysSet <= numberOfKeyDeleted);

    /* Verify that all data are read */
    {
        ret = cs.select(&selectcs, &tmpfd, 1000);
        EXPECT_TRUE(ret == Select::TIMEOUT);
    }
}

TEST(SubscriberStateTable, set)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    DBConnector db(TEST_VIEW, dbhost, dbport, 0);
    Table p(&db, testTableName, CONFIGDB_TABLE_NAME_SEPARATOR);
    string key = "TheKey";
    int maxNumOfFields = 2;

    /* Prepare consumer */
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

    int tmpfd;

    /* Pop operation */
    {
        int ret = cs.select(&selectcs, &tmpfd);
        EXPECT_TRUE(ret == Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_TRUE(kfvKey(kco) == key);
        EXPECT_TRUE(kfvOp(kco) == "SET");

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

TEST(SubscriberStateTable, del)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    DBConnector db(TEST_VIEW, dbhost, dbport, 0);
    Table p(&db, testTableName, CONFIGDB_TABLE_NAME_SEPARATOR);
    string key = "TheKey";
    int maxNumOfFields = 2;

    /* Prepare consumer */
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

    int tmpfd;

    /* Pop operation for set */
    {
        int ret = cs.select(&selectcs, &tmpfd);
        EXPECT_TRUE(ret == Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_TRUE(kfvKey(kco) == key);
        EXPECT_TRUE(kfvOp(kco) == "SET");
    }

    p.del(key);

    /* Pop operation for del */
    {
        int ret = cs.select(&selectcs, &tmpfd);
        EXPECT_TRUE(ret == Select::OBJECT);
        KeyOpFieldsValuesTuple kco;
        c.pop(kco);
        EXPECT_TRUE(kfvKey(kco) == key);
        EXPECT_TRUE(kfvOp(kco) == "DEL");
    }
}

TEST(SubscriberStateTable, table_state)
{
    clearDB();

    /* Prepare producer */
    int index = 0;
    DBConnector db(TEST_VIEW, dbhost, dbport, 0);
    Table p(&db, testTableName, CONFIGDB_TABLE_NAME_SEPARATOR);

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

    /* Prepare consumer */
    SubscriberStateTable c(&db, testTableName);
    Select cs;
    Selectable *selectcs;
    int tmpfd;
    int ret, i = 0;
    KeyOpFieldsValuesTuple kco;

    cs.addSelectable(&c);
    int numberOfKeysSet = 0;

    while ((ret = cs.select(&selectcs, &tmpfd)) == Select::OBJECT)
    {
       c.pop(kco);
       EXPECT_TRUE(kfvOp(kco) == "SET");
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
        ret = cs.select(&selectcs, &tmpfd, 1000);
        EXPECT_TRUE(ret == Select::TIMEOUT);
    }
}

TEST(SubscriberStateTable, one_producer_multiple_consumer)
{
    thread *consumerThreads[NUMBER_OF_THREADS];

    clearDB();

    cout << "Starting " << NUMBER_OF_THREADS << " consumers on redis" << endl;

    int status[NUMBER_OF_THREADS] = { 0 };

    /* Starting the consumers before the producer */
    for (int i = 0; i < NUMBER_OF_THREADS; i++)
    {
        consumerThreads[i] = new thread(consumerWorker, i, status);
    }

    int i = 0;
    /* Wait for consumers initialization */
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
        consumerThreads[i]->join();
        delete consumerThreads[i];
    }
    cout << endl << "Done." << endl;
}
