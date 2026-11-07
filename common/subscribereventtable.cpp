#include <string>
#include <deque>
#include <limits>
#include <hiredis/hiredis.h>
#include "json.h"
#include "dbconnector.h"
#include "rediscommand.h"
#include "table.h"
#include "selectable.h"
#include "redisselect.h"
#include "redisapi.h"
#include "tokenize.h"
#include "subscriberstatetable.h"
#include "subscribereventtable.h"

using namespace std;

namespace swss {

// TODO: reuse
#define REDIS_PUBLISH_MESSAGE_ELEMNTS (3)
#define REDIS_PUBLISH_MESSAGE_INDEX (2)
string processReply(redisReply *reply)
{
    SWSS_LOG_ENTER();

    if (reply->type != REDIS_REPLY_ARRAY)
    {
        // SWSS_LOG_ERROR("expected ARRAY redis reply on channel %s, got: %d", m_channel.c_str(), reply->type);

        throw std::runtime_error("getRedisReply operation failed");
    }

    if (reply->elements != REDIS_PUBLISH_MESSAGE_ELEMNTS)
    {
        // SWSS_LOG_ERROR("expected %d elements in redis reply on channel %s, got: %zu",
        //                REDIS_PUBLISH_MESSAGE_ELEMNTS,
        //                m_channel.c_str(),
        //                reply->elements);

        throw std::runtime_error("getRedisReply operation failed");
    }

    std::string msg = std::string(reply->element[REDIS_PUBLISH_MESSAGE_INDEX]->str);

    SWSS_LOG_DEBUG("got message: %s", msg.c_str());

    return msg;
}

SubscriberEventTable::SubscriberEventTable(DBConnector *db, const string &tableName, int popBatchSize, int pri)
    : ConsumerTableBase(db, tableName, popBatchSize, pri), m_table(db, tableName)
{
    m_channel = getChannelName(m_db->getDbId());

    subscribe(m_db, m_channel);

    vector<string> keys;
    m_table.getKeys(keys);

    for (const auto &key: keys)
    {
        KeyOpFieldsValuesTuple kco;

        kfvKey(kco) = key;
        kfvOp(kco) = SET_COMMAND;

        if (!m_table.get(key, kfvFieldsValues(kco)))
        {
            continue;
        }

        m_buffer.push_back(kco);
    }
}

uint64_t SubscriberEventTable::readData()
{
    redisReply *reply = nullptr;

    /* Read data from redis. This call is non blocking. This method
     * is called from Select framework when data is available in socket.
     * NOTE: All data should be stored in event buffer. It won't be possible to
     * read them second time. */
    if (redisGetReply(m_subscribe->getContext(), reinterpret_cast<void**>(&reply)) != REDIS_OK)
    {
        throw std::runtime_error("Unable to read redis reply");
    }

    m_event_buffer.emplace_back(make_shared<RedisReply>(reply));

    /* Try to read data from redis cacher.
     * If data exists put it to event buffer.
     * NOTE: channel event is not persistent and it won't
     * be possible to read it second time. If it is not stored in
     * the buffer it will be lost. */

    reply = nullptr;
    int status;
    do
    {
        status = redisGetReplyFromReader(m_subscribe->getContext(), reinterpret_cast<void**>(&reply));
        if(reply != nullptr && status == REDIS_OK)
        {
            m_event_buffer.emplace_back(make_shared<RedisReply>(reply));
        }
    }
    while(reply != nullptr && status == REDIS_OK);

    if (status != REDIS_OK)
    {
        throw std::runtime_error("Unable to read redis reply");
    }
    return 0;
}

bool SubscriberEventTable::hasData()
{
    return m_buffer.size() > 0 || m_event_buffer.size() > 0;
}

bool SubscriberEventTable::hasCachedData()
{
    return m_buffer.size() + m_event_buffer.size() > 1;
}

void SubscriberEventTable::pops(deque<KeyOpFieldsValuesTuple> &vkco, const string& /*prefix*/)
{
    vkco.clear();

    if (!m_buffer.empty())
    {
        vkco.insert(vkco.end(), m_buffer.begin(), m_buffer.end());
        m_buffer.clear();
        return;
    }

    while (auto event = popEventBuffer())
    {

        KeyOpFieldsValuesTuple kco;
        auto &values = kfvFieldsValues(kco);
        string msg = processReply(event.get()->getContext());

        JSon::readJson(msg, values);

        FieldValueTuple fvHead = values.at(0);

        kfvOp(kco) = fvField(fvHead);
        kfvKey(kco) = fvValue(fvHead);

        values.erase(values.begin());

        vkco.push_back(kco);
    }

    m_event_buffer.clear();

    return;
}

shared_ptr<RedisReply> SubscriberEventTable::popEventBuffer()
{
    if (m_event_buffer.empty())
    {
        return nullptr;
    }

    auto reply = m_event_buffer.front();
    m_event_buffer.pop_front();

    return reply;
}

}
