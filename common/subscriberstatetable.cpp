#include <string>
#include <deque>
#include <limits>
#include <hiredis/hiredis.h>
#include "dbconnector.h"
#include "table.h"
#include "selectable.h"
#include "redisselect.h"
#include "redisapi.h"
#include "tokenize.h"
#include "subscriberstatetable.h"

using namespace std;

namespace swss {

SubscriberStateTable::SubscriberStateTable(DBConnector *db, const string &tableName, int popBatchSize, int pri, bool update_only)
    : ConsumerTableBase(db, tableName, popBatchSize, pri), m_table(db, tableName), m_update_only(update_only)
{
    m_keyspace = "__keyspace@";

    m_keyspace += to_string(db->getDbId()) + "__:" + tableName + m_table.getTableNameSeparator() + "*";

    psubscribe(m_db, m_keyspace);

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

        if (m_update_only)
        {
            for (auto fv : kfvFieldsValues(kco))
            {
                m_cache[key][fvField(fv)] = fvValue(fv);
            }
        }

        m_buffer.push_back(kco);
    }
}

uint64_t SubscriberStateTable::readData()
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

    m_keyspace_event_buffer.emplace_back(make_shared<RedisReply>(reply));

    /* Try to read data from redis cacher.
     * If data exists put it to event buffer.
     * NOTE: Keyspace event is not persistent and it won't
     * be possible to read it second time. If it is not stared in
     * the buffer it will be lost. */

    reply = nullptr;
    int status;
    do
    {
        status = redisGetReplyFromReader(m_subscribe->getContext(), reinterpret_cast<void**>(&reply));
        if(reply != nullptr && status == REDIS_OK)
        {
            m_keyspace_event_buffer.emplace_back(make_shared<RedisReply>(reply));
        }
    }
    while(reply != nullptr && status == REDIS_OK);

    if (status != REDIS_OK)
    {
        throw std::runtime_error("Unable to read redis reply");
    }
    return 0;
}

bool SubscriberStateTable::hasData()
{
    return m_buffer.size() > 0 || m_keyspace_event_buffer.size() > 0;
}

bool SubscriberStateTable::hasCachedData()
{
    return m_buffer.size() + m_keyspace_event_buffer.size() > 1;
}

void SubscriberStateTable::pops(deque<KeyOpFieldsValuesTuple> &vkco, const string& /*prefix*/)
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
        /* if the Key-space notification is empty, try next one. */
        auto message = event->getReply<RedisMessage>();
        if (message.type.empty())
        {
            continue;
        }

        /* The second element should be the original pattern matched */
        auto ctx = event->getContext()->element[1];
        if (message.pattern != m_keyspace)
        {
            SWSS_LOG_ERROR("invalid pattern %s returned for pmessage of %s", message.pattern.c_str(), m_keyspace.c_str());
            continue;
        }

        string msg = message.channel;
        size_t pos = msg.find(':');
        if (pos == msg.npos)
        {
            SWSS_LOG_ERROR("invalid format %s returned for pmessage of %s", msg.c_str(), m_keyspace.c_str());
            continue;
        }

        string table_entry = msg.substr(pos + 1);
        pos = table_entry.find(m_table.getTableNameSeparator());
        if (pos == table_entry.npos)
        {
            SWSS_LOG_ERROR("invalid key %s returned for pmessage of %s", ctx->str, m_keyspace.c_str());
            continue;
        }
        string key = table_entry.substr(pos + 1);

        string op = message.data;
        if ("del" == op)
        {
            kfvKey(kco) = key;
            kfvOp(kco) = DEL_COMMAND;
            if (m_update_only)
            {
                m_cache.erase(key);
            }
        }
        else
        {
            if (!m_table.get(key, kfvFieldsValues(kco)))
            {
                SWSS_LOG_NOTICE("Miss table key %s, possibly outdated", table_entry.c_str());
                continue;
            }
            if (m_update_only)
            {
                bool update = false;
                for (auto fv : kfvFieldsValues(kco))
                {
                    if (m_cache.find(key) == m_cache.end() ||
                        m_cache.at(key).find(fvField(fv)) == m_cache.at(key).end() ||
                        m_cache.at(key).at(fvField(fv)) != fvValue(fv))
                    {
                        update = true;
                        m_cache[key][fvField(fv)] = fvValue(fv);
                    }
                }
                if (!update)
                {
                    kfvFieldsValues(kco) = std::vector<FieldValueTuple>{};
                }
            }
            kfvKey(kco) = key;
            kfvOp(kco) = SET_COMMAND;
        }

        vkco.push_back(kco);
    }

    m_keyspace_event_buffer.clear();

    return;
}

shared_ptr<RedisReply> SubscriberStateTable::popEventBuffer()
{
    if (m_keyspace_event_buffer.empty())
    {
        return NULL;
    }

    auto reply = m_keyspace_event_buffer.front();
    m_keyspace_event_buffer.pop_front();

    return reply;
}

}
