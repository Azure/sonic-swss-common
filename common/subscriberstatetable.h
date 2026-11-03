#pragma once

#include <string>
#include <deque>
#include <memory.h>
#include <unordered_map>
#include "dbconnector.h"
#include "consumertablebase.h"

namespace swss {

class SubscriberStateTable : public ConsumerTableBase
{
public:
    SubscriberStateTable(DBConnector *db, const std::string &tableName, int popBatchSize = DEFAULT_POP_BATCH_SIZE, int pri = 0, bool update_only = false);

    /* Get all elements available */
    void pops(std::deque<KeyOpFieldsValuesTuple> &vkco, const std::string &prefix = EMPTY_PREFIX);

    /* Read keyspace event from redis */
    uint64_t readData() override;
    bool hasData() override;
    bool hasCachedData() override;
    bool initializedWithData() override
    {
        return !m_buffer.empty();
    }

private:
    /* Pop keyspace event from event buffer. Caller should free resources. */
    std::shared_ptr<RedisReply> popEventBuffer();

    std::string m_keyspace;

    std::deque<std::shared_ptr<RedisReply>> m_keyspace_event_buffer;
    Table m_table;

    /* If update_only is set to true, pop will return empty attribute instead of
     * full list of attributes when there is no change for SET request. */
    bool m_update_only;
    /* Local cache used to check the updated attributes when m_update_only is set. */
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> m_cache;
};

}
