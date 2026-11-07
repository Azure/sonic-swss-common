#pragma once

#include "subscriberstatetable.h"

namespace swss {

class SubscriberEventTable : public ConsumerTableBase
{
public:
    SubscriberEventTable(DBConnector *db, const std::string &tableName, int popBatchSize = DEFAULT_POP_BATCH_SIZE, int pri = 0);

    /* Get all available events from pub/sub channel */
    void pops(std::deque<KeyOpFieldsValuesTuple> &vkco, const std::string &prefix = EMPTY_PREFIX);

    /* Read event from redis channel*/
    uint64_t readData() override;
    bool hasData() override;
    bool hasCachedData() override;
    bool initializedWithData() override
    {
        return !m_buffer.empty();
    }

private:
    /* Pop event from event buffer. Caller should free resources. */
    std::shared_ptr<RedisReply> popEventBuffer();

    std::string m_channel;

    std::deque<std::shared_ptr<RedisReply>> m_event_buffer;
    Table m_table;
};

}
