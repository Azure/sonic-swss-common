#include <nlohmann/json.hpp>
#include "publishereventtable.h"
#include "rediscommand.h"
#include "schema.h"
#include "table.h"

using namespace std;
using namespace swss;

string buildJsonWithKey(const FieldValueTuple &fvHead, const vector<FieldValueTuple> &fv)
{
    nlohmann::json j = nlohmann::json::array();
    j.push_back(fvField(fvHead));
    j.push_back(fvValue(fvHead));

    // we use array to save order
    for (const auto &i : fv)
    {
        j.push_back(fvField(i));
        j.push_back(fvValue(i));
    }

    return j.dump();
}

PublisherEventTable::PublisherEventTable(const DBConnector *db, const std::string &tableName)
    : Table(db, tableName)
{
    m_channel = getChannelName(m_pipe->getDbId());
}

PublisherEventTable::PublisherEventTable(RedisPipeline *pipeline, const std::string &tableName, bool buffered)
    : Table(pipeline, tableName, buffered)
{
    m_channel = getChannelName(m_pipe->getDbId());
}

PublisherEventTable::~PublisherEventTable()
{
}

void PublisherEventTable::set(const string &key, const vector<FieldValueTuple> &values,
                const string &op, const string &prefix)
{
    if (values.size() == 0)
        return;

    RedisCommand cmd;

    cmd.formatHSET(getKeyName(key), values.begin(), values.end());
    m_pipe->push(cmd, REDIS_REPLY_INTEGER);


    FieldValueTuple opdata(SET_COMMAND, key);
    std::string msg = buildJsonWithKey(opdata, values);

    SWSS_LOG_DEBUG("channel %s, publish: %s", m_channel.c_str(), msg.c_str());

    RedisCommand command;
    command.format("PUBLISH %s %s", m_channel.c_str(), msg.c_str());
    m_pipe->push(command, REDIS_REPLY_INTEGER);

    if (!m_buffered)
    {
        m_pipe->flush();
    }
}

void PublisherEventTable::del(const string &key, const string& op, const string& /*prefix*/)
{
    RedisCommand del_key;
    del_key.format("DEL %s", getKeyName(key).c_str());
    m_pipe->push(del_key, REDIS_REPLY_INTEGER);

    FieldValueTuple opdata(DEL_COMMAND, key);
    std::string msg = buildJsonWithKey(opdata, {});

    SWSS_LOG_DEBUG("channel %s, publish: %s", m_channel.c_str(), msg.c_str());

    RedisCommand command;
    command.format("PUBLISH %s %s", m_channel.c_str(), msg.c_str());
    m_pipe->push(command, REDIS_REPLY_INTEGER);

    if (!m_buffered)
    {
        m_pipe->flush();
    }
}

void PublisherEventTable::hdel(const string &key, const string &field, const string& op, const string& /*prefix*/)
{
    RedisCommand hdel_cmd;
    hdel_cmd.format("HDEL %s %s", getKeyName(key).c_str(), field.c_str());
    m_pipe->push(hdel_cmd, REDIS_REPLY_INTEGER);

    FieldValueTuple opdata(HDEL_COMMAND, key);
    std::string msg = buildJsonWithKey(opdata, {FieldValueTuple(field, "")});

    SWSS_LOG_DEBUG("channel %s, publish: %s", m_channel.c_str(), msg.c_str());

    RedisCommand command;
    command.format("PUBLISH %s %s", m_channel.c_str(), msg.c_str());
    m_pipe->push(command, REDIS_REPLY_INTEGER);

    if (!m_buffered)
    {
        m_pipe->flush();
    }
}
