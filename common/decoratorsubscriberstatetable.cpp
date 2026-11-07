#include <boost/algorithm/string.hpp>
#include "common/decoratorsubscriberstatetable.h"
#include "common/defaultvalueprovider.h"
#include "common/profileprovider.h"

using namespace std;
using namespace swss;

DecoratorSubscriberStateTable::DecoratorSubscriberStateTable(DBConnector *db, const string &tableName, int popBatchSize, int pri)
:SubscriberStateTable(db, tableName, popBatchSize, pri)
{
    // Subscribe PROFILE_DELETE table for profile delete event
    m_profile_keyspace = "__keyspace@";
    m_profile_keyprefix = ProfileProvider::instance().getDeletedKeyName(tableName, "", db);
    m_profile_keyspace += to_string(db->getDbId()) + "__:" + m_profile_keyprefix + "*";

    m_subscribe->psubscribe(m_profile_keyspace);

    m_defaultValueProvider = std::make_shared<DefaultValueProvider>();
}

/* Get multiple pop elements */
void DecoratorSubscriberStateTable::pops(deque<KeyOpFieldsValuesTuple> &vkco, const string &prefix)
{
    SubscriberStateTable::pops(vkco, prefix);
    for (auto& kco : vkco)
    {
        appendDefaultValue(kfvKey(kco), kfvOp(kco), kfvFieldsValues(kco));
    }
}

void DecoratorSubscriberStateTable::appendDefaultValue(std::string &key, std::string &op, std::vector<FieldValueTuple> &fvs)
{
    if (op != SET_COMMAND)
    {
        // When pops, 'set' operation means read data, only read need decorate.
        return;
    }

    auto table = getTableName();
    m_defaultValueProvider->appendDefaultValues(table, key, fvs);
}

void DecoratorSubscriberStateTable::onPopUnknownPattern(RedisMessage& message, deque<KeyOpFieldsValuesTuple> &vkco)
{
    if (message.pattern != m_profile_keyspace)
    {
        SWSS_LOG_ERROR("invalid pattern %s returned for pmessage of %s", message.pattern.c_str(), m_profile_keyspace.c_str());
        SubscriberStateTable::onPopUnknownPattern(message, vkco);
        return;
    }

    string op = message.data;
    if ("del" == op)
    {
        // 'DEL' from PROFILE_DETETE table will revert profile config, ignore this event because there will always be a user config 'SET' event after this event.
        return;
    }

    string msg = message.channel;
    size_t pos = msg.find(m_profile_keyprefix);
    if (pos == msg.npos)
    {
        SWSS_LOG_ERROR("invalid key returned for pmessage of %s", m_profile_keyspace.c_str());
        return;
    }

    // 'SET' to PROFILE_DETETE table is delete profile config, convert this event to a config 'DEL' event
    string key = msg.substr(pos + m_profile_keyprefix.length());
    KeyOpFieldsValuesTuple kco;
    kfvKey(kco) = key;
    kfvOp(kco) = DEL_COMMAND;
    vkco.push_back(kco);
}
