#include <boost/algorithm/string.hpp>

#include "common/decoratortable.h"
#include "common/profileprovider.h"

using namespace std;
using namespace swss;

DecoratorTable::DecoratorTable(const DBConnector *db, const string &tableName)
    : Table(db, tableName)
{
    m_defaultValueProvider = std::make_shared<DefaultValueProvider>();
}

DecoratorTable::DecoratorTable(RedisPipeline *pipeline, const string &tableName, bool buffered)
:Table(pipeline, tableName, buffered)
{
    m_defaultValueProvider = std::make_shared<DefaultValueProvider>();
}

/* Get all the field-value tuple of the table entry with the key */
bool DecoratorTable::get(const string &key, vector<pair<string, string>> &ovalues)
{
    bool result = Table::get(key, ovalues);
    auto connector = const_cast<DBConnector*>(m_pipe->getDBConnector());
    auto table = getTableName();

    // Append profile config
    bool append = ProfileProvider::instance().appendConfigs(table, key, ovalues, connector);
    if (!result && append)
    {
        // No user config on this key, but found profile config on this key.
        result = true;
    }

    // Append default values when key exist
    if (result)
    {
        m_defaultValueProvider->appendDefaultValues(table, key, ovalues);
    }

    return result;
}

bool DecoratorTable::hget(const string &key, const string &field,  string &value)
{
    auto result = Table::hget(key,
                        field,
                        value);
    if (result)
    {
        return true;
    }

    auto connector = const_cast<DBConnector*>(m_pipe->getDBConnector());
    auto table = getTableName();

    // Try append profile config
    auto profile = ProfileProvider::instance().getConfig(table, key, field, connector);
    if (profile != nullptr)
    {
        value = *profile;
        return true;
    }

    // Try append default values
    auto default_value = m_defaultValueProvider->getDefaultValue(table, key, field);
    if (default_value != nullptr)
    {
        value = *default_value;
        return true;
    }

    return false;
}

/* Get all the keys in the table */
void DecoratorTable::getKeys(vector<string> &keys)
{
    Table::getKeys(keys);

    // Append profile keys
    auto connector = const_cast<DBConnector*>(m_pipe->getDBConnector());
    auto table = getTableName();
    auto profile_keys = ProfileProvider::instance().getKeys(table, connector);
    for (auto &profile_key : profile_keys)
    {
        if(find(keys.begin(), keys.end(), profile_key) == keys.end())
        {
            keys.emplace_back(profile_key);
        }
    }
}

/* Set an entry in the DB directly and configure ttl for it (op not in use) */
void DecoratorTable::set(const std::string &key,
                 const std::vector<FieldValueTuple> &values, 
                 const std::string &op,
                 const std::string &prefix,
                 const int64_t &ttl)
{
    auto connector = const_cast<DBConnector*>(m_pipe->getDBConnector());
    auto table = getTableName();
    if (values.size())
    {
        ProfileProvider::instance().tryRevertItem(table, key, connector);
    }
    else
    {
        // Set a entry to empty will delete entry.
        ProfileProvider::instance().tryDeleteItem(table, key, connector);
    }

    Table::set(key,
                 values, 
                 op,
                 prefix,
                 ttl);
}

/* Delete an entry in the table */
void DecoratorTable::del(const std::string &key,
                 const std::string &op,
                 const std::string &prefix)
{
    auto connector = const_cast<DBConnector*>(m_pipe->getDBConnector());
    auto table = getTableName();
    ProfileProvider::instance().tryDeleteItem(table, key, connector);

    Table::del(key,
                 op,
                 prefix);
}

void DecoratorTable::hset(const std::string &key,
                 const std::string &field,
                 const std::string &value,
                 const std::string &op,
                 const std::string &prefix)
{
    auto connector = const_cast<DBConnector*>(m_pipe->getDBConnector());
    auto table = getTableName();
    ProfileProvider::instance().tryRevertItem(table, key, connector);

    Table::hset(key,
                 field,
                 value,
                 op,
                 prefix);
}