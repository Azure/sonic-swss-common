#include "sonicv2connector.h"
#include "dbconnector.h"
#include "logger.h"

using namespace swss;

SonicV2Connector_Native::SonicV2Connector_Native(bool use_unix_socket_path, const char *netns)
    : m_use_unix_socket_path(use_unix_socket_path)
    , m_netns(netns)
{
}

std::string SonicV2Connector_Native::getNamespace() const
{
    return m_netns;
}

void SonicV2Connector_Native::connect(const std::string& db_name, bool retry_on)
{
    if (m_use_unix_socket_path)
    {
        m_dbintf.set_redis_kwargs(get_db_socket(db_name), "", 0);
    }
    else
    {
        m_dbintf.set_redis_kwargs("", get_db_hostname(db_name), get_db_port(db_name));
    }
    int db_id = get_dbid(db_name);
    m_dbintf.connect(db_id, db_name, retry_on);
}

void SonicV2Connector_Native::close(const std::string& db_name)
{
    m_dbintf.close(db_name);
}

void SonicV2Connector_Native::close()
{
    m_dbintf.close();
}

std::vector<std::string> SonicV2Connector_Native::get_db_list()
{
    return SonicDBConfig::getDbList(m_netns);
}

int SonicV2Connector_Native::get_dbid(const std::string& db_name)
{
    return SonicDBConfig::getDbId(db_name, m_netns);
}

std::string SonicV2Connector_Native::get_db_separator(const std::string& db_name)
{
    return SonicDBConfig::getSeparator(db_name, m_netns);
}

DBConnector& SonicV2Connector_Native::get_redis_client(const std::string& db_name)
{
    return m_dbintf.get_redis_client(db_name);
}

int64_t SonicV2Connector_Native::publish(const std::string& db_name, const std::string& channel, const std::string& message)
{
    return m_dbintf.publish(db_name, channel, message);
}

bool SonicV2Connector_Native::exists(const std::string& db_name, const std::string& key)
{
    return m_dbintf.exists(db_name, key);
}

std::vector<std::string> SonicV2Connector_Native::keys(const std::string& db_name, const char *pattern, bool blocking)
{
    return m_dbintf.keys(db_name, pattern, blocking);
}

std::pair<int, std::vector<std::string>> SonicV2Connector_Native::scan(const std::string& db_name, int cursor, const char *match, uint32_t count)
{
    return m_dbintf.scan(db_name, cursor, match, count);
}

std::shared_ptr<std::string> SonicV2Connector_Native::get(const std::string& db_name, const std::string& _hash, const std::string& key, bool blocking)
{
    return m_dbintf.get(db_name, _hash, key, blocking);
}

bool SonicV2Connector_Native::hexists(const std::string& db_name, const std::string& _hash, const std::string& key)
{
    return m_dbintf.hexists(db_name, _hash, key);
}

std::map<std::string, std::string> SonicV2Connector_Native::get_all(const std::string& db_name, const std::string& _hash, bool blocking)
{
    return m_dbintf.get_all(db_name, _hash, blocking);
}

void SonicV2Connector_Native::hmset(const std::string& db_name, const std::string &key, const std::map<std::string, std::string> &values)
{
    return m_dbintf.hmset(db_name, key, values);
}

int64_t SonicV2Connector_Native::set(const std::string& db_name, const std::string& _hash, const std::string& key, const std::string& val, bool blocking)
{
    return m_dbintf.set(db_name, _hash, key, val, blocking);
}

int64_t SonicV2Connector_Native::del(const std::string& db_name, const std::string& key, bool blocking)
{
    return m_dbintf.del(db_name, key, blocking);
}

void SonicV2Connector_Native::delete_all_by_pattern(const std::string& db_name, const std::string& pattern)
{
    m_dbintf.delete_all_by_pattern(db_name, pattern);
}

std::string SonicV2Connector_Native::get_db_socket(const std::string& db_name)
{
    return SonicDBConfig::getDbSock(db_name, m_netns);
}

std::string SonicV2Connector_Native::get_db_hostname(const std::string& db_name)
{
    return SonicDBConfig::getDbHostname(db_name, m_netns);
}

int SonicV2Connector_Native::get_db_port(const std::string& db_name)
{
    return SonicDBConfig::getDbPort(db_name, m_netns);
}
