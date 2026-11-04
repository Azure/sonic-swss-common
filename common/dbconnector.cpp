#include <string.h>
#include <stdint.h>
#include <vector>
#include <unistd.h>
#include <errno.h>
#include <system_error>
#include <fstream>
#include <nlohmann/json.hpp>
#include <set>
#include <unistd.h>
#include "logger.h"

#include "common/dbconnector.h"
#include "common/redisreply.h"
#include "common/redispipeline.h"
#include "common/pubsub.h"

using json = nlohmann::json;
using namespace std;
using namespace swss;

namespace {
constexpr size_t KEY_DEL_CHUNK_SIZE = 128;
}


void SonicDBConfig::parseDatabaseConfig(const string &file,
                    std::map<std::string, RedisInstInfo> &inst_entry,
                    std::unordered_map<std::string, SonicDBInfo> &db_entry,
                    std::unordered_map<int, std::string> &separator_entry,
                    bool ignore_nonexistent)
{
    if (ignore_nonexistent && access(file.c_str(), F_OK) == -1) {
        SWSS_LOG_NOTICE("Sonic database config file doesn't exist at %s\n", file.c_str());
        return;
    }

    ifstream i(file);
    if (i.good())
    {
        try
        {
            json j;
            i >> j;
            for (auto it = j["INSTANCES"].begin(); it!= j["INSTANCES"].end(); it++)
            {
               string instName = it.key();
               string socket;
               auto path = it.value().find("unix_socket_path");
               if (path != it.value().end())
               {
                    socket = *path;
               }
               string hostname = it.value().at("hostname");
               int port = it.value().at("port");
               inst_entry[instName] = {socket, hostname, port};
            }

            for (auto it = j["DATABASES"].begin(); it!= j["DATABASES"].end(); it++)
            {
               string dbName = it.key();
               string instName = it.value().at("instance");
               int dbId = it.value().at("id");
               string separator = it.value().at("separator");
               db_entry[dbName] = {instName, dbId, separator};

               separator_entry.emplace(dbId, separator);
            }
        }

        catch (domain_error& e)
        {
            SWSS_LOG_ERROR("key doesn't exist in json object, NULL value has no iterator >> %s\n", e.what());
            throw runtime_error("key doesn't exist in json object, NULL value has no iterator >> " + string(e.what()));
        }
        catch (exception &e)
        {
            SWSS_LOG_ERROR("Sonic database config file syntax error >> %s\n", e.what());
            throw runtime_error("Sonic database config file syntax error >> " + string(e.what()));
        }
    }
    else
    {
        SWSS_LOG_ERROR("Sonic database config file doesn't exist at %s\n", file.c_str());
        throw runtime_error("Sonic database config file doesn't exist at " + file);
    }
}

void SonicDBConfig::initializeGlobalConfig(const string &file, bool ignore_nonexistent)
{
    std::string dir_name;
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    SWSS_LOG_ENTER();

    if (m_global_init)
    {
        SWSS_LOG_ERROR("SonicDBConfig Global config is already initialized");
        return;
    }

    ifstream i(file);
    if (i.good())
    {
        // Get the directory name from the file path given as input.
        std::string::size_type pos = file.rfind("/");
        if( pos != std::string::npos)
        {
            dir_name = file.substr(0,pos+1);
        }

        try
        {
            json j;
            i >> j;

            for (auto& element : j["INCLUDES"])
            {
                std::unordered_map<std::string, SonicDBInfo> db_entry;
                std::map<std::string, RedisInstInfo> inst_entry;
                std::unordered_map<int, std::string> separator_entry;
                std::string local_file = dir_name;
                local_file.append(element["include"]);
                SonicDBKey key;
                if(!element["namespace"].empty())
                {
                    key.netns = element["namespace"];
                }

                if (!element["container_name"].empty())
                {
                    key.containerName = element["container_name"];
                }

                // If database_config.json is already initlized via SonicDBConfig::initialize
                // skip initializing it here again.
                if (key.isEmpty() && m_init)
                {
                    continue;
                }

                parseDatabaseConfig(local_file, inst_entry, db_entry, separator_entry, ignore_nonexistent);
                // all the entries are empty, then don't add them to the map. It may happen if the included
                // config doesn't exist. For example, dash-ha container will only mount the redis instance
                // config file for the dpu it is managing.
                if (!inst_entry.empty() || !db_entry.empty() || !separator_entry.empty())
                {
                    m_inst_info[key] = inst_entry;
                    m_db_info[key] = db_entry;
                    m_db_separator[key] = separator_entry;
                }

                if(key.isEmpty())
                {
                    // Make regular init also done
                    m_init = true;
                }
            }
        }

        catch (domain_error& e)
        {
            SWSS_LOG_ERROR("key doesn't exist in json object, NULL value has no iterator >> %s\n", e.what());
            throw runtime_error("key doesn't exist in json object, NULL value has no iterator >> " + string(e.what()));
        }
        catch (exception &e)
        {
            SWSS_LOG_ERROR("Sonic database config file syntax error >> %s\n", e.what());
            throw runtime_error("Sonic database config file syntax error >> " + string(e.what()));
        }
    }
    else
    {
        SWSS_LOG_ERROR("Sonic database config global file doesn't exist at %s\n", file.c_str());
    }


    // Set it as the global config file is already parsed and init done.
    m_global_init = true;
}

void SonicDBConfig::initialize(const string &file)
{
    std::unordered_map<std::string, SonicDBInfo> db_entry;
    std::map<std::string, RedisInstInfo> inst_entry;
    std::unordered_map<int, std::string> separator_entry;
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    SWSS_LOG_ENTER();

    if (m_init)
    {
        SWSS_LOG_ERROR("SonicDBConfig already initialized");
        throw runtime_error("SonicDBConfig already initialized");
    }

    SonicDBKey empty_key;
    parseDatabaseConfig(file, inst_entry, db_entry, separator_entry);
    m_inst_info.emplace(empty_key, std::move(inst_entry));
    m_db_info.emplace(empty_key, std::move(db_entry));
    m_db_separator.emplace(empty_key, std::move(separator_entry));

    // Set it as the config file is already parsed and init done.
    m_init = true;
}

// This API is used to reset the SonicDBConfig class.
// And then user can call initialize with different config file.
void SonicDBConfig::reset()
{
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);
    m_init = false;
    m_global_init = false;
    m_inst_info.clear();
    m_db_info.clear();
    m_db_separator.clear();
}

void SonicDBConfig::validateNamespace(const string &netns)
{
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    SWSS_LOG_ENTER();

    // With valid namespace input and database_global.json is not loaded, ask user to initializeGlobalConfig first
    if(!netns.empty())
    {
        // If global initialization is not done, ask user to initialize global DB Config first.
        if (!m_global_init)
        {
            SWSS_LOG_THROW("Initialize global DB config using API SonicDBConfig::initializeGlobalConfig");
        }

        // Check if the namespace is valid, check if this is a key in either of this map
        for (const auto &entry: m_inst_info)
        {
            if (entry.first.netns == netns)
            {
                return;
            }
        }
        SWSS_LOG_THROW("Namespace %s is not a valid namespace name in config file", netns.c_str());
    }
}

SonicDBInfo& SonicDBConfig::getDbInfo(const std::string &dbName, const SonicDBKey &key)
{
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    SWSS_LOG_ENTER();

    if (!m_init)
        initialize(DEFAULT_SONIC_DB_CONFIG_FILE);

    if (!key.isEmpty())
    {
        if (!m_global_init)
        {
            SWSS_LOG_THROW("Initialize global DB config using API SonicDBConfig::initializeGlobalConfig");
        }
    }
    auto foundEntry = m_db_info.find(key);
    if (foundEntry == m_db_info.end())
    {
        string msg = "Key " + key.toString() + " is not a valid key name in config file";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    auto& infos = foundEntry->second;
    auto foundDb = infos.find(dbName);
    if (foundDb == infos.end())
    {
        string msg = "Failed to find " + dbName + " database in " + key.toString() + " key";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    return foundDb->second;
}

RedisInstInfo& SonicDBConfig::getRedisInfo(const std::string &dbName, const SonicDBKey &key)
{
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    SWSS_LOG_ENTER();

    if (!m_init)
        initialize(DEFAULT_SONIC_DB_CONFIG_FILE);

    if (!key.isEmpty())
    {
        if (!m_global_init)
        {
            SWSS_LOG_THROW("Initialize global DB config using API SonicDBConfig::initializeGlobalConfig");
        }
    }
    auto foundEntry = m_inst_info.find(key);
    if (foundEntry == m_inst_info.end())
    {
        string msg = "Key " + key.toString() + " is not a valid key name in Redis instances in config file";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    auto& redisInfos = foundEntry->second;
    auto foundRedis = redisInfos.find(getDbInst(dbName, key));
    if (foundRedis == redisInfos.end())
    {
        string msg = "Failed to find the Redis instance for " + dbName + " database in " + key.toString() + " key";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    return foundRedis->second;
}

string SonicDBConfig::getDbInst(const string &dbName, const string &netns, const std::string &containerName)
{
    SonicDBKey key;
    key.netns = netns;
    key.containerName = containerName;
    return getDbInst(dbName, key);
}

string SonicDBConfig::getDbInst(const std::string &dbName, const SonicDBKey &key)
{
    return getDbInfo(dbName, key).instName;
}

int SonicDBConfig::getDbId(const string &dbName, const string &netns, const std::string &containerName)
{
    SonicDBKey key;
    key.netns = netns;
    key.containerName = containerName;
    return getDbId(dbName, key);
}

int SonicDBConfig::getDbId(const std::string &dbName, const SonicDBKey &key)
{
    return getDbInfo(dbName, key).dbId;
}

string SonicDBConfig::getSeparator(const string &dbName, const string &netns, const std::string &containerName)
{
    SonicDBKey key;
    key.netns = netns;
    key.containerName = containerName;
    return getSeparator(dbName, key);
}

string SonicDBConfig::getSeparator(const std::string &dbName, const SonicDBKey &key)
{
    return getDbInfo(dbName, key).separator;
}

string SonicDBConfig::getSeparator(int dbId, const string &netns, const std::string &containerName)
{
    SonicDBKey key;
    key.netns = netns;
    key.containerName = containerName;
    return getSeparator(dbId, key);
}

std::string SonicDBConfig::getSeparator(int dbId, const SonicDBKey &key)
{
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    if (!m_init)
        initialize(DEFAULT_SONIC_DB_CONFIG_FILE);

    if (!key.isEmpty())
    {
        if (!m_global_init)
        {
            SWSS_LOG_THROW("Initialize global DB config using API SonicDBConfig::initializeGlobalConfig");
        }
    }
    auto foundEntry = m_db_separator.find(key);
    if (foundEntry == m_db_separator.end())
    {
        string msg = "Key " + key.toString() + " is not a valid key name in config file";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    auto seps = foundEntry->second;
    auto foundDb = seps.find(dbId);
    if (foundDb == seps.end())
    {
        string msg = "Failed to find " + to_string(dbId) + " database in " + key.toString() + " key";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    return foundDb->second;
}

string SonicDBConfig::getSeparator(const DBConnector* db)
{
    if (!db)
    {
        throw std::invalid_argument("db cannot be null");
    }

    string dbName = db->getDbName();
    auto key = db->getDBKey();
    if (dbName.empty())
    {
        return getSeparator(db->getDbId(), key);
    }
    else
    {
        return getSeparator(dbName, key);
    }
}

string SonicDBConfig::getDbSock(const string &dbName, const string &netns, const std::string &containerName)
{
    SonicDBKey key;
    key.netns = netns;
    key.containerName = containerName;
    return getDbSock(dbName, key);
}

string SonicDBConfig::SonicDBConfig::getDbSock(const string &dbName, const SonicDBKey &key)
{
    return getRedisInfo(dbName, key).unixSocketPath;
}

string SonicDBConfig::getDbHostname(const string &dbName, const string &netns, const std::string &containerName)
{
    SonicDBKey key;
    key.netns = netns;
    key.containerName = containerName;
    return getDbHostname(dbName, key);
}

string SonicDBConfig::getDbHostname(const string &dbName, const SonicDBKey &key)
{
    return getRedisInfo(dbName, key).hostname;
}

int SonicDBConfig::getDbPort(const string &dbName, const string &netns, const std::string &containerName)
{
    SonicDBKey key;
    key.netns = netns;
    key.containerName = containerName;
    return getDbPort(dbName, key);
}

int SonicDBConfig::getDbPort(const string &dbName, const SonicDBKey &key)
{
    return getRedisInfo(dbName, key).port;
}

vector<string> SonicDBConfig::getNamespaces()
{
    set<string> list;
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    if (!m_init)
        initialize(DEFAULT_SONIC_DB_CONFIG_FILE);

    // This API returns back all namespaces including '' representing global ns.
    for (auto it = m_inst_info.cbegin(); it != m_inst_info.cend(); ++it) {
        list.insert(it->first.netns);
    }

    return vector<string>(list.begin(), list.end());
}

vector<SonicDBKey> SonicDBConfig::getDbKeys()
{
    vector<SonicDBKey> keys;
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    if (!m_init)
        initialize(DEFAULT_SONIC_DB_CONFIG_FILE);

    // This API returns back all db keys.
    for (auto it = m_inst_info.cbegin(); it != m_inst_info.cend(); ++it) {
        keys.push_back(it->first);
    }

    return keys;
}

std::vector<std::string> SonicDBConfig::getDbList(const std::string &netns, const std::string &containerName)
{
    SonicDBKey key;
    key.netns = netns;
    key.containerName = containerName;
    return getDbList(key);
}

std::vector<std::string> SonicDBConfig::getDbList(const SonicDBKey &key)
{
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);
    if (!m_init)
    {
        initialize();
    }
    validateNamespace(key.netns);

    std::vector<std::string> dbNames;
    for (auto& imap: m_db_info.at(key))
    {
        dbNames.push_back(imap.first);
    }
    return dbNames;
}

map<string, RedisInstInfo> SonicDBConfig::getInstanceList(const std::string &netns, const std::string &containerName)
{
    SonicDBKey key;
    key.netns = netns;
    key.containerName = containerName;
    return getInstanceList(key);
}

map<string, RedisInstInfo> SonicDBConfig::getInstanceList(const SonicDBKey &key)
{
    if (!m_init)
    {
        initialize();
    }
    validateNamespace(key.netns);

    map<string, RedisInstInfo> result;
    auto iterator = m_inst_info.find(key);
    if (iterator != m_inst_info.end()) {
        return iterator->second;
    }

    return map<string, RedisInstInfo>();
}

constexpr const char *SonicDBConfig::DEFAULT_SONIC_DB_CONFIG_FILE;
constexpr const char *SonicDBConfig::DEFAULT_SONIC_DB_GLOBAL_CONFIG_FILE;
std::recursive_mutex SonicDBConfig::m_db_info_mutex;
unordered_map<SonicDBKey, std::map<std::string, RedisInstInfo>, SonicDBKeyHash> SonicDBConfig::m_inst_info;
unordered_map<SonicDBKey, std::unordered_map<std::string, SonicDBInfo>, SonicDBKeyHash> SonicDBConfig::m_db_info;
unordered_map<SonicDBKey, std::unordered_map<int, std::string>, SonicDBKeyHash> SonicDBConfig::m_db_separator;
bool SonicDBConfig::m_init = false;
bool SonicDBConfig::m_global_init = false;

constexpr const char *RedisContext::DEFAULT_UNIXSOCKET;

RedisContext::~RedisContext()
{
    if(m_conn)
        redisFree(m_conn);
}

RedisContext::RedisContext()
    : m_conn(NULL)
{
}

RedisContext::RedisContext(const RedisContext &other)
{
    auto octx = other.getContext();
    const char *unixPath = octx->unix_sock.path;
    if (unixPath)
    {
#if HIREDIS_MAJOR >= 1
        initContext(unixPath, octx->connect_timeout);
#else
        initContext(unixPath, octx->timeout);
#endif
    }
    else
    {
#if HIREDIS_MAJOR >= 1
        initContext(octx->tcp.host, octx->tcp.port, octx->connect_timeout);
#else
        initContext(octx->tcp.host, octx->tcp.port, octx->timeout);
#endif
    }
}

void RedisContext::initContext(const char *host, int port, const timeval *tv)
{
    if (tv)
    {
        m_conn = redisConnectWithTimeout(host, port, *tv);
    }
    else
    {
        m_conn = redisConnect(host, port);
    }

    if (m_conn->err)
        throw system_error(make_error_code(errc::address_not_available),
                           "Unable to connect to redis - " + std::string(m_conn->errstr) + "(" + std::to_string(m_conn->err) + ")");
}

void RedisContext::initContext(const char *path, const timeval *tv)
{
    if (tv)
    {
        m_conn = redisConnectUnixWithTimeout(path, *tv);
    }
    else
    {
        m_conn = redisConnectUnix(path);
    }

    if (m_conn->err)
        throw system_error(make_error_code(errc::address_not_available),
                           "Unable to connect to redis (unix-socket) - " + std::string(m_conn->errstr) + "(" + std::to_string(m_conn->err) + ")");
}

redisContext *RedisContext::getContext() const
{
    return m_conn;
}

void RedisContext::setContext(redisContext *ctx)
{
    m_conn = ctx;
}

void RedisContext::setClientName(const string& clientName)
{
    string command("CLIENT SETNAME ");
    command += clientName;

    RedisReply r(this, command, REDIS_REPLY_STATUS);
    r.checkStatusOK();
}

string RedisContext::getClientName()
{
    string command("CLIENT GETNAME");

    RedisReply r(this, command);

    auto ctx = r.getContext();
    if (ctx->type == REDIS_REPLY_STRING)
    {
        return r.getReply<std::string>();
    }
    else
    {
        if (ctx->type != REDIS_REPLY_NIL)
            SWSS_LOG_ERROR("Unable to obtain Redis client name");

        return "";
    }
}

void DBConnector::select(DBConnector *db)
{
    string select("SELECT ");
    select += to_string(db->getDbId());

    RedisReply r(db, select, REDIS_REPLY_STATUS);
    r.checkStatusOK();
}

DBConnector::DBConnector(const DBConnector &other)
    : RedisContext(other)
    , m_dbId(other.m_dbId)
    , m_dbName(other.m_dbName)
    , m_key(other.m_key)
{
    select(this);
}

DBConnector::DBConnector(int dbId, const RedisContext& ctx)
    : RedisContext(ctx)
    , m_dbId(dbId)
{
    select(this);
}

static struct timeval ms_to_timeval(unsigned int ms) {
    return {
        .tv_sec = (time_t)ms / 1000,
        .tv_usec = ((suseconds_t)ms % 1000) * 1000
    };
}

DBConnector::DBConnector(int dbId, const string& hostname, int port,
                         unsigned int timeout_ms)
    : m_dbId(dbId)
{
    struct timeval tv = ms_to_timeval(timeout_ms);
    struct timeval *ptv = timeout_ms ? &tv : NULL;
    initContext(hostname.c_str(), port, ptv);

    select(this);
}

DBConnector::DBConnector(int dbId, const string& unixPath, unsigned int timeout_ms)
    : m_dbId(dbId)
{
    struct timeval tv = ms_to_timeval(timeout_ms);
    struct timeval *ptv = timeout_ms ? &tv : NULL;
    initContext(unixPath.c_str(), ptv);

    select(this);
}

DBConnector::DBConnector(const string& dbName, unsigned int timeout_ms, bool isTcpConn, const string& netns)
    : DBConnector(dbName, timeout_ms, isTcpConn, SonicDBKey(netns))
{
}

DBConnector::DBConnector(const string& dbName, unsigned int timeout_ms, bool isTcpConn, const SonicDBKey &key)
    : m_dbId(SonicDBConfig::getDbId(dbName, key))
    , m_dbName(dbName)
    , m_key(key)
{
    struct timeval tv = ms_to_timeval(timeout_ms);
    struct timeval *ptv = timeout_ms ? &tv : NULL;
    if (isTcpConn)
    {
        initContext(SonicDBConfig::getDbHostname(dbName, m_key).c_str(), SonicDBConfig::getDbPort(dbName, m_key), ptv);
    }
    else
    {
        initContext(SonicDBConfig::getDbSock(dbName, m_key).c_str(), ptv);
    }

    select(this);
}

DBConnector::DBConnector(const string& dbName, unsigned int timeout_ms, bool isTcpConn)
    : DBConnector(dbName, timeout_ms, isTcpConn, SonicDBKey())
{
    // Empty constructor
}

int DBConnector::getDbId() const
{
    return m_dbId;
}

string DBConnector::getDbName() const
{
    return m_dbName;
}

void DBConnector::setNamespace(const string& netns)
{
    m_key.netns = netns;
}

string DBConnector::getNamespace() const
{
    return m_key.netns;
}

void DBConnector::setDBKey(const SonicDBKey &key)
{
    m_key = key;
}

SonicDBKey DBConnector::getDBKey() const
{
    return m_key;
}

DBConnector *DBConnector::newConnector(unsigned int timeout) const
{
    DBConnector *ret;

    if (getContext()->connection_type == REDIS_CONN_TCP)
        ret = new DBConnector(getDbId(),
                               getContext()->tcp.host,
                               getContext()->tcp.port,
                               timeout);
    else
        ret = new DBConnector(getDbId(),
                               getContext()->unix_sock.path,
                               timeout);

    ret->m_dbName = m_dbName;
    ret->setDBKey(getDBKey());

    return ret;
}

PubSub *DBConnector::pubsub()
{
    return new PubSub(this);
}

int64_t DBConnector::del(const string &key)
{
    RedisCommand sdel;
    sdel.format("DEL %s", key.c_str());
    RedisReply r(this, sdel, REDIS_REPLY_INTEGER);
    return r.getContext()->integer;
}

bool DBConnector::exists(const string &key)
{
    RedisCommand rexists;
    if (key.find_first_of(" \t") != string::npos)
    {
        SWSS_LOG_ERROR("EXISTS failed, invalid space or tab in single key: %s", key.c_str());
        throw runtime_error("EXISTS failed, invalid space or tab in single key");
    }
    rexists.format("EXISTS %s", key.c_str());
    RedisReply r(this, rexists, REDIS_REPLY_INTEGER);
    return r.getContext()->integer > 0;
}

int64_t DBConnector::hdel(const string &key, const string &field)
{
    RedisCommand shdel;
    shdel.format("HDEL %s %s", key.c_str(), field.c_str());
    RedisReply r(this, shdel, REDIS_REPLY_INTEGER);
    return r.getContext()->integer;
}

int64_t DBConnector::hdel(const std::string &key, const std::vector<std::string> &fields)
{
    RedisCommand shdel;
    shdel.formatHDEL(key, fields);
    RedisReply r(this, shdel, REDIS_REPLY_INTEGER);
    return r.getContext()->integer;
}

void DBConnector::hset(const string &key, const string &field, const string &value)
{
    RedisCommand shset;
    shset.format("HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
    RedisReply r(this, shset, REDIS_REPLY_INTEGER);
}

bool DBConnector::set(const string &key, const string &value)
{
    RedisCommand sset;
    sset.format("SET %s %s", key.c_str(), value.c_str());
    RedisReply r(this, sset, REDIS_REPLY_STATUS);
    string s = r.getReply<string>();
    return s == "OK";
}

bool DBConnector::set(const string &key, int value)
{
    return set(key, to_string(value));
}

void DBConnector::config_set(const std::string &key, const std::string &value)
{
    RedisCommand sset;
    sset.format("CONFIG SET %s %s", key.c_str(), value.c_str());
    RedisReply r(this, sset, REDIS_REPLY_STATUS);
}

bool DBConnector::flushdb()
{
    RedisCommand sflushdb;
    sflushdb.format("FLUSHDB");
    RedisReply r(this, sflushdb, REDIS_REPLY_STATUS);
    string s = r.getReply<string>();
    return s == "OK";
}

vector<string> DBConnector::keys(const string &key)
{
    RedisCommand skeys;
    skeys.format("KEYS %s", key.c_str());
    RedisReply r(this, skeys, REDIS_REPLY_ARRAY);

    auto ctx = r.getContext();

    vector<string> list;
    for (unsigned int i = 0; i < ctx->elements; i++)
        list.emplace_back(ctx->element[i]->str);

    return list;
}

pair<int, vector<string>> DBConnector::scan(int cursor, const char *match, uint32_t count)
{
    RedisCommand sscan;
    sscan.format("SCAN %d MATCH %s COUNT %u", cursor, match, count);
    RedisReply r(this, sscan, REDIS_REPLY_ARRAY);

    RedisReply r0(r.releaseChild(0));
    r0.checkReplyType(REDIS_REPLY_STRING);
    RedisReply r1(r.releaseChild(1));
    r1.checkReplyType(REDIS_REPLY_ARRAY);

    pair<int64_t, vector<string>> ret;
    string cur = r0.getReply<string>();
    try
    {
        ret.first = stoi(cur);
    }
    catch(logic_error& ex)
    {
        throw system_error(make_error_code(errc::io_error), "Invalid cursor string returned by scan: " + cur);
    }
    for (size_t i = 0; i < r1.getChildCount(); i++)
    {
        RedisReply r11(r1.releaseChild(i));
        ret.second.emplace_back(r11.getReply<string>());
    }
    return ret;
}

int64_t DBConnector::incr(const string &key)
{
    RedisCommand sincr;
    sincr.format("INCR %s", key.c_str());
    RedisReply r(this, sincr, REDIS_REPLY_INTEGER);
    return r.getContext()->integer;
}

int64_t DBConnector::decr(const string &key)
{
    RedisCommand sdecr;
    sdecr.format("DECR %s", key.c_str());
    RedisReply r(this, sdecr, REDIS_REPLY_INTEGER);
    return r.getContext()->integer;
}

shared_ptr<string> DBConnector::get(const string &key)
{
    RedisCommand sget;
    sget.format("GET %s", key.c_str());
    RedisReply r(this, sget);
    auto reply = r.getContext();

    if (reply->type == REDIS_REPLY_NIL)
    {
        return shared_ptr<string>(NULL);
    }

    if (reply->type == REDIS_REPLY_STRING)
    {
        shared_ptr<string> ptr(new string(reply->str));
        return ptr;
    }

    throw runtime_error("GET failed, memory exception");
}

shared_ptr<string> DBConnector::hget(const string &key, const string &field)
{
    RedisCommand shget;
    shget.format("HGET %s %s", key.c_str(), field.c_str());
    RedisReply r(this, shget);
    auto reply = r.getContext();

    if (reply->type == REDIS_REPLY_NIL)
    {
        return shared_ptr<string>(NULL);
    }

    if (reply->type == REDIS_REPLY_STRING)
    {
        shared_ptr<string> ptr(new string(reply->str));
        return ptr;
    }

    SWSS_LOG_ERROR("HGET failed, reply-type: %d, %s: %s", reply->type, key.c_str(), field.c_str());
    throw runtime_error("HGET failed, unexpected reply type, memory exception");
}

bool DBConnector::hexists(const string &key, const string &field)
{
    RedisCommand rexists;
    rexists.format("HEXISTS %s %s", key.c_str(), field.c_str());
    RedisReply r(this, rexists, REDIS_REPLY_INTEGER);
    return r.getContext()->integer > 0;
}

int64_t DBConnector::rpush(const string &list, const string &item)
{
    RedisCommand srpush;
    srpush.format("RPUSH %s %s", list.c_str(), item.c_str());
    RedisReply r(this, srpush, REDIS_REPLY_INTEGER);
    return r.getContext()->integer;
}

shared_ptr<string> DBConnector::blpop(const string &list, int timeout)
{
    RedisCommand sblpop;
    sblpop.format("BLPOP %s %d", list.c_str(), timeout);
    RedisReply r(this, sblpop);
    auto reply = r.getContext();

    if (reply->type == REDIS_REPLY_NIL)
    {
        return shared_ptr<string>(NULL);
    }

    if (reply->type == REDIS_REPLY_STRING)
    {
        shared_ptr<string> ptr(new string(reply->str));
        return ptr;
    }

    throw runtime_error("GET failed, memory exception");
}

void DBConnector::subscribe(const std::string &pattern)
{
    std::string s("SUBSCRIBE ");
    s += pattern;
    RedisReply r(this, s, REDIS_REPLY_ARRAY);
}

void DBConnector::psubscribe(const std::string &pattern)
{
    std::string s("PSUBSCRIBE ");
    s += pattern;
    RedisReply r(this, s, REDIS_REPLY_ARRAY);
}

void DBConnector::punsubscribe(const std::string &pattern)
{
    std::string s("PUNSUBSCRIBE ");
    s += pattern;
    RedisReply r(this, s, REDIS_REPLY_ARRAY);
}

int64_t DBConnector::publish(const string &channel, const string &message)
{
    RedisCommand publish;
    publish.format("PUBLISH %s %s", channel.c_str(), message.c_str());
    RedisReply r(this, publish, REDIS_REPLY_INTEGER);
    return r.getReply<long long int>();
}

void DBConnector::hmset(const std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>>& multiHash)
{
    SWSS_LOG_ENTER();

    RedisPipeline pipe(this);
    for (auto& hash : multiHash)
    {
        RedisCommand hset;
        hset.formatHSET(hash.first, hash.second.begin(), hash.second.end());
        pipe.push(hset, REDIS_REPLY_INTEGER);
    }

    pipe.flush();
}

void DBConnector::del(const std::vector<std::string>& keys)
{
    SWSS_LOG_ENTER();
    RedisPipeline pipe(this);

    std::vector<std::string> batched_keys;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        batched_keys.push_back(keys[i]);
        if (((i + 1) % KEY_DEL_CHUNK_SIZE == 0) || (i == keys.size()-1)) {
            RedisCommand del;
            del.formatDEL(batched_keys);
            pipe.push(del, REDIS_REPLY_INTEGER);
            batched_keys.clear();
        }
    }
    pipe.flush();
}

map<string, map<string, map<string, string>>> DBConnector::getall()
{
    const string separator = SonicDBConfig::getSeparator(this);
    auto const& keys = this->keys("*");
    map<string, map<string, map<string, string>>> data;
    for (string key: keys)
    {
        size_t pos = key.find(separator);
        if (pos == string::npos) {
            continue;
        }
        string table_name = key.substr(0, pos);
        string row = key.substr(pos + 1);
        auto const& entry = this->hgetall<map<string, string>>(key);

        if (!entry.empty())
        {
            data[table_name][row] = entry;
        }
    }
    return data;
}
