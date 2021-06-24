#include <string.h>
#include <stdint.h>
#include <vector>
#include <unistd.h>
#include <errno.h>
#include <system_error>
#include <fstream>
#include "json.hpp"
#include "logger.h"

#include "common/dbconnector.h"
#include "common/redisreply.h"
#include "common/redisapi.h"
#include "common/pubsub.h"

using json = nlohmann::json;
using namespace std;
using namespace swss;

void SonicDBConfig::parseDatabaseConfig(const string &file,
                    std::unordered_map<std::string, RedisInstInfo> &inst_entry,
                    std::unordered_map<std::string, SonicDBInfo> &db_entry,
                    std::unordered_map<int, std::string> &separator_entry)
{
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
               string socket = it.value().at("unix_socket_path");
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

void SonicDBConfig::initializeGlobalConfig(const string &file)
{
    std::string local_file, dir_name, ns_name;
    std::unordered_map<std::string, SonicDBInfo> db_entry;
    std::unordered_map<std::string, RedisInstInfo> inst_entry;
    std::unordered_map<int, std::string> separator_entry;
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
        local_file = dir_name = std::string();

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
                local_file.append(dir_name);
                local_file.append(element["include"]);

                if(element["namespace"].empty())
                {
                    // If database_config.json is already initlized via SonicDBConfig::initialize
                    // skip initializing it here again.
                    if(m_init)
                    {
                        local_file.clear();
                        continue;
                    }
                    ns_name = EMPTY_NAMESPACE;
                }
                else
                {
                    ns_name = element["namespace"];
                }

                parseDatabaseConfig(local_file, inst_entry, db_entry, separator_entry);
                m_inst_info[ns_name] = inst_entry;
                m_db_info[ns_name] = db_entry;
                m_db_separator[ns_name] = separator_entry;

                if(element["namespace"].empty())
                {
                    // Make regular init also done
                    m_init = true;
                }

                inst_entry.clear();
                db_entry.clear();
                separator_entry.clear();
                local_file.clear();
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
    std::unordered_map<std::string, RedisInstInfo> inst_entry;
    std::unordered_map<int, std::string> separator_entry;
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    SWSS_LOG_ENTER();

    if (m_init)
    {
        SWSS_LOG_ERROR("SonicDBConfig already initialized");
        throw runtime_error("SonicDBConfig already initialized");
    }

    parseDatabaseConfig(file, inst_entry, db_entry, separator_entry);
    m_inst_info[EMPTY_NAMESPACE] = inst_entry;
    m_db_info[EMPTY_NAMESPACE] = db_entry;
    m_db_separator[EMPTY_NAMESPACE] = separator_entry;

    // Set it as the config file is already parsed and init done.
    m_init = true;
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
        unordered_map<string, unordered_map<string, RedisInstInfo>>::const_iterator entry = m_inst_info.find(netns);
        if (entry == m_inst_info.end())
        {
            SWSS_LOG_THROW("Namespace %s is not a valid namespace name in config file", netns.c_str());
        }
    }
}

SonicDBInfo& SonicDBConfig::getDbInfo(const std::string &dbName, const std::string &netns)
{
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    SWSS_LOG_ENTER();

    if (!m_init)
        initialize(DEFAULT_SONIC_DB_CONFIG_FILE);

    if (!netns.empty())
    {
        if (!m_global_init)
        {
            SWSS_LOG_THROW("Initialize global DB config using API SonicDBConfig::initializeGlobalConfig");
        }
    }
    auto foundNetns = m_db_info.find(netns);
    if (foundNetns == m_db_info.end())
    {
        string msg = "Namespace " + netns + " is not a valid namespace name in config file";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    auto& infos = foundNetns->second;
    auto foundDb = infos.find(dbName);
    if (foundDb == infos.end())
    {
        string msg = "Failed to find " + dbName + " database in " + netns + " namespace";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    return foundDb->second;
}

RedisInstInfo& SonicDBConfig::getRedisInfo(const std::string &dbName, const std::string &netns)
{
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    SWSS_LOG_ENTER();

    if (!m_init)
        initialize(DEFAULT_SONIC_DB_CONFIG_FILE);

    if (!netns.empty())
    {
        if (!m_global_init)
        {
            SWSS_LOG_THROW("Initialize global DB config using API SonicDBConfig::initializeGlobalConfig");
        }
    }
    auto foundNetns = m_inst_info.find(netns);
    if (foundNetns == m_inst_info.end())
    {
        string msg = "Namespace " + netns + " is not a valid namespace name in Redis instances in config file";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    auto& redisInfos = foundNetns->second;
    auto foundRedis = redisInfos.find(getDbInst(dbName, netns));
    if (foundRedis == redisInfos.end())
    {
        string msg = "Failed to find the Redis instance for " + dbName + " database in " + netns + " namespace";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    return foundRedis->second;
}

string SonicDBConfig::getDbInst(const string &dbName, const string &netns)
{
    return getDbInfo(dbName, netns).instName;
}

int SonicDBConfig::getDbId(const string &dbName, const string &netns)
{
    return getDbInfo(dbName, netns).dbId;
}

string SonicDBConfig::getSeparator(const string &dbName, const string &netns)
{
    return getDbInfo(dbName, netns).separator;
}

string SonicDBConfig::getSeparator(int dbId, const string &netns)
{
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    if (!m_init)
        initialize(DEFAULT_SONIC_DB_CONFIG_FILE);

    if (!netns.empty())
    {
        if (!m_global_init)
        {
            SWSS_LOG_THROW("Initialize global DB config using API SonicDBConfig::initializeGlobalConfig");
        }
    }
    auto foundNetns = m_db_separator.find(netns);
    if (foundNetns == m_db_separator.end())
    {
        string msg = "Namespace " + netns + " is not a valid namespace name in config file";
        SWSS_LOG_ERROR("%s", msg.c_str());
        throw out_of_range(msg);
    }
    auto seps = foundNetns->second;
    auto foundDb = seps.find(dbId);
    if (foundDb == seps.end())
    {
        string msg = "Failed to find " + to_string(dbId) + " database in " + netns + " namespace";
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
    string netns = db->getNamespace();
    if (dbName.empty())
    {
        return getSeparator(db->getDbId(), netns);
    }
    else
    {
        return getSeparator(dbName, netns);
    }
}

string SonicDBConfig::getDbSock(const string &dbName, const string &netns)
{
    return getRedisInfo(dbName, netns).unixSocketPath;
}

string SonicDBConfig::getDbHostname(const string &dbName, const string &netns)
{
    return getRedisInfo(dbName, netns).hostname;
}

int SonicDBConfig::getDbPort(const string &dbName, const string &netns)
{
    return getRedisInfo(dbName, netns).port;
}

vector<string> SonicDBConfig::getNamespaces()
{
    vector<string> list;
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);

    if (!m_init)
        initialize(DEFAULT_SONIC_DB_CONFIG_FILE);

    // This API returns back all namespaces including '' representing global ns.
    for (auto it = m_inst_info.cbegin(); it != m_inst_info.cend(); ++it) {
        list.push_back(it->first);
    }

    return list;
}

std::vector<std::string> SonicDBConfig::getDbList(const std::string &netns)
{
    std::lock_guard<std::recursive_mutex> guard(m_db_info_mutex);
    if (!m_init)
    {
        initialize();
    }
    validateNamespace(netns);

    std::vector<std::string> dbNames;
    for (auto& imap: m_db_info.at(netns))
    {
        dbNames.push_back(imap.first);
    }
    return dbNames;
}

constexpr const char *SonicDBConfig::DEFAULT_SONIC_DB_CONFIG_FILE;
constexpr const char *SonicDBConfig::DEFAULT_SONIC_DB_GLOBAL_CONFIG_FILE;
std::recursive_mutex SonicDBConfig::m_db_info_mutex;
unordered_map<string, unordered_map<string, RedisInstInfo>> SonicDBConfig::m_inst_info;
unordered_map<string, unordered_map<string, SonicDBInfo>> SonicDBConfig::m_db_info;
unordered_map<string, unordered_map<int, string>> SonicDBConfig::m_db_separator;
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
        initContext(unixPath, octx->timeout);
    }
    else
    {
        initContext(octx->tcp.host, octx->tcp.port, octx->timeout);
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
                           "Unable to connect to redis");
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
                           "Unable to connect to redis (unix-socket)");
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
    , m_namespace(other.m_namespace)
{
    select(this);
}

DBConnector::DBConnector(int dbId, const RedisContext& ctx)
    : RedisContext(ctx)
    , m_dbId(dbId)
    , m_namespace(EMPTY_NAMESPACE)
{
    select(this);
}

DBConnector::DBConnector(int dbId, const string& hostname, int port,
                         unsigned int timeout)
    : m_dbId(dbId)
    , m_namespace(EMPTY_NAMESPACE)
{
    struct timeval tv = {0, (suseconds_t)timeout * 1000};
    struct timeval *ptv = timeout ? &tv : NULL;
    initContext(hostname.c_str(), port, ptv);

    select(this);
}

DBConnector::DBConnector(int dbId, const string& unixPath, unsigned int timeout)
    : m_dbId(dbId)
    , m_namespace(EMPTY_NAMESPACE)
{
    struct timeval tv = {0, (suseconds_t)timeout * 1000};
    struct timeval *ptv = timeout ? &tv : NULL;
    initContext(unixPath.c_str(), ptv);

    select(this);
}

DBConnector::DBConnector(const string& dbName, unsigned int timeout, bool isTcpConn, const string& netns)
    : m_dbId(SonicDBConfig::getDbId(dbName, netns))
    , m_dbName(dbName)
    , m_namespace(netns)
{
    struct timeval tv = {0, (suseconds_t)timeout * 1000};
    struct timeval *ptv = timeout ? &tv : NULL;
    if (isTcpConn)
    {
        initContext(SonicDBConfig::getDbHostname(dbName, netns).c_str(), SonicDBConfig::getDbPort(dbName, netns), ptv);
    }
    else
    {
        initContext(SonicDBConfig::getDbSock(dbName, netns).c_str(), ptv);
    }

    select(this);
}

DBConnector::DBConnector(const string& dbName, unsigned int timeout, bool isTcpConn)
    : DBConnector(dbName, timeout, isTcpConn, EMPTY_NAMESPACE)
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
    m_namespace = netns;
}

string DBConnector::getNamespace() const
{
    return m_namespace;
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
    ret->setNamespace(getNamespace());

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

    // make sure this will be object (not null) when multi hash is empty
    json j = json::object();

    // pack multi hash to json (takes bout 70 ms for 10k to construct)
    for (const auto& kvp: multiHash)
    {
        json o;

        for (const auto &item: kvp.second)
        {
            o[std::get<0>(item)] = std::get<1>(item);
        }

        j[kvp.first] = o;
    }

    std::string strJson = j.dump();

    lazyLoadRedisScriptFile(this, "redis_multi.lua", m_shaRedisMulti);
    RedisCommand command;
    command.format(
        "EVALSHA %s 1 %s %s",
        m_shaRedisMulti.c_str(),
        strJson.c_str(),
        "mhset");

    RedisReply r(this, command, REDIS_REPLY_NIL);
}

void DBConnector::del(const std::vector<std::string>& keys)
{
    SWSS_LOG_ENTER();

    json j = json::array();

    for (const auto& key: keys)
    {
        j.push_back(key);
    }

    std::string strJson = j.dump();

    lazyLoadRedisScriptFile(this, "redis_multi.lua", m_shaRedisMulti);
    RedisCommand command;
    command.format(
        "EVALSHA %s 1 %s %s",
        m_shaRedisMulti.c_str(),
        strJson.c_str(),
        "mdel");

    RedisReply r(this, command, REDIS_REPLY_NIL);
}

/*******************************************************************************
    / \   ___ _   _ _ __   ___
   / _ \ / __| | | | '_ \ / __|
  / ___ \\__ \ |_| | | | | (__
 /_/   \_\___/\__, |_| |_|\___|
              |___/
*******************************************************************************/
DBConnector_async::DBConnector_async(const std::string  &dbName,
                                     void               *userCtxPtr) :
    m_dbName(dbName),
    m_dbId(SonicDBConfig::getDbId(m_dbName)),
    m_sockAddr(SonicDBConfig::getDbSock(m_dbName)),
    m_userCtxPtr(userCtxPtr)
{
    m_acPtr = redisAsyncConnectUnix(m_sockAddr.c_str());

    if (m_acPtr->err != 0)
    {
        std::string errmsg("Unable to connect to redis: (" + std::to_string(m_acPtr->err) + ')');

        if ((m_acPtr->errstr != nullptr) && (m_acPtr->errstr[0] != '\0'))
             errmsg += " " + std::string(m_acPtr->errstr);

        redisAsyncFree(m_acPtr);
        m_acPtr = nullptr;

        throw std::system_error(std::make_error_code(errc::address_not_available), errmsg);
    }

    m_acPtr->data = this;

    command(nullptr, nullptr, "SELECT %d", m_dbId);
}

DBConnector_async::~DBConnector_async()
{
    if (m_acPtr != nullptr)
    {
        // We can't use redisAsyncFree() here because there may
        // be pending messages to be sent or received. redisAsyncDisconnect()
        // will ensure that all pending messages are processed before the
        // context gets deleted.
        redisAsyncDisconnect(m_acPtr);
        m_acPtr = nullptr;
    }
}

int DBConnector_async::command(redisCallbackFn *cb_func_p, void *cb_data_p, const char *format_p, ...)
{
    va_list ap;
    int status;
    va_start(ap, format_p);
    status = redisvAsyncCommand(m_acPtr, cb_func_p, cb_data_p, format_p, ap);
    va_end(ap);
    return status;
}

int DBConnector_async::formatted_command(redisCallbackFn *cb_func_p, void *cb_data_p, const char *cmd_p, size_t len)
{
    return redisAsyncFormattedCommand(m_acPtr, cb_func_p, cb_data_p, cmd_p, len);
}