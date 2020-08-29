#ifndef __DBCONNECTOR__
#define __DBCONNECTOR__

#include <string>
#include <vector>
#include <unordered_map>
#include <utility>
#include <memory>

#include <hiredis/hiredis.h>
#include "rediscommand.h"
#include "redisreply.h"
#define EMPTY_NAMESPACE std::string()

namespace swss {

class DBConnector;

class RedisInstInfo
{
public:
    std::string unixSocketPath;
    std::string hostname;
    int port;
};

class SonicDBInfo
{
public:
    std::string instName;
    int dbId;
    std::string separator;
};

class SonicDBConfig
{
public:
    static void initialize(const std::string &file = DEFAULT_SONIC_DB_CONFIG_FILE);
    static void initializeGlobalConfig(const std::string &file = DEFAULT_SONIC_DB_GLOBAL_CONFIG_FILE);
    static void validateNamespace(const std::string &netns);
    static std::string getDbInst(const std::string &dbName, const std::string &netns = EMPTY_NAMESPACE);
    static int getDbId(const std::string &dbName, const std::string &netns = EMPTY_NAMESPACE);
    static std::string getSeparator(const std::string &dbName, const std::string &netns = EMPTY_NAMESPACE);
    static std::string getSeparator(int dbId, const std::string &netns = EMPTY_NAMESPACE);
    static std::string getSeparator(const DBConnector* db);
    static std::string getDbSock(const std::string &dbName, const std::string &netns = EMPTY_NAMESPACE);
    static std::string getDbHostname(const std::string &dbName, const std::string &netns = EMPTY_NAMESPACE);
    static int getDbPort(const std::string &dbName, const std::string &netns = EMPTY_NAMESPACE);
    static std::vector<std::string> getNamespaces();
    static bool isInit() { return m_init; };
    static bool isGlobalInit() { return m_global_init; };

private:
    static constexpr const char *DEFAULT_SONIC_DB_CONFIG_FILE = "/var/run/redis/sonic-db/database_config.json";
    static constexpr const char *DEFAULT_SONIC_DB_GLOBAL_CONFIG_FILE = "/var/run/redis/sonic-db/database_global.json";
    // { namespace { instName, { unix_socket_path, hostname, port } } }
    static std::unordered_map<std::string, std::unordered_map<std::string, RedisInstInfo>> m_inst_info;
    // { namespace, { dbName, {instName, dbId, separator} } }
    static std::unordered_map<std::string, std::unordered_map<std::string, SonicDBInfo>> m_db_info;
    // { namespace, { dbId, separator } }
    static std::unordered_map<std::string, std::unordered_map<int, std::string>> m_db_separator;
    static bool m_init;
    static bool m_global_init;
    static void parseDatabaseConfig(const std::string &file,
                                    std::unordered_map<std::string, RedisInstInfo> &inst_entry,
                                    std::unordered_map<std::string, SonicDBInfo> &db_entry,
                                    std::unordered_map<int, std::string> &separator_entry);
    static SonicDBInfo& getDbInfo(const std::string &dbName, const std::string &netns = EMPTY_NAMESPACE);
    static RedisInstInfo& getRedisInfo(const std::string &dbName, const std::string &netns = EMPTY_NAMESPACE);
};

class RedisConnector
{
public:
    static constexpr const char *DEFAULT_UNIXSOCKET = "/var/run/redis/redis.sock";

    /*
     * Connect to Redis DB wither with a hostname:port or unix socket
     * Select the database index provided by "db"
     *
     * Timeout - The time in milisecond until exception is been thrown. For
     *           infinite wait, set this value to 0
     */
    RedisConnector(const std::string &hostname, int port, unsigned int timeout);
    RedisConnector(const std::string &unixPath, unsigned int timeout);

    ~RedisConnector();

    redisContext *getContext() const;
    std::string getNamespace() const;

    /*
     * Assign a name to the Redis client used for this connection
     * This is helpful when debugging Redis clients using `redis-cli client list`
     */
    void setClientName(const std::string& clientName);

    std::string getClientName();

    int64_t del(const std::string &key);

    bool exists(const std::string &key);

    int64_t hdel(const std::string &key, const std::string &field);

    int64_t hdel(const std::string &key, const std::vector<std::string> &fields);

    std::unordered_map<std::string, std::string> hgetall(const std::string &key);

    template <typename OutputIterator>
    void hgetall(const std::string &key, OutputIterator result);

    std::vector<std::string> keys(const std::string &key);

    void set(const std::string &key, const std::string &value);

    void hset(const std::string &key, const std::string &field, const std::string &value);

    template<typename InputIterator>
    void hmset(const std::string &key, InputIterator start, InputIterator stop);

    std::shared_ptr<std::string> get(const std::string &key);

    std::shared_ptr<std::string> hget(const std::string &key, const std::string &field);

    int64_t incr(const std::string &key);

    int64_t decr(const std::string &key);

    int64_t rpush(const std::string &list, const std::string &item);

    std::shared_ptr<std::string> blpop(const std::string &list, int timeout);

protected:
    RedisConnector();
    void setContext(redisContext *conn);
    void setNamespace(const std::string &netns);

private:
    redisContext *m_conn;
    std::string m_namespace;
};

class DBConnector : public RedisConnector
{
public:
    static constexpr const char *DEFAULT_UNIXSOCKET = "/var/run/redis/redis.sock";

    /*
     * Connect to Redis DB wither with a hostname:port or unix socket
     * Select the database index provided by "db"
     *
     * Timeout - The time in milisecond until exception is been thrown. For
     *           infinite wait, set this value to 0
     */
    DBConnector(int dbId, const std::string &hostname, int port, unsigned int timeout);
    DBConnector(int dbId, const std::string &unixPath, unsigned int timeout);
    DBConnector(const std::string &dbName, unsigned int timeout, bool isTcpConn = false);
    DBConnector(const std::string &dbName, unsigned int timeout, bool isTcpConn, const std::string &netns);

    int getDbId() const;
    std::string getDbName() const;

    static void select(DBConnector *db);

    /* Create new context to DB */
    DBConnector *newConnector(unsigned int timeout) const;

private:
    int m_dbId;
    std::string m_dbName;
};

template<typename OutputIterator>
void DBConnector::hgetall(const std::string &key, OutputIterator result)
{
    RedisCommand sincr;
    sincr.format("HGETALL %s", key.c_str());
    RedisReply r(this, sincr, REDIS_REPLY_ARRAY);

    auto ctx = r.getContext();

    for (unsigned int i = 0; i < ctx->elements; i += 2)
    {
        *result = std::make_pair(ctx->element[i]->str, ctx->element[i+1]->str);
        ++result;
    }
}

template<typename InputIterator>
void DBConnector::hmset(const std::string &key, InputIterator start, InputIterator stop)
{
    RedisCommand shmset;
    shmset.formatHMSET(key, start, stop);
    RedisReply r(this, shmset, REDIS_REPLY_STATUS);
}

}
#endif
