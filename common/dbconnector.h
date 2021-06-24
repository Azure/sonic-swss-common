#ifndef __DBCONNECTOR__
#define __DBCONNECTOR__

#include <string>
#include <vector>
#include <unordered_map>
#include <utility>
#include <memory>
#include <mutex>

#include <hiredis/hiredis.h>
#include <hiredis/async.h>      // redisAsyncContext
#include "rediscommand.h"
#include "redisreply.h"
#define EMPTY_NAMESPACE std::string()

namespace swss {

class DBConnector;
class PubSub;

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
    static constexpr const char *DEFAULT_SONIC_DB_CONFIG_FILE = "/var/run/redis/sonic-db/database_config.json";
    static constexpr const char *DEFAULT_SONIC_DB_GLOBAL_CONFIG_FILE = "/var/run/redis/sonic-db/database_global.json";
    static void initialize(const std::string &file = DEFAULT_SONIC_DB_CONFIG_FILE);
#ifdef SWIG
    %pythoncode %{
        ## TODO: the python function and C++ one is not on-par
        @staticmethod
        def load_sonic_db_config(sonic_db_file_path=DEFAULT_SONIC_DB_CONFIG_FILE):
            SonicDBConfig.initialize(sonic_db_file_path)
    %}
#endif

    static void initializeGlobalConfig(const std::string &file = DEFAULT_SONIC_DB_GLOBAL_CONFIG_FILE);
#ifdef SWIG
    %pythoncode %{
        ## TODO: the python function and C++ one is not on-par
        @staticmethod
        def load_sonic_global_db_config(global_db_file_path=DEFAULT_SONIC_DB_GLOBAL_CONFIG_FILE, namespace=None):
            SonicDBConfig.initializeGlobalConfig(global_db_file_path)
    %}
#endif

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
#ifdef SWIG
    %pythoncode %{
        ## TODO: the python function and C++ one is not on-par
        @staticmethod
        def get_ns_list():
            return SonicDBConfig.getNamespaces()
    %}
#endif

    static std::vector<std::string> getDbList(const std::string &netns = EMPTY_NAMESPACE);
    static bool isInit() { return m_init; };
    static bool isGlobalInit() { return m_global_init; };

private:
    static std::recursive_mutex m_db_info_mutex;
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

class RedisContext
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
    RedisContext(const RedisContext &other);
    RedisContext& operator=(const RedisContext&) = delete;

    ~RedisContext();

    redisContext *getContext() const;

    /*
     * Assign a name to the Redis client used for this connection
     * This is helpful when debugging Redis clients using `redis-cli client list`
     */
    void setClientName(const std::string& clientName);

    std::string getClientName();

protected:
    RedisContext();
    void initContext(const char *host, int port, const timeval *tv);
    void initContext(const char *path, const timeval *tv);
    void setContext(redisContext *ctx);

private:
    redisContext *m_conn;
};

class DBConnector : public RedisContext
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
    explicit DBConnector(const DBConnector &other);
    DBConnector(int dbId, const RedisContext &ctx);
    DBConnector(int dbId, const std::string &hostname, int port, unsigned int timeout);
    DBConnector(int dbId, const std::string &unixPath, unsigned int timeout);
    DBConnector(const std::string &dbName, unsigned int timeout, bool isTcpConn = false);
    DBConnector(const std::string &dbName, unsigned int timeout, bool isTcpConn, const std::string &netns);
    DBConnector& operator=(const DBConnector&) = delete;

    int getDbId() const;
    std::string getDbName() const;
    std::string getNamespace() const;

#ifdef SWIG
    %pythoncode %{
        __swig_getmethods__["namespace"] = getNamespace
        __swig_setmethods__["namespace"] = None
        if _newclass: namespace = property(getNamespace, None)
    %}
#endif

    static void select(DBConnector *db);

    /* Create new context to DB */
    DBConnector *newConnector(unsigned int timeout) const;

    PubSub *pubsub();

    int64_t del(const std::string &key);

#ifdef SWIG
    // SWIG interface file (.i) globally rename map C++ `del` to python `delete`,
    // but applications already followed the old behavior of auto renamed `_del`.
    // So we implemented old behavior for backward compatibility
    // TODO: remove this function after applications use the function name `delete`
    %pythoncode %{
        def _del(self, *args, **kwargs):
            return self.delete(*args, **kwargs)
    %}
#endif

    bool exists(const std::string &key);

    int64_t hdel(const std::string &key, const std::string &field);

    int64_t hdel(const std::string &key, const std::vector<std::string> &fields);

    void del(const std::vector<std::string>& keys);

    template <typename ReturnType=std::unordered_map<std::string, std::string>>
    ReturnType hgetall(const std::string &key);

#ifndef SWIG
    template <typename OutputIterator>
    void hgetall(const std::string &key, OutputIterator result);
#endif

    std::vector<std::string> keys(const std::string &key);

    std::pair<int, std::vector<std::string>> scan(int cursor = 0, const char *match = "", uint32_t count = 10);

    bool set(const std::string &key, const std::string &value);
    bool set(const std::string &key, int value);

    void hset(const std::string &key, const std::string &field, const std::string &value);

    template<typename InputIterator>
    void hmset(const std::string &key, InputIterator start, InputIterator stop);

    void hmset(const std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>>& multiHash);

    std::shared_ptr<std::string> get(const std::string &key);

    std::shared_ptr<std::string> hget(const std::string &key, const std::string &field);

    bool hexists(const std::string &key, const std::string &field);

    int64_t incr(const std::string &key);

    int64_t decr(const std::string &key);

    int64_t rpush(const std::string &list, const std::string &item);

    std::shared_ptr<std::string> blpop(const std::string &list, int timeout);

    void subscribe(const std::string &pattern);

    void psubscribe(const std::string &pattern);

    void punsubscribe(const std::string &pattern);

    int64_t publish(const std::string &channel, const std::string &message);

    void config_set(const std::string &key, const std::string &value);

    bool flushdb();

private:
    void setNamespace(const std::string &netns);

    int m_dbId;
    std::string m_dbName;
    std::string m_namespace;

    std::string m_shaRedisMulti;
};

template <typename ReturnType>
ReturnType DBConnector::hgetall(const std::string &key)
{
    ReturnType map;
    hgetall(key, std::inserter(map, map.end()));
    return map;
}

#ifndef SWIG
template<typename OutputIterator>
void DBConnector::hgetall(const std::string &key, OutputIterator result)
{
    RedisCommand shgetall;
    shgetall.format("HGETALL %s", key.c_str());
    RedisReply r(this, shgetall, REDIS_REPLY_ARRAY);

    auto ctx = r.getContext();

    for (unsigned int i = 0; i < ctx->elements; i += 2)
    {
        *result = std::make_pair(ctx->element[i]->str, ctx->element[i+1]->str);
        ++result;
    }
}
#endif

template<typename InputIterator>
void DBConnector::hmset(const std::string &key, InputIterator start, InputIterator stop)
{
    RedisCommand shmset;
    shmset.formatHMSET(key, start, stop);
    RedisReply r(this, shmset, REDIS_REPLY_STATUS);
}



/*******************************************************************************
    / \   ___ _   _ _ __   ___
   / _ \ / __| | | | '_ \ / __|
  / ___ \\__ \ |_| | | | | (__
 /_/   \_\___/\__, |_| |_|\___|
              |___/
*******************************************************************************/

/**
 * @brief   This uses hiredis asynchronous APIs (hiredis/async.h) to connect
 *          to the REDIS server.
 *
 * @details Asyncronous architecture means that the application is designed
 *          around an event loop that calls callback functions for each event.
 *          The hiredis library provides a number of adapters for 3rd party
 *          event loop libraries such as GLib, libevent, qt, etc.
 *          Ref: <a href="https://github.com/redis/hiredis/tree/master/adapters">hiredis/adapters/[*.h]</a>
 *
 *          No additional thread is created when using the async hiredis APIs.
 *          The hiredis context is simply "hooked up" to the event loop and
 *          that allows the event loop to dispatch the "events" (i.e. replies
 *          from the REDIS server) to their corresponding callback functions.
 */
class DBConnector_async
{
public:
    /**
     * @brief Asynchronous Connector object to the REDIS server.
     *
     * @param p_dbName Name of the DB we want to connect to (e.g. CONFIG_DB,
     *                  APPL_DB, ...)
     *
     * @param userCtxPtr Optional user context (future use).
     */
    DBConnector_async(const std::string &dbName,
                      void              *userCtxPtr=nullptr);
    ~DBConnector_async();

    /**
     * @brief Get the hiredis context
     *
     * @return Return the hiredis async connection context. This API should be
     *         used when your application needs to hook up the hiredis context
     *         to the event loop. For example, if your application uses the
     *         GLib main loop, you would hook up the hiredis context as in the
     *         example below:
     *
     * @code
     *  // --------------------------------------------------------------------
     *  // Example showing how to hook up the hiredis context retrived
     *  // from a DBConnector_async object to a GLib main event loop.
     *  --------------------------------------------------------------------
     *  #include <glib.h>                   // g_main_context_default()
     *  #include <hiredis/adapters/glib.h>  // redis_source_new()
     *  #include "dbconnector.h"            // class DBConnector_async
     *
     *  GMainContext * main_ctx_p = g_main_context_default();
     *  GMainLoop    * loop_p     = g_main_loop_new(main_ctx_p, FALSE);
     *  .
     *  .
     *
     *  DBConnector_async  db("APPL_DB");
     *  .
     *  .
     *
     *  // Hook up hiredis context to GLib main loop
     *  g_source_attach(redis_source_new(db.context()), main_ctx_p);
     *  .
     *  .
     *
     *  g_main_loop_run(loop_p);
     *
     * @endcode
     *
     */
    redisAsyncContext *context() const { return m_acPtr; }

    /**
     * @brief Return the user context that was provided in the constructor.
     *
     * @return User context pointer.
     */
    void *getUserCtx() const { return m_userCtxPtr; }

    /**
     * @brief Get the DB name (e.g. "CONFIG_DB", "APPL_DB", etc.). This is the
     *        same name that was provided to the constructor.
     *
     * @return The DB name associated with this connector.
     */
    const char *getDbName() const { return m_dbName.c_str(); }

    /**
     * @brief Get the DB ID (e.g. 0 for "APPL_DB", 4 for "CONFIG_DB", etc.)
     *
     * @return The DB ID associated with this connector.
     */
    int getDbId() const { return m_dbId; }

    /**
     * @brief Get the socket address associated with this connector.
     *
     * @return The Unix Domain Socket name.
     */
    const char *getSockAddr() const { return m_sockAddr.c_str(); }

    /**
     * @brief Invoke a REDIS a command
     *
     * @param cb_func_p Callback function to be invoked when reply is received
     * @param cb_data_p User data passed to the callback function
     * @param format A format string similar to printf(), but specific to
     *               hiredis. For more info refer to the documentation for <a
     *               href="https://github.com/redis/hiredis/blob/master/hiredis.c#L531">redisFormatCommand()</a>.
     *
     * @return 0 on success, otherwise the status returned by
     *         redisvAsyncCommand()
     */
    int command(redisCallbackFn *cb_func_p, void *cb_data_p, const char *format, ...);

    /**
     * @brief Invoke a REDIS a command
     *
     * @param cb_func_p Callback function to be invoked when reply is received
     * @param cb_data_p User data passed to the callback function
     * @param cmd A formatted command to be sent to the REDIS server
     * @param len The length of %cmd
     *
     * @return 0 on success, otherwise the status returned by
     *         redisAsyncFormattedCommand()
     */
    int formatted_command(redisCallbackFn *cb_func_p, void *cb_data_p, const char *cmd_p, size_t len);

private:
    const std::string   m_dbName;
    const int           m_dbId = -1;
    std::string         m_sockAddr;
    redisAsyncContext  *m_acPtr = nullptr;
    void               *m_userCtxPtr = nullptr;
};

} // namespace swss
#endif
