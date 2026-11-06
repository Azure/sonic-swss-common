#pragma once

#include <stdint.h>
#include <unistd.h>
#include <stdexcept>
#include <memory>

#include "dbconnector.h"
#include "logger.h"

namespace swss
{

class UnavailableDataError : public std::runtime_error
{
public:
    UnavailableDataError(const std::string& message, const std::string& data)
        : std::runtime_error(message)
        , m_data(data)
    {
    }

    const char *getData() const
    {
        return m_data.c_str();
    }

private:
    const std::string m_data;
};

class DBInterface
{
public:
    void connect(int dbId, const std::string& dbName, bool retry = true);
    void close(const std::string& dbName);
    void close();
    int64_t del(const std::string& dbName, const std::string& key, bool blocking = false);
    // Delete all keys which match %pattern from DB
    void delete_all_by_pattern(const std::string& dbName, const std::string& pattern);
    bool exists(const std::string& dbName, const std::string& key);
    std::shared_ptr<std::string> get(const std::string& dbName, const std::string& hash, const std::string& key, bool blocking = false);
    bool hexists(const std::string& dbName, const std::string& hash, const std::string& key);
    std::map<std::string, std::string> get_all(const std::string& dbName, const std::string& hash, bool blocking = false);
    std::vector<std::string> keys(const std::string& dbName, const char *pattern = "*", bool blocking = false);
    std::pair<int, std::vector<std::string>> scan(const std::string& db_name, int cursor, const char *match, uint32_t count);
    int64_t publish(const std::string& dbName, const std::string& channel, const std::string& message);
    void hmset(const std::string& dbName, const std::string &hash, const std::map<std::string, std::string> &values);
    int64_t set(const std::string& dbName, const std::string& hash, const std::string& key, const std::string& value, bool blocking = false);
    DBConnector& get_redis_client(const std::string& dbName);
    void set_redis_kwargs(std::string unix_socket_path, std::string host, int port);

    static const int BLOCKING_ATTEMPT_ERROR_THRESHOLD = 10;
    static const int BLOCKING_ATTEMPT_SUPPRESSION = BLOCKING_ATTEMPT_ERROR_THRESHOLD + 5;

    // Wait period in seconds before attempting to reconnect to Redis.
    static const int CONNECT_RETRY_WAIT_TIME = 10;

    // Wait period in seconds to wait before attempting to retrieve missing data.
    static const int DATA_RETRIEVAL_WAIT_TIME = 3;

    // Time to wait for any given message to arrive via pub-sub.
    static const int PUB_SUB_NOTIFICATION_TIMEOUT = 10;  // seconds

    // Maximum allowable time to wait on a specific pub-sub notification.
    static constexpr double PUB_SUB_MAXIMUM_DATA_WAIT = 60.0;  // seconds

private:
    template <typename T, typename FUNC>
    T blockable(FUNC f, const std::string& dbName, bool blocking = false);
    // Unsubscribe the chosent client from keyspace event notifications
    void _unsubscribe_keyspace_notification(const std::string& dbName);
    bool _unavailable_data_handler(const std::string& dbName, const char *data);
    // Subscribe the chosent client to keyspace event notifications
    void _subscribe_keyspace_notification(const std::string& dbName);
    // In the event Redis is unavailable, close existing connections, and try again.
    void _connection_error_handler(const std::string& dbName);
    void _onetime_connect(int dbId, const std::string& dbName);
    // Keep reconnecting to Database 'dbId' until success
    void _persistent_connect(int dbId, const std::string& dbName);

    // Redis keyspace notifications must be enabled by server configuration before
    // callers subscribe through this interface.
    static constexpr const char *KEYSPACE_PATTERN = "__key*__:*";

    std::unordered_map<std::string, std::shared_ptr<DBConnector>> keyspace_notification_channels;

    std::unordered_map<std::string, DBConnector> m_redisClient;

    std::string m_unix_socket_path;
    std::string m_host = "127.0.0.1";
    int m_port = 6379;
};

}
