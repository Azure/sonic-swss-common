#include <chrono>
#include <cstring>
#include <utility>
#include <hiredis/hiredis.h>
#include "dbinterface.h"

using namespace std;
using namespace std::chrono;
using namespace swss;

void DBInterface::set_redis_kwargs(std::string unix_socket_path, std::string host, int port)
{
    m_unix_socket_path = unix_socket_path;
    m_host = host;
    m_port = port;
}

void DBInterface::connect(int dbId, const std::string& dbName, bool retry)
{
    if (retry)
    {
        _persistent_connect(dbId, dbName);
    }
    else
    {
        _onetime_connect(dbId, dbName);
    }
}

void DBInterface::close(const std::string& dbName)
{
    m_redisClient.erase(dbName);
}

void DBInterface::close()
{
    m_redisClient.clear();
}

int64_t DBInterface::del(const string& dbName, const std::string& key, bool blocking)
{
    auto innerfunc = [&]
    {
        return m_redisClient.at(dbName).del(key);
    };
    return blockable<int64_t>(innerfunc, dbName, blocking);
}

void DBInterface::delete_all_by_pattern(const string& dbName, const string& pattern)
{
    auto& client = m_redisClient.at(dbName);
    auto keys = client.keys(pattern);
    for (auto& key: keys)
    {
        client.del(key);
    }
}

bool DBInterface::exists(const string& dbName, const std::string& key)
{
    return m_redisClient.at(dbName).exists(key);
}

std::shared_ptr<std::string> DBInterface::get(const std::string& dbName, const std::string& hash, const std::string& key, bool blocking)
{
    auto innerfunc = [&]
    {
        auto pvalue = m_redisClient.at(dbName).hget(hash, key);
        if (!pvalue)
        {
            std::string message = "Key '" + hash + "' field '" + key + "' unavailable in database '" + dbName + "'";
            throw UnavailableDataError(message, hash);
        }
        const std::string& value = *pvalue;
        return value == "None" ? std::shared_ptr<std::string>() : std::make_shared<std::string>(value);
    };
    return blockable<std::shared_ptr<std::string>>(innerfunc, dbName, blocking);
}

bool DBInterface::hexists(const std::string& dbName, const std::string& hash, const std::string& key)
{
    return m_redisClient.at(dbName).hexists(hash, key);
}

std::map<std::string, std::string> DBInterface::get_all(const std::string& dbName, const std::string& hash, bool blocking)
{
    auto innerfunc = [&]
    {
        std::map<std::string, std::string> map;
        m_redisClient.at(dbName).hgetall(hash, std::inserter(map, map.end()));

        if (map.empty())
        {
            std::string message = "Key '{" + hash + "}' unavailable in database '{" + dbName + "}'";
            throw UnavailableDataError(message, hash);
        }
        for (auto& i : map)
        {
            std::string& value = i.second;
            if (value == "None")
            {
                value = "";
            }
        }

        return map;
    };
    return blockable<std::map<std::string, std::string>>(innerfunc, dbName, blocking);
}

std::vector<std::string> DBInterface::keys(const std::string& dbName, const char *pattern, bool blocking)
{
    auto innerfunc = [&]
    {
        auto keys = m_redisClient.at(dbName).keys(pattern);
        if (keys.empty())
        {
            std::string message = "DB '{" + dbName + "}' is empty with pattern '" + pattern + "'!";
            throw UnavailableDataError(message, "hset");
        }
        return keys;
    };
    return blockable<std::vector<std::string>>(innerfunc, dbName, blocking);
}

std::pair<int, std::vector<std::string>> DBInterface::scan(const std::string& db_name, int cursor, const char *match, uint32_t count)
{
    return m_redisClient.at(db_name).scan(cursor, match, count);
}

int64_t DBInterface::publish(const std::string& dbName, const std::string& channel, const std::string& message)
{
    return m_redisClient.at(dbName).publish(channel, message);
}

void DBInterface::hmset(const std::string& dbName, const std::string &hash, const std::map<std::string, std::string> &values)
{
    m_redisClient.at(dbName).hmset(hash, values.begin(), values.end());
}

int64_t DBInterface::set(const std::string& dbName, const std::string& hash, const std::string& key, const std::string& value, bool blocking)
{
    auto innerfunc = [&]
    {
        m_redisClient.at(dbName).hset(hash, key, value);
        // Return the number of fields that were added.
        return 1;
    };
    return blockable<int64_t>(innerfunc, dbName, blocking);
}

DBConnector& DBInterface::get_redis_client(const std::string& dbName)
{
    return m_redisClient.at(dbName);
}

template <typename T, typename FUNC>
T DBInterface::blockable(FUNC f, const std::string& dbName, bool blocking)
{
    int attempts = 0;
    for (;;)
    {
        try
        {
            T ret_data = f();
            _unsubscribe_keyspace_notification(dbName);
            return ret_data;
        }
        catch (const UnavailableDataError& e)
        {
            if (blocking)
            {
                SWSS_LOG_WARN("%s", e.what());
                auto found = keyspace_notification_channels.find(dbName);
                if (found != keyspace_notification_channels.end())
                {
                    bool result = _unavailable_data_handler(dbName, e.getData());
                    if (result)
                    {
                        continue; // received updates, try to read data again
                    }
                    else
                    {
                        _unsubscribe_keyspace_notification(dbName);
                        throw; // No updates was received. Raise exception
                    }
                }
                else
                {
                    // Subscribe to updates and try it again (avoiding race condition)
                    _subscribe_keyspace_notification(dbName);
                }
            }
            else
            {
                return T();
            }
        }
        catch (const std::system_error&)
        {
            /*
            Something is fundamentally wrong with the request itself.
            Retrying the request won't pass unless the schema itself changes. In this case, the error
            should be attributed to the application itself. Re-raise the error.
            */
            SWSS_LOG_ERROR("Bad DB request [%s]", dbName.c_str());
            throw;
        }
        catch (const RedisError&)
        {
            // Redis connection broken and we need to retry several times
            attempts += 1;
            _connection_error_handler(dbName);
            std::string msg = "DB access failure by [" + dbName + + "]";
            if (BLOCKING_ATTEMPT_ERROR_THRESHOLD < attempts && attempts < BLOCKING_ATTEMPT_SUPPRESSION)
            {
                // Repeated access failures implies the database itself is unhealthy.
                SWSS_LOG_ERROR("%s", msg.c_str());
            }
            else
            {
                SWSS_LOG_WARN("%s", msg.c_str());
            }
        }
    }
}

// Unsubscribe the chosent client from keyspace event notifications
void DBInterface::_unsubscribe_keyspace_notification(const std::string& dbName)
{
    auto found = keyspace_notification_channels.find(dbName);
    if (found != keyspace_notification_channels.end())
    {
        SWSS_LOG_DEBUG("Unsubscribe from keyspace notification");

        keyspace_notification_channels.erase(found);
    }
}

// When the queried config is not available in Redis--wait until it is available.
// Two timeouts are at work here:
// 1. Notification timeout - how long to wait before giving up on receiving any given pub-sub message.
// 2. Max data wait - swsssdk-specific. how long to wait for the data to populate (in absolute time)
bool DBInterface::_unavailable_data_handler(const std::string& dbName, const char *data)
{
    auto start = system_clock::now();
    SWSS_LOG_DEBUG("Listening on pubsub channel '%s'", dbName.c_str());
    auto wait = duration<float>(PUB_SUB_MAXIMUM_DATA_WAIT);
    while (system_clock::now() - start < wait)
    {
        auto& channel = keyspace_notification_channels.at(dbName);
        auto ctx = channel->getContext();
        redisReply *reply;
        ctx->err = REDIS_OK; // Stop later redisGetReply early return on no data after first redisReply timeout
        int rc = redisGetReply(ctx, reinterpret_cast<void**>(&reply));
        if (rc == REDIS_ERR && ctx->err == REDIS_ERR_IO && errno == EAGAIN)
        {
            // Timeout
            continue;
        }
        if (rc != REDIS_OK)
        {
            throw RedisError("Failed to redisGetReply with on pubsub channel on dbName=" + dbName, ctx);
        }

        RedisReply r(reply);
        // r is an array of:
        //   0. 'type': 'pmessage',
        //   1. 'pattern': '__key*__:*'
        //   2. 'channel':
        //   3. 'data':
        redisReply& r3 = *r.getChild(3);
        if (r3.type != REDIS_REPLY_STRING)
        {
            throw system_error(make_error_code(errc::io_error),
                               "Wrong expected type of result");
        }

        if (strcmp(r3.str, data) == 0)
        {
            SWSS_LOG_INFO("'%s' acquired via pub-sub dbName=%s. Unblocking...", data, dbName.c_str());
            // Wait for a "settling" period before releasing the wait.
            sleep(DATA_RETRIEVAL_WAIT_TIME);
            return true;
        }
    }

    SWSS_LOG_WARN("No notification for '%s' from '%s' received before timeout.", data, dbName.c_str());
    return false;
}

// Subscribe the chosent client to keyspace event notifications
void DBInterface::_subscribe_keyspace_notification(const std::string& dbName)
{
    SWSS_LOG_DEBUG("Subscribe to keyspace notification");
    auto& client = m_redisClient.at(dbName);
    DBConnector *pubsub = client.newConnector(0);
    pubsub->psubscribe(KEYSPACE_PATTERN);

    // Set the timeout of the pubsub channel, so future redisGetReply will be impacted
    struct timeval tv = { PUB_SUB_NOTIFICATION_TIMEOUT, 0 };
    int rc = redisSetTimeout(pubsub->getContext(), tv);
    if (rc != REDIS_OK)
    {
        throw RedisError("Failed to redisSetTimeout", pubsub->getContext());
    }

    keyspace_notification_channels.emplace(std::piecewise_construct, std::forward_as_tuple(dbName), std::forward_as_tuple(pubsub));
}

// In the event Redis is unavailable, close existing connections, and try again.
void DBInterface::_connection_error_handler(const std::string& dbName)
{
    SWSS_LOG_WARN("Could not connect to Redis--waiting before trying again.");
    int dbId = get_redis_client(dbName).getDbId();
    close(dbName);
    sleep(CONNECT_RETRY_WAIT_TIME);
    connect(dbId, dbName, true);
}

void DBInterface::_onetime_connect(int dbId, const string& dbName)
{
    if (dbName.empty())
    {
        throw invalid_argument("dbName");
    }

    if (m_unix_socket_path.empty())
    {
        m_redisClient.emplace(std::piecewise_construct
                , std::forward_as_tuple(dbName)
                , std::forward_as_tuple(dbId, m_host, m_port, 0));
    }
    else
    {
        m_redisClient.emplace(std::piecewise_construct
                , std::forward_as_tuple(dbName)
                , std::forward_as_tuple(dbId, m_unix_socket_path, 0));
    }
}

// Keep reconnecting to Database 'dbId' until success
void DBInterface::_persistent_connect(int dbId, const string& dbName)
{
    for (;;)
    {
        try
        {
            _onetime_connect(dbId, dbName);
            return;
        }
        catch (RedisError&)
        {
            const int wait = CONNECT_RETRY_WAIT_TIME;
            SWSS_LOG_WARN("Connecting to DB '%s(%d)' failed, will retry in %d s", dbName.c_str(), dbId, wait);
            close(dbName);
            sleep(wait);
        }
    }
}
