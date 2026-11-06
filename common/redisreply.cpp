#include <set>
#include <string.h>
#include <stdint.h>
#include <vector>
#include <iostream>
#include <sstream>
#include <functional>
#include <boost/algorithm/string.hpp>
#include <nlohmann/json.hpp>

#include "common/logger.h"
#include "common/redisreply.h"
#include "common/dbconnector.h"
#include "common/rediscommand.h"
#include "common/stringutility.h"

using namespace std;
using namespace boost;

namespace swss {

static set<string> g_intToBoolCommands = {
                        "COPY",
                        "EXPIRE",
                        "EXPIREAT",
                        "PEXPIRE",
                        "PEXPIREAT",
                        "HEXISTS",
                        "MOVE",
                        "MSETNX",
                        "PERSIST",
                        "RENAMENX",
                        "SISMEMBER",
                        "SMOVE",
                        "SETNX"
                        };

static set<string> g_strToBoolCommands = {
                        "AUTH",
                        "HMSET",
                        "PSETEX",
                        "SETEX",
                        "FLUSHALL",
                        "FLUSHDB",
                        "LSET",
                        "LTRIM",
                        "MSET",
                        "PFMERGE",
                        "ASKING",
                        "READONLY",
                        "READWRITE",
                        "RENAME",
                        "SAVE",
                        "SELECT",
                        "SHUTDOWN",
                        "SLAVEOF",
                        "SWAPDB",
                        "WATCH",
                        "UNWATCH",
                        "SET"
                        };

RedisReply::RedisReply(RedisContext *ctx, const RedisCommand& command)
{
    int rc = command.appendTo(ctx->getContext());
    if (rc != REDIS_OK)
    {
        // The only reason of error is REDIS_ERR_OOM (Out of memory)
        // ref: https://github.com/redis/hiredis/blob/master/hiredis.c
        throw bad_alloc();
    }

    rc = redisGetReply(ctx->getContext(), (void**)&m_reply);
    if (rc != REDIS_OK)
    {
        throw RedisError("Failed to redisGetReply with " + command.toPrintableString(), ctx->getContext());
    }
    guard([&]{checkReply();}, command.toPrintableString().c_str());
}

RedisReply::RedisReply(RedisContext *ctx, const string& command)
{
    int rc = redisAppendCommand(ctx->getContext(), command.c_str());
    if (rc != REDIS_OK)
    {
        // The only reason of error is REDIS_ERR_OOM (Out of memory)
        // ref: https://github.com/redis/hiredis/blob/master/hiredis.c
        throw bad_alloc();
    }

    rc = redisGetReply(ctx->getContext(), (void**)&m_reply);
    if (rc != REDIS_OK)
    {
        throw RedisError("Failed to redisGetReply with " + command, ctx->getContext());
    }
    guard([&]{checkReply();}, binary_to_printable(command.c_str(), command.length()).c_str());
}

RedisReply::RedisReply(RedisContext *ctx, const RedisCommand& command, int expectedType)
    : RedisReply(ctx, command)
{
    guard([&]{checkReplyType(expectedType);}, command.toPrintableString().c_str());
}

RedisReply::RedisReply(RedisContext *ctx, const string& command, int expectedType)
    : RedisReply(ctx, command)
{
    guard([&]{checkReplyType(expectedType);}, binary_to_printable(command.c_str(), command.length()).c_str());
}

RedisReply::RedisReply(redisReply *reply) :
    m_reply(reply)
{
}

RedisReply::~RedisReply()
{
    freeReplyObject(m_reply);
}

redisReply *RedisReply::release()
{
    redisReply *ret = m_reply;
    m_reply = NULL;
    return ret;
}


redisReply *RedisReply::getContext()
{
    return m_reply;
}

size_t RedisReply::getChildCount()
{
    return m_reply->elements;
}

redisReply *RedisReply::getChild(size_t index)
{
    if (index >= m_reply->elements)
    {
        throw out_of_range("Out of the range of redisReply elements");
    }
    return m_reply->element[index];
}

redisReply *RedisReply::releaseChild(size_t index)
{
    auto ret = getChild(index);
    m_reply->element[index] = NULL;
    return ret;
}

void RedisReply::checkStatus(const char *status)
{
    if (strcmp(m_reply->str, status) != 0)
    {
        SWSS_LOG_ERROR("Redis reply %s != %s", m_reply->str, status);

        throw system_error(make_error_code(errc::io_error),
                           "Invalid return code");
    }
}

void RedisReply::checkReply()
{
    if (!m_reply)
    {
        throw system_error(make_error_code(errc::not_enough_memory),
                           "Memory exception, reply is null");
    }

    if (m_reply->type == REDIS_REPLY_ERROR)
    {
        system_error ex(make_error_code(errc::invalid_argument),
                           m_reply->str);
        freeReplyObject(m_reply);
        m_reply = NULL;
        throw ex;
    }
}

void RedisReply::checkReplyType(int expectedType)
{
    if (m_reply->type != expectedType)
    {
        const char *err = (m_reply->type == REDIS_REPLY_STRING || m_reply->type == REDIS_REPLY_ERROR) ?
            m_reply->str : "NON-STRING-REPLY";

        string errmsg = "Expected to get redis type " + std::to_string(expectedType) + " got type " + std::to_string(m_reply->type) + ", err: " + err;
        SWSS_LOG_ERROR("%s", errmsg.c_str());
        throw system_error(make_error_code(errc::io_error), errmsg);
    }
}

void RedisReply::checkStatusOK()
{
    checkStatus("OK");
}

void RedisReply::checkStatusQueued()
{
    checkStatus("QUEUED");
}

template<> long long int RedisReply::getReply<long long int>()
{
    return getContext()->integer;
}

template<> string RedisReply::getReply<string>()
{
    char *s = getContext()->str;
    if (s == NULL) return string();
    return string(s);
}

template<> RedisMessage RedisReply::getReply<RedisMessage>()
{
    RedisMessage ret;
    /* if the Key-space notification is empty, try empty message. */
    if (getContext()->type == REDIS_REPLY_NIL)
    {
        return ret;
    }

    if (getContext()->type != REDIS_REPLY_ARRAY)
    {
        SWSS_LOG_ERROR("invalid type %d for message", getContext()->type);
        return ret;
    }
    size_t n = getContext()->elements;

    /* Expecting 4 elements for each keyspace pmessage notification */
    if (n != 4)
    {
        SWSS_LOG_ERROR("invalid number of elements %zu for message", n);
        return ret;
    }

    auto ctx = getContext()->element[0];
    ret.type = ctx->str;

    /* The second element should be the original pattern matched */
    ctx = getContext()->element[1];
    ret.pattern = ctx->str;

    ctx = getContext()->element[2];
    ret.channel = ctx->str;

    ctx = getContext()->element[3];
    ret.data = ctx->str;

    return ret;
}

string RedisReply::to_string()
{
    return RedisReply::to_string(this->getContext());
}

string RedisReply::to_string(redisReply *reply, string command)
{
    /*
        Response format need keep as same as redis-py, redis-py using a command to result type mapping to convert result:
            https://github.com/redis/redis-py/blob/bedf3c82a55b4b67eed93f686cb17e82f7ab19cd/redis/client.py#L682
        Redis command result type can be found here:
            https://redis.io/commands/
        Only commands used by scripts in sonic repos are supported by these method.
        For example: 'Info' command not used by any sonic scripts, so the output format will different with redis-py.
    */
    switch(reply->type)
    {
    case REDIS_REPLY_INTEGER:
        return formatReply(command, reply->integer);

    case REDIS_REPLY_STRING:
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_NIL:
        return formatReply(command, reply->str, reply->len);

    case REDIS_REPLY_ARRAY:
    {
        return formatReply(command, reply->element, reply->elements);
    }

    default:
        SWSS_LOG_ERROR("invalid type %d for message", reply->type);
        return string();
    }
}

static nlohmann::ordered_json buildJsonDict(struct redisReply **element, size_t elements)
{
    if (elements%2 != 0)
    {
        throw system_error(make_error_code(errc::io_error),
                           "Invalid result");
    }

    auto result = nlohmann::ordered_json::object();
    for (size_t i = 0; i < elements; i += 2)
    {
        result[RedisReply::to_string(element[i])] = RedisReply::to_string(element[i + 1]);
    }

    return result;
}

static nlohmann::ordered_json buildJsonReply(redisReply *reply, const string& command)
{
    switch(reply->type)
    {
    case REDIS_REPLY_INTEGER:
        return reply->integer;

    case REDIS_REPLY_NIL:
        return nullptr;

    case REDIS_REPLY_STRING:
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STATUS:
        return string(reply->str, reply->len);

    case REDIS_REPLY_ARRAY:
    {
        if (command == "HGETALL")
        {
            return buildJsonDict(reply->element, reply->elements);
        }

        if (command == "HSCAN" && reply->elements == 2 &&
            reply->element[1]->type == REDIS_REPLY_ARRAY)
        {
            auto result = nlohmann::ordered_json::array();
            result.push_back(buildJsonReply(reply->element[0], string()));
            result.push_back(buildJsonDict(reply->element[1]->element, reply->element[1]->elements));
            return result;
        }

        auto result = nlohmann::ordered_json::array();
        for (size_t i = 0; i < reply->elements; i++)
        {
            result.push_back(buildJsonReply(reply->element[i], string()));
        }
        return result;
    }

    default:
        SWSS_LOG_ERROR("invalid type %d for message", reply->type);
        return nullptr;
    }
}

string RedisReply::to_json_string(redisReply *reply, string command)
{
    /*
        JSON output mode for sonic-db-cli --json.
        Unlike to_string(), this does not emulate redis-py output: replies map
        directly to JSON types (integer -> number, nil -> null, status/error -> string),
        except that HGETALL (and the field map element of HSCAN) become a JSON object.
        Bytes that are not valid UTF-8 are replaced with U+FFFD rather than failing.
    */
    auto result = buildJsonReply(reply, command);
    return result.dump(-1, ' ', false, nlohmann::ordered_json::error_handler_t::replace);
}

string RedisReply::formatReply(string command, long long integer)
{
    if (g_intToBoolCommands.find(command) != g_intToBoolCommands.end())
    {
        if (integer == 1)
        {
            return string("True");
        }
        else if (integer == 0)
        {
            return string("False");
        }
    }
    else if (command == "AUTH")
    {
        if (integer != 0)
        {
            return string("OK");
        }
    }

    return std::to_string(integer);
}

string RedisReply::formatReply(string command, const char* str, size_t len)
{
    string result = string(str, len);
    if (g_strToBoolCommands.find(command) != g_strToBoolCommands.end()
        && result == "OK")
    {
        return string("True");
    }
    
    if (command == "PING")
    {
        // In redis-py, result for PING was hard coded: https://github.com/redis/redis-py/blob/bedf3c82a55b4b67eed93f686cb17e82f7ab19cd/redis/client.py#L796
        if (result == "PONG")
        {
            return string("True");
        }
        else
        {
            return string("False");
        }
    }

    return result;
}

string RedisReply::formatReply(string command, struct redisReply **element, size_t elements)
{
    if (command == "HGETALL")
    {
        return formatDictReply(element, elements);
    }
    else if(command == "SCAN"
            || command == "SSCAN")
    {
        return formatSscanReply(element, elements);
    }
    else if(command == "HSCAN")
    {
        return formatHscanReply(element, elements);
    }
    else if(command == "BLPOP"
            || command == "BRPOP")
    {
        return formatTupleReply(element, elements);
    }
    else
    {
        return formatListReply(element, elements);
    }
}

string RedisReply::formatSscanReply(struct redisReply **element, size_t elements)
{
    if (elements != 2)
    {
        throw system_error(make_error_code(errc::io_error),
                           "Invalid result");
    }

    // format HSCAN result, here is a example:
    //  (0, {'test3': 'test3', 'test2': 'test2'})
    ostringstream result;
    result << "(" << element[0]->integer << ", ";
    // format the field mapping part
    result << formatArrayReply(element[1]->element, element[1]->elements);
    result << ")";

    return result.str();
}

string RedisReply::formatHscanReply(struct redisReply **element, size_t elements)
{
    if (elements != 2)
    {
        throw system_error(make_error_code(errc::io_error),
                           "Invalid result");
    }

    // format HSCAN result, here is a example:
    //  (0, {'test3': 'test3', 'test2': 'test2'})
    ostringstream result;
    result << "(" << element[0]->integer << ", ";
    // format the field mapping part
    result << formatDictReply(element[1]->element, element[1]->elements);
    result << ")";

    return result.str();
}

string RedisReply::formatDictReply(struct redisReply **element, size_t elements)
{
    if (elements%2 != 0)
    {
        throw system_error(make_error_code(errc::io_error),
                           "Invalid result");
    }

    // format dictionary, not using json.h because the output format are different, here is a example:
    //      {'test3': 'test3', 'test2': 'test2'}
    vector<string> elementvector;
    for (unsigned int i = 0; i < elements; i += 2)
    {
        string key = formatStringWithQuot(to_string(element[i]));
        string value = formatStringWithQuot(to_string(element[i + 1]));
        elementvector.push_back(key + ": " + value);
    }

    return swss::join(", ", '{', '}', elementvector.begin(), elementvector.end());
}

string RedisReply::formatArrayReply(struct redisReply **element, size_t elements)
{
    vector<string> elementvector;
    for (unsigned int i = 0; i < elements; i++)
    {
        elementvector.push_back(formatStringWithQuot(to_string(element[i])));
    }

    return swss::join(", ", '[', ']', elementvector.begin(), elementvector.end());
}

string RedisReply::formatListReply(struct redisReply **element, size_t elements)
{
    vector<string> elementvector;
    for (unsigned int i = 0; i < elements; i++)
    {
        elementvector.push_back(to_string(element[i]));
    }

    return swss::join("\n", elementvector.begin(), elementvector.end());
}

string RedisReply::formatTupleReply(struct redisReply **element, size_t elements)
{
    vector<string> elementvector;
    for (unsigned int i = 0; i < elements; i++)
    {
        elementvector.push_back(formatStringWithQuot(to_string(element[i])));
    }

    return swss::join(", ", '(', ')', elementvector.begin(), elementvector.end());
}

string RedisReply::formatStringWithQuot(string str)
{
    if (str.find('\'') != std::string::npos)
    {
        return "\"" + str + "\"";
    }
    
    return "'" + str + "'";
}

}
