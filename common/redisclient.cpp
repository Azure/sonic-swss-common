#include "redisclient.h"

namespace swss
{

RedisClient::RedisClient(swss::DBConnector *db):
    m_db(db)
{
}

int64_t RedisClient::del(std::string key)
{
    char *temp;
    int len = redisFormatCommand(&temp, "DEL %s", key.c_str());
    std::string del(temp, len);
    free(temp);

    RedisReply r(m_db, del, REDIS_REPLY_INTEGER, true);

    if (r.getContext()->type != REDIS_REPLY_INTEGER)
        throw std::runtime_error("DEL operation failed");

    return r.getContext()->integer;
}

int64_t RedisClient::hdel(std::string key, std::string field)
{
    char *temp;
    int len = redisFormatCommand(&temp, "HDEL %s %s", key.c_str(), field.c_str());
    std::string hdel(temp, len);
    free(temp);

    RedisReply r(m_db, hdel, REDIS_REPLY_INTEGER, true);

    if (r.getContext()->type != REDIS_REPLY_INTEGER)
        throw std::runtime_error("HDEL operation failed");

    return r.getContext()->integer;
}

void RedisClient::hset(std::string key, std::string field, std::string value)
{
    char *temp;
    int len = redisFormatCommand(&temp, "HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
    std::string hset(temp, len);
    free(temp);

    RedisReply r(m_db, hset, REDIS_REPLY_INTEGER, true);

    if (r.getContext()->type != REDIS_REPLY_INTEGER)
        throw std::runtime_error("HSET operation failed");
}

std::unordered_map<std::string, std::string> RedisClient::hgetall(std::string key)
{
    std::unordered_map<std::string, std::string> map;

    char *temp;
    int len = redisFormatCommand(&temp, "HGETALL %s", key.c_str());

    std::string incr(temp, len);
    free(temp);

    RedisReply r(m_db, incr, REDIS_REPLY_ARRAY, true);

    if (r.getContext()->type != REDIS_REPLY_ARRAY)
        throw std::runtime_error("HGETALL operation failed");

    auto ctx = r.getContext();

    for (unsigned int i = 0; i < ctx->elements; i += 2)
        map[std::string(ctx->element[i]->str)] = std::string(ctx->element[i+1]->str);

    return map;
}

std::vector<std::string> RedisClient::keys(std::string key)
{
    std::vector<std::string> list;

    char *temp;
    int len = redisFormatCommand(&temp, "KEYS %s", key.c_str());

    std::string keys(temp, len);
    free(temp);

    RedisReply r(m_db, keys, REDIS_REPLY_ARRAY, true);

    if (r.getContext()->type != REDIS_REPLY_ARRAY)
        throw std::runtime_error("KEYS operation failed");

    auto ctx = r.getContext();

    for (unsigned int i = 0; i < ctx->elements; i++)
        list.push_back(ctx->element[i]->str);

    return list;
}

int64_t RedisClient::incr(std::string key)
{
    char *temp;
    int len = redisFormatCommand(&temp, "INCR %s", key.c_str());

    std::string incr(temp, len);
    free(temp);

    RedisReply r(m_db, incr, REDIS_REPLY_INTEGER, true);

    if (r.getContext()->type != REDIS_REPLY_INTEGER)
        throw std::runtime_error("INCR command failed");

    return r.getContext()->integer;
}

int64_t RedisClient::decr(std::string key)
{
    char *temp;
    int len = redisFormatCommand(&temp, "DECR %s", key.c_str());

    std::string decr(temp, len);
    free(temp);

    RedisReply r(m_db, decr, REDIS_REPLY_INTEGER, true);

    if (r.getContext()->type != REDIS_REPLY_INTEGER)
        throw std::runtime_error("DECR command failed");

    return r.getContext()->integer;
}

std::shared_ptr<std::string> RedisClient::get(std::string key)
{
    char *temp;
    int len = redisFormatCommand(&temp, "GET %s", key.c_str());

    std::string get(temp, len);
    free(temp);

    redisReply *reply;

    redisAppendFormattedCommand(m_db->getContext(), get.c_str(), get.length());
    redisGetReply(m_db->getContext(), (void**)&reply);

    if (!reply)
        throw std::runtime_error("GET failed, memory exception");

    if (reply->type == REDIS_REPLY_NIL)
    {
        freeReplyObject(reply);
        return std::shared_ptr<std::string>(NULL);
    }

    if (reply->type == REDIS_REPLY_STRING)
    {
        std::shared_ptr<std::string> ptr(new std::string(reply->str));
        freeReplyObject(reply);
        return ptr;
    }

    freeReplyObject(reply);

    throw std::runtime_error("GET failed, memory exception");
}

std::shared_ptr<std::string> RedisClient::hget(std::string key, std::string field)
{
    char *temp;
    int len = redisFormatCommand(&temp, "HGET %s %s", key.c_str(), field.c_str());

    std::string hget(temp, len);
    free(temp);

    redisReply *reply;

    redisAppendFormattedCommand(m_db->getContext(), hget.c_str(), hget.length());
    redisGetReply(m_db->getContext(), (void**)&reply);

    if (!reply)
        throw std::runtime_error("HGET failed, memory exception");

    if (reply->type == REDIS_REPLY_NIL)
    {
        freeReplyObject(reply);
        return std::shared_ptr<std::string>(NULL);
    }

    if (reply->type == REDIS_REPLY_STRING)
    {
        std::shared_ptr<std::string> ptr(new std::string(reply->str));
        freeReplyObject(reply);
        return ptr;
    }

    freeReplyObject(reply);

    throw std::runtime_error("HGET failed, memory exception");
}

}
