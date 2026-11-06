#pragma once

#include <stdexcept>
#include <hiredis/hiredis.h>

namespace swss
{
// A local policy or credential error cannot be repaired by reconnecting.
class RedisAuthenticationError : public std::runtime_error
{
public:
    using std::runtime_error::runtime_error;
};

// Used only by connectors explicitly constructed with authenticate=true.
void authenticateRedisConnection(redisContext *context);
using RedisAuthCommand = redisReply *(*)(redisContext *, const char **, const size_t *, void *);
void authenticateRedisConnection(redisContext *context, RedisAuthCommand command, void *opaque);
}
