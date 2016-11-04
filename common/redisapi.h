#pragma once
#include <stdexcept>
#include "logger.h"
#include "rediscommand.h"

namespace swss {

static inline std::string loadRedisScript(DBConnector* db, const std::string& script)
{
    SWSS_LOG_ENTER();

    RedisCommand loadcmd;
    loadcmd.format("SCRIPT LOAD %s", script.c_str());
    RedisReply r(db, loadcmd, REDIS_REPLY_STRING);
    return r.getReply<std::string>();
}

// Hit Redis bug: Lua redis() command arguments must be strings or integers,
//   however, an empty string is not accepted
// Bypass by prefix a dummy char and remove it in the lua script
static inline std::string encodeLuaArgument(const std::string& arg)
{
    return '`' + arg;
}

}
