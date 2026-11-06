#include "redissocket.h"
#include "redisauth.h"
#include "redisreply.h"
#include <chrono>
#include <fcntl.h>
#include <memory>
#include <system_error>

int swss::openRedisUnixSocket(const std::string& path, unsigned int timeout_ms, bool authenticate)
{
    if (path.empty() || path.front() != '/' || path.find('\0') != std::string::npos)
        throw std::invalid_argument("An absolute Redis Unix path is required");
    using Clock = std::chrono::steady_clock;
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    const timeval timeout{static_cast<time_t>(timeout_ms / 1000), static_cast<suseconds_t>((timeout_ms % 1000) * 1000)};
    std::unique_ptr<redisContext, decltype(&redisFree)> context(timeout_ms
        ? redisConnectUnixWithTimeout(path.c_str(), timeout) : redisConnectUnix(path.c_str()), redisFree);
    if (!context) throw std::bad_alloc();
    if (context->err) throw RedisError("Redis Unix connection failed", context.get());
    if (authenticate)
    {
        if (timeout_ms)
        {
            const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
            if (left <= 0) throw std::system_error(std::make_error_code(std::errc::timed_out));
            const timeval remaining{static_cast<time_t>(left / 1000), static_cast<suseconds_t>((left % 1000) * 1000)};
            if (redisSetTimeout(context.get(), remaining) != REDIS_OK)
                throw RedisError("Cannot set AUTH timeout", context.get());
        }
        try { authenticateRedisConnection(context.get()); }
        catch (const RedisError&) {
            if (timeout_ms && Clock::now() >= deadline)
                throw std::system_error(std::make_error_code(std::errc::timed_out));
            throw;
        }
        const timeval unlimited{};
        if (timeout_ms && redisSetTimeout(context.get(), unlimited) != REDIS_OK)
            throw RedisError("Cannot clear AUTH timeout", context.get());
    }
    const int fd = fcntl(context->fd, F_DUPFD_CLOEXEC, 0);
    if (fd < 0) throw std::system_error(errno, std::generic_category(), "Redis descriptor duplication");
    return fd;
}
