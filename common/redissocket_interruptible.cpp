#include "redissocket_interruptible.h"
#include "redisauth.h"
#include "redisreply.h"
#include <algorithm>
#include <chrono>
#include <climits>
#include <fcntl.h>
#include <memory>
#include <poll.h>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>

namespace
{
class Waiter
{
    using Clock = std::chrono::steady_clock;
    int m_cancel;
    bool m_limited;
    Clock::time_point m_end;
public:
    Waiter(int cancel, unsigned int milliseconds)
        : m_cancel(cancel), m_limited(milliseconds != 0),
          m_end(Clock::now() + std::chrono::milliseconds(milliseconds)) {}

    void check()
    {
        pollfd event{m_cancel, POLLIN, 0};
        int rc;
        do { rc = poll(&event, 1, 0); } while (rc < 0 && errno == EINTR);
        if (rc < 0 || (event.revents & POLLNVAL))
            throw std::system_error(rc < 0 ? errno : EBADF, std::generic_category(), "Redis cancellation descriptor");
        if (event.revents)
            throw std::system_error(std::make_error_code(std::errc::operation_canceled));
        if (m_limited && Clock::now() >= m_end)
            throw std::system_error(std::make_error_code(std::errc::timed_out));
    }

    void wait(int fd, short events)
    {
        for (;;)
        {
            check();
            int timeout = -1;
            if (m_limited)
            {
                const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(m_end - Clock::now()).count();
                timeout = static_cast<int>(std::min<long long>(std::max<long long>(left, 0) + 1, INT_MAX));
            }
            pollfd fds[] = {{fd, events, 0}, {m_cancel, POLLIN, 0}};
            const int rc = poll(fds, 2, timeout);
            if (rc < 0 && errno == EINTR) continue;
            if (rc < 0) throw std::system_error(errno, std::generic_category(), "Redis socket poll");
            check();
            if (fds[0].revents & POLLNVAL)
                throw std::system_error(EBADF, std::generic_category(), "Redis socket");
            if (fds[0].revents) return;
        }
    }

    redisReply *command(redisContext *context, const char **args, const size_t *lengths)
    {
        if (redisAppendCommandArgv(context, 3, args, lengths) != REDIS_OK)
            throw swss::RedisError("Cannot queue Redis authentication", context);
        int flushed = 0;
        while (!flushed)
        {
            check();
            if (redisBufferWrite(context, &flushed) != REDIS_OK)
                throw swss::RedisError("Redis authentication write failure", context);
            if (!flushed) wait(context->fd, POLLOUT);
        }
        for (;;)
        {
            check();
            void *reply = nullptr;
            if (redisGetReplyFromReader(context, &reply) != REDIS_OK)
                throw swss::RedisError("Redis authentication reply failure", context);
            if (reply) return static_cast<redisReply *>(reply);
            wait(context->fd, POLLIN);
            if (redisBufferRead(context) != REDIS_OK)
                throw swss::RedisError("Redis authentication read failure", context);
        }
    }
};

redisReply *authenticate(redisContext *context, const char **args, const size_t *lengths, void *waiter)
{
    return static_cast<Waiter *>(waiter)->command(context, args, lengths);
}
}

int swss::openRedisUnixSocketInterruptible(const std::string& path, unsigned int timeout_ms, bool authenticate, int cancelFd)
{
    if (path.empty() || path.front() != '/' || path.find('\0') != std::string::npos || cancelFd < -1)
        throw std::invalid_argument("An absolute Redis Unix path and valid cancellation descriptor are required");
    Waiter waiter(cancelFd, timeout_ms);
    waiter.check();
    std::unique_ptr<redisContext, decltype(&redisFree)> context(redisConnectUnixNonBlock(path.c_str()), redisFree);
    if (!context) throw std::bad_alloc();
    if (context->err) throw RedisError("Redis Unix connection failed", context.get());
    waiter.wait(context->fd, POLLOUT);
    int error = 0;
    socklen_t length = sizeof(error);
    if (getsockopt(context->fd, SOL_SOCKET, SO_ERROR, &error, &length) != 0)
        throw std::system_error(errno, std::generic_category(), "Redis connect status");
    if (error) throw std::system_error(error, std::generic_category(), "Redis Unix connection");
    if (authenticate) authenticateRedisConnection(context.get(), ::authenticate, &waiter);
    waiter.check();
    const int flags = fcntl(context->fd, F_GETFL);
    if (flags < 0 || fcntl(context->fd, F_SETFL, flags & ~O_NONBLOCK) < 0)
        throw std::system_error(errno, std::generic_category(), "Redis blocking mode");
    const int fd = fcntl(context->fd, F_DUPFD_CLOEXEC, 0);
    if (fd < 0) throw std::system_error(errno, std::generic_category(), "Redis descriptor duplication");
    try { waiter.check(); }
    catch (...) { close(fd); throw; }
    return fd;
}
