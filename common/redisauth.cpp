#include "redisauth.h"
#include "redisreply.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <set>
#include <sys/stat.h>
#include <unistd.h>
#include <nlohmann/json.hpp>

namespace
{
using swss::RedisAuthenticationError;

struct File
{
    int fd;
    ~File() { if (fd >= 0) close(fd); }
    File(const File&) = delete;
    File& operator=(const File&) = delete;
    explicit File(int value) : fd(value) {}
};

void checkFile(int fd, bool directory, bool secret = false)
{
    struct stat info{};
    if (fstat(fd, &info) || info.st_uid != 0 ||
        (directory ? !S_ISDIR(info.st_mode) : !S_ISREG(info.st_mode)) ||
        (info.st_mode & (S_IWGRP | S_IWOTH)) ||
        (secret && (info.st_mode & (S_IXUSR | S_IXGRP | S_IRWXO))))
        throw RedisAuthenticationError("Unsafe Redis authentication file permissions");
}

std::string readFile(int fd, size_t limit)
{
    std::string result;
    char buffer[256];
    for (;;)
    {
        const auto count = read(fd, buffer, sizeof(buffer));
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) throw RedisAuthenticationError("Cannot read Redis authentication file");
        if (count == 0) break;
        result.append(buffer, static_cast<size_t>(count));
        if (result.size() > limit)
            throw RedisAuthenticationError("Redis authentication file exceeds size limit");
    }
    if (result.empty()) throw RedisAuthenticationError("Empty Redis authentication file");
    return result;
}
}

void swss::authenticateRedisConnection(redisContext *context)
{
    authenticateRedisConnection(context, [](redisContext *ctx, const char **args, const size_t *sizes, void *) {
        return static_cast<redisReply *>(redisCommandArgv(ctx, 3, args, sizes));
    }, nullptr);
}

void swss::authenticateRedisConnection(redisContext *context, RedisAuthCommand command, void *opaque)
{
    if (context->connection_type != REDIS_CONN_UNIX)
        throw RedisAuthenticationError("Redis writer authentication requires an explicit Unix socket");

    // Descriptor inheritance is restricted only for the new authenticated API.
    const int flags = fcntl(context->fd, F_GETFD);
    if (flags < 0 || fcntl(context->fd, F_SETFD, flags | FD_CLOEXEC) < 0)
        throw RedisAuthenticationError("Cannot set close-on-exec on authenticated Redis connection");

    // Startup owns this trust anchor. No environment or caller path overrides.
    File directory(open("/run/redis-auth", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (directory.fd < 0)
    {
        if (errno == ENOENT) return; // The same opted-in binary can run in legacy mode.
        throw RedisAuthenticationError("Cannot open Redis authentication directory");
    }
    checkFile(directory.fd, true);
    File policy(openat(directory.fd, "local.json", O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK));
    if (policy.fd < 0)
    {
        if (errno == ENOENT) return;
        throw RedisAuthenticationError("Cannot open Redis authentication policy");
    }
    checkFile(policy.fd, false);

    std::string username;
    try
    {
        auto entries = nlohmann::json::parse(readFile(policy.fd, 4096));
        if (entries.is_object()) entries = nlohmann::json::array({entries});
        if (!entries.is_array() || entries.empty())
            throw RedisAuthenticationError("Invalid Redis authentication policy");
        std::set<std::string> sockets;
        for (const auto& entry : entries)
        {
            const auto path = entry.at("unix_socket").get<std::string>();
            const auto user = entry.at("username").get<std::string>();
            const auto host = entry.at("tcp_host").get<std::string>();
            const auto& port = entry.at("tcp_port");
            if (entry.size() != 4 || path.empty() || path.front() != '/' ||
                path.find('\0') != std::string::npos || user.empty() || user.size() > 128 ||
                user.find('\0') != std::string::npos || (host != "127.0.0.1" && host != "::1") ||
                !port.is_number_integer() || port < 0 || port > 65535 || !sockets.insert(path).second)
                throw RedisAuthenticationError("Invalid Redis authentication policy");
            if (context->unix_sock.path && path == context->unix_sock.path) username = user;
        }
    }
    catch (const nlohmann::json::exception&)
    {
        throw RedisAuthenticationError("Invalid Redis authentication policy");
    }
    if (username.empty()) return;

    File secrets(openat(directory.fd, "local-system", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (secrets.fd < 0) throw RedisAuthenticationError("Cannot open Redis credential directory");
    checkFile(secrets.fd, true);
    File password(openat(secrets.fd, "writer.secret", O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK));
    if (password.fd < 0)
    {
        // Opt-in requests native handling; file access still determines writer eligibility.
        if (errno == EACCES && geteuid() != 0) return;
        throw RedisAuthenticationError("Cannot open Redis writer credential");
    }
    checkFile(password.fd, false, true);
    auto secret = readFile(password.fd, 128);
    struct Scrub {
        std::string& value;
        ~Scrub() { volatile char *p = &value[0]; for (size_t i = 0; i < value.size(); ++i) p[i] = 0; }
    } scrub{secret};
    const char *args[] = {"AUTH", username.data(), secret.data()};
    const size_t lengths[] = {4, username.size(), secret.size()};
    auto *reply = command(context, args, lengths, opaque);
    if (!reply) throw RedisError("Redis authentication transport failure", context);
    const bool accepted = reply && reply->type == REDIS_REPLY_STATUS && reply->str &&
        reply->len == 2 && std::memcmp(reply->str, "OK", 2) == 0;
    if (reply) freeReplyObject(reply);
    if (!accepted) throw RedisAuthenticationError("Redis writer authentication failed");
}
