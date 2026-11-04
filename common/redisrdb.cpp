#include "redisrdb.h"
#include "redissocket.h"
#include <algorithm>
#include <chrono>
#include <climits>
#include <cstdio>
#include <fcntl.h>
#include <poll.h>
#include <stdexcept>
#include <system_error>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace
{
struct File { int fd; ~File() { if (fd >= 0) close(fd); } };
}

void swss::exportRedisRdb(const std::string& path, const std::string& destination,
                         unsigned int timeout_ms, bool authenticate)
{
    if (destination.empty() || destination.find('\0') != std::string::npos)
        throw std::invalid_argument("Invalid RDB destination");
    using Clock = std::chrono::steady_clock;
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    File connection{openRedisUnixSocket(path, timeout_ms, authenticate)};
    const int flags = fcntl(connection.fd, F_GETFL);
    if (flags < 0 || fcntl(connection.fd, F_SETFL, flags | O_NONBLOCK) < 0)
        throw std::system_error(errno, std::generic_category(), "RDB socket mode");
    const auto wait = [&](short events) {
        for (;;)
        {
            int timeout = -1;
            if (timeout_ms)
            {
                const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
                if (left < 0) throw std::system_error(std::make_error_code(std::errc::timed_out));
                timeout = static_cast<int>(std::min<long long>(left + 1, INT_MAX));
            }
            pollfd event{connection.fd, events, 0};
            const int rc = poll(&event, 1, timeout);
            if (rc < 0 && errno == EINTR) continue;
            if (rc < 0) throw std::system_error(errno, std::generic_category(), "RDB poll");
            if (rc) return;
        }
    };
    const auto receive = [&](char *buffer, size_t size) {
        for (;;)
        {
            wait(POLLIN);
            const auto count = read(connection.fd, buffer, size);
            if (count < 0 && (errno == EINTR || errno == EAGAIN)) continue;
            if (count <= 0) throw std::runtime_error("RDB transfer truncated");
            return static_cast<size_t>(count);
        }
    };
    const std::string command = "SYNC\r\n";
    size_t sent = 0;
    while (sent < command.size())
    {
        wait(POLLOUT);
        const auto count = send(connection.fd, command.data() + sent, command.size() - sent, MSG_NOSIGNAL);
        if (count < 0 && (errno == EINTR || errno == EAGAIN)) continue;
        if (count <= 0) throw std::runtime_error("Cannot request RDB snapshot");
        sent += static_cast<size_t>(count);
    }
    std::string header;
    do {
        header.clear();
        for (;;)
        {
            char byte;
            receive(&byte, 1);
            if (byte == '\n') break;
            if (header.size() >= 128) throw std::runtime_error("Invalid RDB framing");
            header += byte;
        }
    } while (header.empty() || header == "\r");
    if (header.size() < 3 || header.front() != '$' || header.back() != '\r')
        throw std::runtime_error("RDB export requires length framing");
    const auto digits = header.substr(1, header.size() - 2);
    if (digits.empty() || digits.find_first_not_of("0123456789") != std::string::npos)
        throw std::runtime_error("RDB export requires length framing");
    auto remaining = std::stoull(digits);
    if (!remaining) throw std::runtime_error("Empty RDB snapshot");
    std::vector<char> temporary(destination.begin(), destination.end());
    const char suffix[] = ".XXXXXX";
    temporary.insert(temporary.end(), suffix, suffix + sizeof(suffix));
    File output{mkostemp(temporary.data(), O_CLOEXEC)};
    if (output.fd < 0) throw std::system_error(errno, std::generic_category(), "RDB output");
    try
    {
        char buffer[65536];
        while (remaining)
        {
            const auto count = receive(buffer, static_cast<size_t>(std::min<unsigned long long>(remaining, sizeof(buffer))));
            size_t written = 0;
            while (written < count)
            {
                const auto size = write(output.fd, buffer + written, count - written);
                if (size < 0 && errno == EINTR) continue;
                if (size <= 0) throw std::runtime_error("Cannot write RDB snapshot");
                written += static_cast<size_t>(size);
            }
            remaining -= count;
        }
        if (fsync(output.fd) || rename(temporary.data(), destination.c_str()))
            throw std::system_error(errno, std::generic_category(), "RDB replacement");
    }
    catch (...) { unlink(temporary.data()); throw; }
}
