#include "redissocket.h"
#include "../redissocket_interruptible.h"
#include <system_error>

SWSSResult SWSSRedisSocket_open_unix(const char *path, uint32_t timeout_ms, uint8_t authenticate,
                                    int32_t cancelFd, int32_t *outFd, int32_t *outError)
{
    if (outFd) *outFd = -1;
    if (outError) *outError = 0;
    SWSSTry({
        if (!path || !outFd || authenticate > 1) throw std::invalid_argument("Invalid native Unix factory arguments");
        try { *outFd = swss::openRedisUnixSocketInterruptible(path, timeout_ms, authenticate != 0, cancelFd); }
        catch (const std::system_error& error) {
            if (outError) *outError = error.code().value();
            throw;
        }
    });
}
