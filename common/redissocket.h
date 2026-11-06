#pragma once
#include <string>

namespace swss
{
// Return an owned close-on-exec Unix descriptor, without selecting a database.
// One timeout covers connect and AUTH; zero is unlimited.
int openRedisUnixSocket(const std::string& path, unsigned int timeout_ms = 0,
                        bool authenticate = false);
}
