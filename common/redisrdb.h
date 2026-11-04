#pragma once
#include <string>

namespace swss
{
// Export a length-framed SYNC snapshot through the native Unix factory.
// Replace destination only after a complete, fsynced transfer. New files use 0600.
void exportRedisRdb(const std::string& path, const std::string& destination,
                    unsigned int timeout_ms = 60000, bool authenticate = false);
}
