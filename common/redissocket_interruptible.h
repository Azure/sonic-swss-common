#pragma once
#include <string>
namespace swss {
// Single budget and cancellation during connect and AUTH, for native dialers.
int openRedisUnixSocketInterruptible(const std::string&, unsigned int, bool, int);
}
