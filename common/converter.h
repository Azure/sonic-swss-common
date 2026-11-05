#ifndef __CONVERTER__
#define __CONVERTER__

#include <algorithm>
#include <utility>
#include <exception>
#include <stdexcept>
#include <limits>
#include <string>
#include <inttypes.h>

namespace swss {

static inline uint64_t __to_uint64(const std::string &str, uint64_t min = std::numeric_limits<uint64_t>::min(), uint64_t max = std::numeric_limits<uint64_t>::max())
{
    size_t idx = 0;
    // stoul() accepts a leading '-' and silently wraps it (e.g. "-1" -> ULONG_MAX);
    // reject a negative value so unsigned conversion fails instead of returning garbage.
    if (str.find('-') != std::string::npos)
    {
        throw std::invalid_argument("failed to convert " + str + " value to uint64_t type");
    }
    uint64_t ret = stoul(str, &idx, 0);
    if (str[idx])
    {
        throw std::invalid_argument("failed to convert " + str + " value to uint64_t type");
    }

    if (ret < min || ret > max)
    {
        throw std::invalid_argument("failed to convert " + str + " value is not in range " + std::to_string(min) + " - " + std::to_string(max));
    }

    return ret;
}

static inline int64_t __to_int64(const std::string &str, int64_t min = std::numeric_limits<int64_t>::min(), int64_t max = std::numeric_limits<int64_t>::max())
{
    size_t idx = 0;
    int64_t ret = stol(str, &idx, 0);
    if (str[idx])
    {
        throw std::invalid_argument("failed to convert " + str + " value to int64_t type");
    }

    if (ret < min || ret > max)
    {
        throw std::invalid_argument("failed to convert " + str + " value is not in range " + std::to_string(min) + " - " + std::to_string(max));
    }

    return ret;
}

template <typename T>
T to_int(const std::string &str, T min = std::numeric_limits<T>::min(), T max = std::numeric_limits<T>::max())
{
    static_assert(std::is_signed<T>::value, "Signed integer is expected");
    static_assert(std::numeric_limits<T>::max() <= std::numeric_limits<int64_t>::max(), "Type is too big");

    return static_cast<T>(__to_int64(str, min, max));
}

template <typename T>
T to_uint(const std::string &str, T min = std::numeric_limits<T>::min(), T max = std::numeric_limits<T>::max())
{
    static_assert(std::is_unsigned<T>::value, "Unsigned integer is expected");
    static_assert(std::numeric_limits<T>::max() <= std::numeric_limits<uint64_t>::max(), "Type is too big");

    return static_cast<T>(__to_uint64(str, min, max));
}

static inline std::string to_upper(std::string str)
{
    transform(str.begin(), str.end(), str.begin(), ::toupper);
    return str;
}

}

#endif /* __CONVERTER__ */
