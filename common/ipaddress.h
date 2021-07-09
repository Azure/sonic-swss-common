#ifndef __IPADDRESS__
#define __IPADDRESS__

#include <stdint.h>
#include <string.h>
#include <string>
#include <netinet/in.h>

namespace swss {

struct ip_addr_t {
    uint8_t family;
    union {
        uint32_t ipv4_addr;
        unsigned char ipv6_addr[16];
    } ip_addr;
};

class IpAddress
{
public:
    IpAddress() = default;
    IpAddress(const ip_addr_t &ip) : m_ip(ip) {}
    IpAddress(uint32_t ip);
    IpAddress(const std::string &ipStr);

    inline bool isV4() const
    {
        return m_ip.family == AF_INET;
    }

    inline bool isV6() const
    {
        return m_ip.family == AF_INET6;
    }

    inline bool isZero() const
    {
        unsigned char zerobuf[16] = { 0 };

        return ((m_ip.family == AF_INET && m_ip.ip_addr.ipv4_addr == 0) ||
                (m_ip.family == AF_INET6 && memcmp(&m_ip.ip_addr.ipv6_addr, zerobuf, 16) == 0));
    }

    inline ip_addr_t getIp() const
    {
        return m_ip;
    }

    inline uint32_t getV4Addr() const
    {
        return m_ip.ip_addr.ipv4_addr;
    }

    inline const unsigned char* getV6Addr() const
    {
        return m_ip.ip_addr.ipv6_addr;
    }

    inline bool operator<(const IpAddress &o) const
    {
        if (m_ip.family != o.m_ip.family)
           return m_ip.family < o.m_ip.family;

        if (m_ip.family == AF_INET)
            return m_ip.ip_addr.ipv4_addr <
                   o.m_ip.ip_addr.ipv4_addr;

        return (memcmp(&m_ip.ip_addr.ipv6_addr, &o.m_ip.ip_addr.ipv6_addr, 16) < 0);
    }

    inline bool operator==(const IpAddress &o) const
    {
        return m_ip.family == o.m_ip.family &&
               ((m_ip.family == AF_INET && m_ip.ip_addr.ipv4_addr == o.m_ip.ip_addr.ipv4_addr) ||
                (m_ip.family == AF_INET6 && memcmp(&m_ip.ip_addr.ipv6_addr, o.m_ip.ip_addr.ipv6_addr, 16) == 0)
               );
    }

    inline bool operator!=(const IpAddress &o) const
    {
        return (! (*this == o) );
    }

    std::string to_string() const;

    enum AddrScope {
        GLOBAL_SCOPE,
        LINK_SCOPE,
        HOST_SCOPE,
        MCAST_SCOPE
    };

    IpAddress::AddrScope getAddrScope() const;

private:
    struct ip_addr_t m_ip;
};

}

#endif
