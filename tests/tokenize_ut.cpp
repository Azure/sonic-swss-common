#include "common/ipaddress.h"
#include "common/ipaddresses.h"
#include "common/tokenize.h"

#include "gtest/gtest.h"

using namespace std;
using namespace swss;

TEST(TOKENIZE, basic)
{
    string str1 = "str1";
    string str2 = "str2";

    vector<string> origin;
    vector<string> result;

    origin.push_back(str1);
    result = tokenize("str1", ':');

    // Test1: check tokenize works on single token
    EXPECT_EQ(origin, result);

    origin.push_back(str2);
    result = tokenize("str1:str2", ':');

    // Test2: check tokenize works on multiple tokens
    EXPECT_EQ(origin, result);
}

TEST(TOKENIZE, edge_cases)
{
    EXPECT_EQ(tokenize("", ','), vector<string>({}));
    EXPECT_EQ(tokenize("a", ','), vector<string>({"a"}));
    EXPECT_EQ(tokenize("a,b", ','), vector<string>({"a", "b"}));
    EXPECT_EQ(tokenize("a,b,", ','), vector<string>({"a", "b"}));
    EXPECT_EQ(tokenize(",a", ','), vector<string>({"", "a"}));
    EXPECT_EQ(tokenize("a,,b", ','), vector<string>({"a", "", "b"}));
    EXPECT_EQ(tokenize(",", ','), vector<string>({""}));
    EXPECT_EQ(tokenize(",,", ','), vector<string>({"", ""}));
    EXPECT_EQ(tokenize("10.0.0.1,10.0.0.2", ','), vector<string>({"10.0.0.1", "10.0.0.2"}));
    EXPECT_EQ(tokenize("Ethernet0,Ethernet4", ','), vector<string>({"Ethernet0", "Ethernet4"}));
}

TEST(TOKENIZE, IP)
{
    IpAddresses origin("192.168.0.1,192.168.0.2");

    IpAddress ip1("192.168.0.1");
    IpAddress ip2("192.168.0.2");

    IpAddresses result;
    result.add(ip1);
    result.add(ip2);

    EXPECT_EQ(origin, result);
}

TEST(TOKENIZE, IP_2)
{
    IpAddresses origin("192.168.0.1");

    IpAddress ip("192.168.0.1");

    IpAddresses result;
    result.add(ip);

    EXPECT_EQ(origin, result);
}

TEST(TOKENIZEFIRST, zero)
{
    string key("Hello world!");
    auto tokens = tokenize(key, ' ', 0);
    EXPECT_EQ(tokens[0], key);
}

TEST(TOKENIZEFIRST, out_of_bound)
{
    string key("Hello world!");
    auto tokens = tokenize(key, ' ', 999);
    EXPECT_EQ(tokens[0], "Hello");
    EXPECT_EQ(tokens[1], "world!");
    EXPECT_EQ(tokens.size(), (size_t) 2);
}

TEST(TOKENIZEFIRST, MAC)
{
    string key("neigh:00:00:00:00:00:00");
    auto tokens = tokenize(key, ':', 1);

    EXPECT_EQ(tokens[0], "neigh");
    EXPECT_EQ(tokens[1], "00:00:00:00:00:00");
}

TEST(TOKENIZEFIRST, IPv6)
{
    string key("NEIGH_TABLE:lo:fc00::79");
    auto tokens = tokenize(key, ':', 1);

    EXPECT_EQ(tokens[0], "NEIGH_TABLE");

    tokens = tokenize(tokens[1], ':', 1);

    EXPECT_EQ(tokens[0], "lo");
    EXPECT_EQ(tokens[1], "fc00::79");
}

TEST(TOKENIZEFIRST, IPv6_2)
{
    string key("NEIGH_TABLE:lo:fc00::79");

    auto tokens = tokenize(key, ':', 2);

    EXPECT_EQ(tokens[0], "NEIGH_TABLE");
    EXPECT_EQ(tokens[1], "lo");
    EXPECT_EQ(tokens[2], "fc00::79");
}

TEST(TOKENIZEFISRT, not_found)
{
    string key("neigh:00:00:00:00:00:00");
    auto tokens = tokenize(key, '|', 1);

    EXPECT_EQ(tokens[0], key);

    string key_2("neigh");
    auto tokens_2 = tokenize(key_2, ':', 1);

    EXPECT_EQ(tokens_2[0], key_2);
}
