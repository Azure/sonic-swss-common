#include <iostream>
#include <memory>
#include <thread>
#include <algorithm>
#include <deque>
#include <regex>
#include "gtest/gtest.h"
#include "common/events_common.h"

using namespace std;

const char *test_cfg_data = "{\
\"events\" : {  \
    \"xsub_path\": \"xsub_path\", \
    \"req_rep_path\": \"req_rep_path\" \
    }\
}";


TEST(events_common, get_config)
{
    EXPECT_EQ(string("tcp://240.127.1.1:5570"), get_config(string(XSUB_END_KEY)));
    EXPECT_EQ(string("tcp://240.127.1.1:5572"), get_config(string(REQ_REP_END_KEY)));
    EXPECT_EQ(string("5"), get_config(string(STATS_UPD_SECS)));

    ofstream tfile;
    const char *tfile_name = "/tmp/init_cfg.json";
    tfile.open (tfile_name);
    tfile << test_cfg_data << "\n";
    tfile.close();

    read_init_config(tfile_name);

    EXPECT_EQ(string(XSUB_END_KEY), get_config(string(XSUB_END_KEY)));
    EXPECT_EQ(string(REQ_REP_END_KEY), get_config(string(REQ_REP_END_KEY)));
    EXPECT_EQ(string("5"), get_config(string(STATS_UPD_SECS)));

    EXPECT_EQ(100, get_config_data(CACHE_MAX_CNT, 100));

    read_init_config(NULL);
    EXPECT_EQ(string("tcp://240.127.1.1:5570"), get_config(string(XSUB_END_KEY)));
}

void
events_validate_ts(const string s)
{
    string reg = "^(?:[1-9]\\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d\\.[0-9]{1,6}Z$";

    EXPECT_TRUE(regex_match (s, regex(reg)));
}

TEST(events_common, ts)
{
    string s = get_timestamp();

    cout << s << "\n";
    events_validate_ts(s);
}

/*
 * Tests serialize, deserialize, map_to_zmsg, zmsg_to_map
 * zmq_read_part & zmq_send_part while invoking zmq_message_send &
 * zmq_message_read. 
 */
TEST(events_common, send_recv)
{
#if 0
    {
        /* Direct log messages to stdout */
        string dummy, op("STDOUT");
        swss::Logger::swssOutputNotify(dummy, op);
        swss::Logger::setMinPrio(swss::Logger::SWSS_DEBUG);
    }
#endif

    char *path = "tcp://127.0.0.1:5570";
    void *zmq_ctx = zmq_ctx_new();
    void *sock_p0 = zmq_socket (zmq_ctx, ZMQ_PAIR);
    EXPECT_EQ(0, zmq_connect (sock_p0, path));

    void *sock_p1 = zmq_socket (zmq_ctx, ZMQ_PAIR);
    EXPECT_EQ(0, zmq_bind (sock_p1, path));

    string source("Hello"), source1;

    map<string, string> m = {{"foo", "bar"}, {"hello", "world"}, {"good", "day"}};
    map<string, string> m1;

    EXPECT_EQ(0, zmq_message_send(sock_p0, source, m));

    EXPECT_EQ(0, zmq_message_read(sock_p1, 0, source1, m1));

    EXPECT_EQ(source, source1);
    EXPECT_EQ(m, m1);
    zmq_close(sock_p0);
    zmq_close(sock_p1);
    zmq_ctx_term(zmq_ctx);
}

TEST(events_common, send_recv_control_character)
{
#if 0
    {
        /* Direct log messages to stdout */
        string dummy, op("STDOUT");
        swss::Logger::swssOutputNotify(dummy, op);
        swss::Logger::setMinPrio(swss::Logger::SWSS_DEBUG);
    }
#endif

    char *path = "tcp://127.0.0.1:5570";
    void *zmq_ctx = zmq_ctx_new();
    void *sock_p0 = zmq_socket (zmq_ctx, ZMQ_PAIR);
    EXPECT_EQ(0, zmq_connect (sock_p0, path));

    void *sock_p1 = zmq_socket (zmq_ctx, ZMQ_PAIR);
    EXPECT_EQ(0, zmq_bind (sock_p1, path));

    string source;
    map<string, string> m;

    // Subscription based control character test
    zmq_msg_t sub_msg;
    zmq_msg_init_size(&sub_msg, 1);
    *(char*)zmq_msg_data(&sub_msg) = 0x01;
    EXPECT_EQ(1, zmq_msg_send(&sub_msg, sock_p0, 0));
    zmq_msg_close(&sub_msg);
    // First part will be read only and will return as 0, but will not be deserialized event
    EXPECT_EQ(0, zmq_message_read(sock_p1, 0, source, m));
    EXPECT_EQ("", source);
    EXPECT_EQ(0, m.size());

   // Non-subscription based control character test
    zmq_msg_t ctrl_msg;
    zmq_msg_init_size(&ctrl_msg, 1);
    *(char*)zmq_msg_data(&ctrl_msg) = 0x07;
    EXPECT_EQ(1, zmq_msg_send(&ctrl_msg, sock_p0, 0));
    zmq_msg_close(&ctrl_msg);
    // First part will be read only and will return as 0, but will not be deserialized event
    EXPECT_EQ(0, zmq_message_read(sock_p1, 0, source, m));
    EXPECT_EQ("", source);
    EXPECT_EQ(0, m.size());

    zmq_close(sock_p0);
    zmq_close(sock_p1);
    zmq_ctx_term(zmq_ctx);
}
