#include <iostream>
#include <memory>
#include <thread>
#include <algorithm>
#include <deque>
#include <regex>
#include "gtest/gtest.h"
#include "common/events_common.h"
#include "common/events_service.h"

using namespace std;

static bool do_terminate = false;
static void *zmq_ctx = NULL;
static event_service service_cl, service_svr;
static int server_rd_code, server_ret;
static event_serialized_lst_t server_rd_lst, server_wr_lst;

const char *test_cfg_data_localhost = "{\
\"events\" : {  \
    \"xsub_path\": \"127.0.0.1:5570\", \
    \"req_rep_path\": \"127.0.0.1:5572\" \
    }\
}";


/* Mimic the eventd service that handles service requests via dedicated thread */
void serve_commands()
{
    int code;
    event_serialized_lst_t lst, opt_lst;

    ofstream tfile;
    const char *tfile_name = "/tmp/init_cfg.json";
    tfile.open (tfile_name);
    tfile << test_cfg_data_localhost << "\n";
    tfile.close();

    read_init_config(tfile_name);

    EXPECT_EQ(0, service_svr.init_server(zmq_ctx, 1000));
    while(!do_terminate) {
        if (0 != service_svr.channel_read(code, lst)) {
            /* check client service status, before blocking on read */
            continue;
        }
        server_rd_code = code;
        server_rd_lst = lst;

        switch(code) {
            case EVENT_CACHE_INIT:
                server_ret = 0;
                server_wr_lst.clear();
                break;
            case EVENT_CACHE_START:
                server_ret = 0;
                server_wr_lst.clear();
                break;
            case EVENT_CACHE_STOP:
                server_ret = 0;
                server_wr_lst.clear();
                break;
            case EVENT_CACHE_READ:
                server_ret = 0;
                server_wr_lst = { "rerer", "rrrr", "cccc" };
                break;
            case EVENT_ECHO:
                server_ret = 0;
                server_wr_lst = lst;
                break;
            case EVENT_OPTIONS:
                server_ret = 0;
                if (lst.empty()) {
                    server_wr_lst = opt_lst;
                }
                else {
                    opt_lst = lst;
                    server_wr_lst.clear();
                }
                break;
            default:
                EXPECT_TRUE(false);
                server_ret = -1;
                break;
        }
        EXPECT_EQ(0, service_svr.channel_write(server_ret, server_wr_lst));
    }
    service_svr.close_service();
    read_init_config(NULL);
    EXPECT_FALSE(service_svr.is_active());
}


TEST(events_common, cache_cmds)
{
    event_serialized_lst_t lst_start, lst;

#if 0
    {
        /* Direct log messages to stdout */
        string dummy, op("STDOUT");
        swss::Logger::swssOutputNotify(dummy, op);
        swss::Logger::setMinPrio(swss::Logger::SWSS_DEBUG);
    }
#endif

    zmq_ctx = zmq_ctx_new();
    EXPECT_TRUE(NULL != zmq_ctx);

    /* Start mock service in a separate thread */
    thread thr(&serve_commands);

    EXPECT_EQ(0, service_cl.init_client(zmq_ctx));

    EXPECT_EQ(0, service_cl.cache_init());
    /* Cache init sends echo upon success */
    EXPECT_EQ(EVENT_ECHO, server_rd_code);
    EXPECT_TRUE(server_rd_lst.empty());
    EXPECT_EQ(server_rd_lst, server_wr_lst);

    /*
     * Bunch of serialized internal_event_t as strings
     * Sending random for test purpose 
     */
    lst_start = event_serialized_lst_t(
            { "hello", "world", "ok" });
    EXPECT_EQ(0, service_cl.cache_start(lst_start));
    EXPECT_EQ(EVENT_CACHE_START, server_rd_code);
    EXPECT_EQ(lst_start, server_rd_lst);
    EXPECT_TRUE(server_wr_lst.empty());

    EXPECT_EQ(0, service_cl.cache_stop());
    EXPECT_EQ(EVENT_CACHE_STOP, server_rd_code);
    EXPECT_TRUE(server_rd_lst.empty());
    EXPECT_TRUE(server_wr_lst.empty());

    EXPECT_EQ(0, service_cl.cache_read(lst));
    EXPECT_EQ(EVENT_CACHE_READ, server_rd_code);
    EXPECT_TRUE(server_rd_lst.empty());
    EXPECT_EQ(server_wr_lst, lst);

    string s("hello"), s1;
    EXPECT_EQ(0, service_cl.echo_send(s));
    EXPECT_EQ(0, service_cl.echo_receive(s1));
    EXPECT_EQ(EVENT_ECHO, server_rd_code);
    EXPECT_FALSE(server_rd_lst.empty());
    EXPECT_EQ(s1, s);

    string sopt("{\"HEARTBEAT_INTERVAL\": 2000, \"OFFLINE_CACHE_SIZE\": 500}");
    char rd_opt[100];
    rd_opt[0] = 0;
    EXPECT_EQ(0, service_cl.global_options_set(sopt.c_str()));
    EXPECT_LT(0, service_cl.global_options_get(rd_opt, (int)sizeof(rd_opt)));
    EXPECT_EQ(EVENT_OPTIONS, server_rd_code);
    EXPECT_EQ(sopt, string(rd_opt));

    do_terminate = true;
    service_cl.close_service();
    EXPECT_FALSE(service_cl.is_active());
    thr.join();
    zmq_ctx_term(zmq_ctx);
}


