#pragma once

#include "selectable.h"

#include <netlink/netlink.h>
#include <netlink/route/rtnl.h>

namespace swss
{
    class NetLink :
        public Selectable
    {
        public:

            NetLink(int pri = 0);
            virtual ~NetLink();

            void registerGroup(int rtnlGroup);
            void dumpRequest(int rtmGetCommand, int family = AF_UNSPEC);

            int getFd() override;
            uint64_t readData() override;

        private:

            static int onNetlinkMsg(struct nl_msg *msg, void *arg);

            struct nl_sock *m_socket;
    };
}
