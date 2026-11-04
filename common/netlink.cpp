#include "common/logger.h"
#include "common/netlink.h"
#include "common/netdispatcher.h"

#include <errno.h>

#include <system_error>
#include <cstring>

using namespace swss;
using namespace std;

NetLink::NetLink(int pri) :
    Selectable(pri), m_socket(NULL)
{
    m_socket = nl_socket_alloc();
    if (!m_socket)
    {
        SWSS_LOG_ERROR("Unable to allocated netlink socket");
        throw system_error(make_error_code(errc::address_not_available),
                           "Unable to allocated netlink socket");
    }

    nl_socket_disable_seq_check(m_socket);
    nl_socket_modify_cb(m_socket, NL_CB_VALID, NL_CB_CUSTOM, onNetlinkMsg, this);

    int err = nl_connect(m_socket, NETLINK_ROUTE);
    if (err < 0)
    {
        SWSS_LOG_ERROR("Unable to connect netlink socket: %s", nl_geterror(err));
        nl_socket_free(m_socket);
        m_socket = NULL;
        throw system_error(make_error_code(errc::address_not_available),
                           "Unable to connect netlink socket");
    }

    nl_socket_set_nonblocking(m_socket);
    /* Set socket buffer size to 16MB */
    nl_socket_set_buffer_size(m_socket, 16777216, 0);
}

NetLink::~NetLink()
{
    if (m_socket != NULL)
    {
        nl_close(m_socket);
        nl_socket_free(m_socket);
    }
}

void NetLink::registerGroup(int rtnlGroup)
{
    int err = nl_socket_add_membership(m_socket, rtnlGroup);
    if (err < 0)
    {
        SWSS_LOG_ERROR("Unable to register to group %d: %s", rtnlGroup,
                       nl_geterror(err));
        throw system_error(make_error_code(errc::address_not_available),
                           "Unable to register group");
    }
}

void NetLink::dumpRequest(int rtmGetCommand, int family)
{
    int err = -1;
    if ((rtmGetCommand == RTM_GETNEIGH) && (family == AF_BRIDGE))
    {
        struct ifinfomsg hdr;
        hdr.ifi_family = static_cast<unsigned char>(family);
        struct nl_msg *msg;

        msg = nlmsg_alloc_simple(RTM_GETNEIGH, NLM_F_REQUEST | NLM_F_DUMP);
        if (!msg)
        {
            err = -NLE_NOMEM;
        }
        else
        {
            if (nlmsg_append(msg, &hdr, sizeof(hdr), NLMSG_ALIGNTO) < 0)
            {
                nlmsg_free(msg);
                err = -NLE_MSGSIZE;
            }
            else
            {
                err = nl_send_auto(m_socket, msg);
                if (err > 0)
                {
                    err = 0;
                }
                else
                {
                    err = -NLE_INVAL;
                }
            }
        }
    }
    else
    {
        err = nl_rtgen_request(m_socket, rtmGetCommand, family, NLM_F_DUMP);
    }
    if (err < 0)
    {
        SWSS_LOG_ERROR("Unable to request dump on group %d: %s", rtmGetCommand,
                       nl_geterror(err));
        throw system_error(make_error_code(errc::address_not_available),
                           "Unable to request dump");
    }
}

int NetLink::getFd()
{
    return nl_socket_get_fd(m_socket);
}

uint64_t NetLink::readData()
{
    int err;

    do
    {
        err = nl_recvmsgs_default(m_socket);
    }
    while(err == -NLE_INTR); // Retry if the process was interrupted by a signal

    if (err < 0)
    {
        if (err == -NLE_NOMEM)
            SWSS_LOG_ERROR("netlink reports out of memory on reading a netlink socket. High possibility of a lost message");
        else if (err == -NLE_AGAIN)
            SWSS_LOG_DEBUG("netlink reports NLE_AGAIN on reading a netlink socket");
        else
            SWSS_LOG_ERROR("netlink reports an error=%d on reading a netlink socket", err);
    }
    return 0;
}

int NetLink::onNetlinkMsg(struct nl_msg *msg, void *arg)
{
    NetDispatcher::getInstance().onNetlinkMessage(msg);
    return NL_OK;
}
