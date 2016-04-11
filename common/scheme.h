#ifndef __SCHEME__
#define __SCHEME__


#define _in_
#define _out_
#define _inout_

namespace swss {

#define APPL_DB         0
#define ASIC_DB         1

#define APP_PORT_TABLE_NAME             "PORT_TABLE"
#define APP_VLAN_TABLE_NAME             "VLAN_TABLE"
#define APP_LAG_TABLE_NAME              "LAG_TABLE"
#define APP_INTF_TABLE_NAME             "INTF_TABLE"
#define APP_NEIGH_TABLE_NAME            "NEIGH_TABLE"
#define APP_ROUTE_TABLE_NAME            "ROUTE_TABLE"

#define APP_TC_TO_QUEUE_MAP_TABLE_NAME  "TC_TO_QUEUE_MAP_TABLE"
#define APP_SCHEDULER_TABLE_NAME        "SCHEDULER_TABLE"
#define APP_DSCP_TO_TC_MAP_TABLE_NAME   "DSCP_TO_TC_MAP_TABLE"
#define APP_QUEUE_TABLE_NAME            "QUEUE_TABLE"
#define APP_PORT_QOS_MAP_TABLE_NAME     "PORT_QOS_MAP_TABLE"
#define APP_WRED_PROFILE_TABLE_NAME     "WRED_PROFILE_TABLE"




#define IPV4_NAME "IPv4"
#define IPV6_NAME "IPv6"

#define SET_COMMAND "SET"
#define DEL_COMMAND "DEL"

}

#endif
