lib_LTLIBRARIES += common/libswsscommon.la

dist_swss_DATA = \
    common/consumer_state_table_pops.lua \
    common/consumer_table_pops.lua \
    common/producer_state_table_apply_view.lua \
    common/table_dump.lua \
    common/portcounter.lua \
    common/fdb_flush.lua \
    common/fdb_flush.v2.lua \
    common/fdb_flush_scan.lua

dist_swsscommon_DATA= common/database_config.json

swssdir = $(datadir)/swss
swsscommondir = /var/run/redis/sonic-db

bin_PROGRAMS += common/swssloglevel

# Generate cfg_schema.h before building the library
BUILT_SOURCES = common/cfg_schema.h

if YANGMODS
# Generate schema dynamically when yang modules are enabled
common/cfg_schema.h: FORCE
	$(AM_V_GEN)$(PYTHON3) $(srcdir)/gen_cfg_schema.py -o $@
else
# Generate minimal static schema when yang modules are disabled
common/cfg_schema.h: FORCE
	$(AM_V_GEN)echo '#ifndef CFG_SCHEMA_H' > $@; \
	echo '#define CFG_SCHEMA_H' >> $@; \
	echo '' >> $@; \
	echo '// Minimal cfg_schema.h generated when YANG modules are disabled' >> $@; \
	echo '#ifdef __cplusplus' >> $@; \
	echo 'namespace swss {' >> $@; \
	echo '#endif' >> $@; \
	echo '' >> $@; \
	echo '#ifdef __cplusplus' >> $@; \
	echo '}' >> $@; \
	echo '#endif' >> $@; \
	echo '#endif' >> $@
endif

# FORCE target to ensure cfg_schema.h is always regenerated
.PHONY: FORCE
FORCE:

common_libswsscommon_la_SOURCES = \
    common/events_common.cpp         \
    common/events_service.cpp        \
    common/events.cpp                \
    common/logger.cpp                \
    common/redisreply.cpp            \
    common/configdb.cpp              \
    common/dbconnector.cpp           \
    common/dbinterface.cpp           \
    common/sonicv2connector.cpp      \
    common/table.cpp                 \
    common/json.cpp                  \
    common/producertable.cpp         \
    common/producerstatetable.cpp    \
    common/zmqproducerstatetable.cpp \
    common/rediscommand.cpp          \
    common/redistran.cpp             \
    common/redisselect.cpp           \
    common/select.cpp                \
    common/selectableevent.cpp       \
    common/selectabletimer.cpp       \
    common/consumertable.cpp         \
    common/consumertablebase.cpp     \
    common/consumerstatetable.cpp    \
    common/zmqconsumerstatetable.cpp \
    common/zmqrouteconsumerstatetable.cpp \
    common/ipaddress.cpp             \
    common/ipprefix.cpp              \
    common/ipaddresses.cpp           \
    common/macaddress.cpp            \
    common/netdispatcher.cpp         \
    common/netmsg.cpp                \
    common/netlink.cpp               \
    common/nfnetlink.cpp             \
    common/notificationconsumer.cpp  \
    common/notificationproducer.cpp  \
    common/linkcache.cpp             \
    common/portmap.cpp               \
    common/pubsub.cpp                \
    common/tokenize.cpp              \
    common/exec.cpp                  \
    common/saiaclschema.cpp          \
    common/subscriberstatetable.cpp  \
    common/timestamp.cpp             \
    common/warm_restart.cpp          \
    common/luatable.cpp              \
    common/countertable.cpp          \
    common/redisutility.cpp          \
    common/restart_waiter.cpp        \
    common/profileprovider.cpp       \
    common/zmqclient.cpp             \
    common/zmqserver.cpp             \
    common/zmqrouteserver.cpp        \
    common/asyncdbupdater.cpp        \
    common/redis_table_waiter.cpp    \
    common/interface.h               \
    common/vrf.h                     \
    common/c-api/util.cpp                  \
    common/c-api/dbconnector.cpp           \
    common/c-api/configdbconnector.cpp     \
    common/c-api/sonicv2connector.cpp      \
    common/c-api/consumerstatetable.cpp    \
    common/c-api/producerstatetable.cpp    \
    common/c-api/subscriberstatetable.cpp  \
    common/c-api/zmqclient.cpp             \
    common/c-api/zmqserver.cpp             \
    common/c-api/zmqconsumerstatetable.cpp \
    common/c-api/zmqproducerstatetable.cpp \
    common/c-api/table.cpp                 \
    common/c-api/logger.cpp                \
    common/c-api/events.cpp                \
    common/performancetimer.cpp

common_libswsscommon_la_CXXFLAGS = $(DBGFLAGS) $(AM_CFLAGS) $(CFLAGS_COMMON) $(LIBNL_CFLAGS) $(CODE_COVERAGE_CXXFLAGS)
common_libswsscommon_la_CPPFLAGS = $(DBGFLAGS) $(AM_CFLAGS) $(CFLAGS_COMMON) $(LIBNL_CPPFLAGS) $(CODE_COVERAGE_CPPFLAGS)
common_libswsscommon_la_LIBADD = -lpthread $(LIBNL_LIBS) $(CODE_COVERAGE_LIBS) -lzmq -lboost_serialization -luuid
common_libswsscommon_la_LDFLAGS = -Wl,-z,now $(LDFLAGS)

if YANGMODS
common_libswsscommon_la_SOURCES += \
    common/defaultvalueprovider.cpp  \
    common/decoratortable.cpp        \
    common/decoratorsubscriberstatetable.cpp

common_libswsscommon_la_LIBADD += -lyang
endif

common_swssloglevel_SOURCES =  \
	common/loglevel.cpp        \
	common/loglevel_util.cpp


common_swssloglevel_CXXFLAGS = $(DBGFLAGS) $(AM_CFLAGS) $(CFLAGS_COMMON) $(CODE_COVERAGE_CXXFLAGS)
common_swssloglevel_CPPFLAGS = $(DBGFLAGS) $(AM_CFLAGS) $(CFLAGS_COMMON) $(CODE_COVERAGE_CPPFLAGS)
common_swssloglevel_LDADD = common/libswsscommon.la $(CODE_COVERAGE_LIBS)
common_swssloglevel_LDFLAGS = -Wl,-z,now $(LDFLAGS)
