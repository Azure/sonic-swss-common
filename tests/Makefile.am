bin_PROGRAMS += tests/tests

CFLAGS_GTEST =
LDADD_GTEST = -L/usr/src/gtest -lgtest -lgtest_main -lgmock -lgmock_main

tests_tests_SOURCES = tests/redis_ut.cpp                \
                      tests/redis_piped_ut.cpp          \
                      tests/redis_command_ut.cpp        \
                      tests/redis_state_ut.cpp          \
                      tests/redis_piped_state_ut.cpp    \
                      tests/tokenize_ut.cpp             \
                      tests/json_ut.cpp                 \
                      tests/ntf_ut.cpp                  \
                      tests/netdispatcher_ut.cpp         \
                      tests/ipaddress_ut.cpp            \
                      tests/ipprefix_ut.cpp             \
                      tests/interface_ut.cpp            \
                      tests/vrf_ut.cpp                  \
                      tests/macaddress_ut.cpp           \
                      tests/converter_ut.cpp            \
                      tests/exec_ut.cpp                 \
                      tests/redis_subscriber_state_ut.cpp \
                      tests/selectable_priority.cpp       \
                      tests/warm_restart_ut.cpp         \
                      tests/redis_multi_db_ut.cpp       \
                      tests/logger_ut.cpp               \
                      common/loglevel.cpp               \
                      tests/loglevel_ut.cpp             \
                      tests/redis_multi_ns_ut.cpp       \
                      tests/redis_smartswitch_ut.cpp    \
                      tests/fdb_flush.cpp               \
                      tests/stringutility_ut.cpp        \
                      tests/redisutility_ut.cpp         \
                      tests/boolean_ut.cpp              \
                      tests/status_code_util_test.cpp   \
                      tests/saiaclschema_ut.cpp         \
                      tests/notification_queue_ut.cpp   \
                      tests/countertable_ut.cpp         \
                      tests/timer_ut.cpp                \
                      tests/cli_ut.cpp                  \
                      tests/events_common_ut.cpp        \
                      tests/events_service_ut.cpp       \
                      tests/events_ut.cpp               \
                      tests/restart_waiter_ut.cpp       \
                      tests/redis_table_waiter_ut.cpp   \
                      tests/binary_serializer_ut.cpp    \
                      tests/zmq_state_ut.cpp            \
                      tests/zmq_route_ut.cpp            \
                      tests/profileprovider_ut.cpp      \
                      tests/c_api_ut.cpp                \
                      tests/test_sonicv2connector_c_api.cpp \
                      tests/performancetimer_ut.cpp     \
                      tests/timestamp_ut.cpp            \
                      tests/main.cpp

tests_tests_CFLAGS = $(DBGFLAGS) $(AM_CFLAGS) $(CFLAGS_COMMON) $(CFLAGS_GTEST) $(LIBNL_CFLAGS)
tests_tests_CPPFLAGS = $(DBGFLAGS) $(AM_CFLAGS) $(CFLAGS_COMMON) $(CFLAGS_GTEST) $(LIBNL_CFLAGS) -fno-access-control
tests_tests_LDADD = $(LDADD_GTEST) -lpthread common/libswsscommon.la $(LIBNL_LIBS) $(CODE_COVERAGE_LIBS) sonic-db-cli/libsonicdbcli.la -lzmq -luuid -lboost_serialization

if YANGMODS
tests_tests_SOURCES += tests/defaultvalueprovider_ut.cpp
endif
