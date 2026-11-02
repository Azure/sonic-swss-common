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
                      tests/configdb_ut.cpp             \
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

# Compile only the policy reader again; reuse the other production objects.
# These targets are never installed or included in the Debian packages.
check_LTLIBRARIES = tests/libswsscommon-auth-test.la
check_PROGRAMS = tests/redis_auto_auth_tests
REDIS_AUTH_TEST_DIR = $(abs_builddir)/tests/redis-auth
redis_auth_test_CPPFLAGS = -DREDIS_AUTH_DIR='"$(REDIS_AUTH_TEST_DIR)/policy"'
redis_auth_test_objects = $(filter-out common/libswsscommon_la-dbconnector.lo,$(common_libswsscommon_la_OBJECTS))

tests_libswsscommon_auth_test_la_SOURCES = common/dbconnector.cpp
tests_libswsscommon_auth_test_la_CPPFLAGS = $(common_libswsscommon_la_CPPFLAGS) $(redis_auth_test_CPPFLAGS)
tests_libswsscommon_auth_test_la_CXXFLAGS = $(common_libswsscommon_la_CXXFLAGS)
tests_libswsscommon_auth_test_la_LIBADD = $(redis_auth_test_objects) $(common_libswsscommon_la_LIBADD)
tests_libswsscommon_auth_test_la_DEPENDENCIES = $(redis_auth_test_objects)
tests_libswsscommon_auth_test_la_LDFLAGS = $(common_libswsscommon_la_LDFLAGS) -rpath $(abs_builddir)/tests/.libs

tests_redis_auto_auth_tests_SOURCES = tests/redis_auto_auth_ut.cpp tests/main.cpp
tests_redis_auto_auth_tests_CPPFLAGS = $(tests_tests_CPPFLAGS) $(redis_auth_test_CPPFLAGS)
tests_redis_auto_auth_tests_LDADD = $(LDADD_GTEST) tests/libswsscommon-auth-test.la

# Each runner invocation selects a fresh directory, so rebuild the two objects
# that embed it even when make finds artifacts from an earlier run.
common/tests_libswsscommon_auth_test_la-dbconnector.lo tests/redis_auto_auth_tests-redis_auto_auth_ut.o: FORCE

EXTRA_DIST += tests/run-redis-auth-tests.sh tests/test_redis_auto_auth.py
