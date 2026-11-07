
bin_PROGRAMS += test_zmq_memory/test_zmq_memory
test_zmq_memory_test_zmq_memory_SOURCES = test_zmq_memory/main.cpp
test_zmq_memory_test_zmq_memory_CFLAGS = $(DBGFLAGS) $(AM_CFLAGS) $(CFLAGS_COMMON)
test_zmq_memory_test_zmq_memory_CPPFLAGS = $(DBGFLAGS) $(AM_CPPFLAGS) $(CFLAGS_COMMON)
test_zmq_memory_test_zmq_memory_LDADD = common/libswsscommon.la -lpthread
# build with tcmalloc
# test_zmq_memory_test_zmq_memory_LDADD = common/libswsscommon.la -lpthread -ltcmalloc
test_zmq_memory_test_zmq_memory_LDFLAGS = -Wl,-z,now $(LDFLAGS)
