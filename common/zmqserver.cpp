#include <unistd.h>
#include <string>
#include <deque>
#include <limits>
#include <hiredis/hiredis.h>
#include <zmq.h>
#include <pthread.h>
#include "zmqserver.h"
#include "binaryserializer.h"

using namespace std;

namespace swss {

void ZmqHandlerRegistry::registerHandler(
    const std::string& dbName,
    const std::string& tableName,
    ZmqMessageHandler* handler)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    auto dbResult = m_handlers.insert(make_pair(dbName, map<string, ZmqMessageHandler*>()));
    if (dbResult.second) {
        SWSS_LOG_DEBUG("ZmqHandlerRegistry add mapping for db: %s", dbName.c_str());
    }

    auto tableResult = dbResult.first->second.insert(make_pair(tableName, handler));
    if (tableResult.second) {
        SWSS_LOG_DEBUG("ZmqHandlerRegistry register handler for db: %s, table: %s",
                       dbName.c_str(), tableName.c_str());
    }
}

void ZmqHandlerRegistry::removeHandler(
    const std::string& dbName,
    const std::string& tableName,
    const ZmqMessageHandler* handler)
{
    // Take the same mutex that dispatch() holds across the callback. Once we
    // acquire it, no callback into this (dbName, tableName) handler is in
    // flight and no further one can start — making it safe for the caller to
    // destroy the handler object after removeHandler() returns.
    std::lock_guard<std::mutex> lock(m_mutex);

    auto dbIter = m_handlers.find(dbName);
    if (dbIter == m_handlers.end()) {
        return;
    }

    // Only remove the entry if it still points at the caller. Teardown detaches
    // twice (derived + base destructor); after the first removal the slot may
    // have been re-registered by a replacement consumer, and a key-only erase
    // here would evict that innocent replacement.
    auto tableIter = dbIter->second.find(tableName);
    if (tableIter == dbIter->second.end() || tableIter->second != handler) {
        return;
    }

    dbIter->second.erase(tableIter);
    if (dbIter->second.empty()) {
        m_handlers.erase(dbIter);
    }

    SWSS_LOG_DEBUG("ZmqHandlerRegistry removed handler for db: %s, table: %s",
                   dbName.c_str(), tableName.c_str());
}

void ZmqHandlerRegistry::flushDirtyHandlers(DirtyHandlerMap& dirty,
                                            std::chrono::steady_clock::time_point cutoff)
{
    if (dirty.empty()) {
        return;
    }

    // Same mutex as removeHandler(): while we hold it no handler can be
    // unregistered (and therefore destroyed) underneath us, and any handler
    // unregistered before we got here is no longer in the registry, so it is
    // skipped rather than notified through a dangling pointer.
    std::lock_guard<std::mutex> lock(m_mutex);

    // Pass 1: walk the registry and notify the live handlers that are dirty
    // and overdue. Registry membership is the liveness check. A handler
    // registered under several (db, table) pairs would be notified once per
    // registration, which is harmless -- eventfd writes coalesce.
    for (auto& dbEntry : m_handlers) {
        for (auto& tableEntry : dbEntry.second) {
            auto it = dirty.find(tableEntry.second);
            if (it != dirty.end() && it->second <= cutoff) {
                tableEntry.second->notifyPending();
            }
        }
    }

    // Pass 2: erase every overdue entry, live or dead. Dead entries must not
    // linger: a dangling pointer left in `dirty` would keep the poll loop in
    // its short-timeout mode forever.
    for (auto it = dirty.begin(); it != dirty.end();) {
        if (it->second <= cutoff) {
            it = dirty.erase(it);
        } else {
            ++it;
        }
    }
}

ZmqMessageHandler* ZmqHandlerRegistry::dispatch(
    const std::string& dbName,
    const std::string& tableName,
    const std::vector<std::shared_ptr<KeyOpFieldsValuesTuple>>& kcos)
{
    // Hold the mutex for the duration of the callback. Concurrent
    // removeHandler() on this (dbName, tableName) blocks until we return,
    // so the handler cannot be destroyed mid-call.
    std::lock_guard<std::mutex> lock(m_mutex);

    auto dbIter = m_handlers.find(dbName);
    if (dbIter == m_handlers.end()) {
        SWSS_LOG_DEBUG("ZmqHandlerRegistry can't find any handler for db: %s", dbName.c_str());
        return nullptr;
    }

    auto tableIter = dbIter->second.find(tableName);
    if (tableIter == dbIter->second.end()) {
        SWSS_LOG_DEBUG("ZmqHandlerRegistry can't find handler for db: %s, table: %s",
                       dbName.c_str(), tableName.c_str());
        return nullptr;
    }

    tableIter->second->handleReceivedData(kcos);
    return tableIter->second;
}

ZmqServer::ZmqServer(const std::string& endpoint)
    : ZmqServer(endpoint, "", false, false)
{
}

ZmqServer::ZmqServer(const std::string& endpoint, const std::string& vrf)
    : ZmqServer(endpoint, vrf, false, false)
{
}

ZmqServer::ZmqServer(const std::string& endpoint, const std::string& vrf, bool lazyBind)
    : ZmqServer(endpoint, vrf, lazyBind, false)
{
}

ZmqServer::ZmqServer(const std::string& endpoint, const std::string& vrf, bool lazyBind, bool oneToOneSync)
    : m_mqPollThread(nullptr),
    m_endpoint(endpoint),
    m_vrf(vrf),
    m_context(nullptr),
    m_socket(nullptr),
    m_oneToOneSync(oneToOneSync),
    m_allowZmqPoll(true),
    m_registry(std::make_shared<ZmqHandlerRegistry>())
{
    if (!lazyBind)
    {
        bind();
    }

    SWSS_LOG_DEBUG("ZmqServer ctor endpoint: %s", endpoint.c_str());
}

ZmqServer::~ZmqServer()
{
    m_allowZmqPoll = true;
    m_runThread = false;
    if (m_mqPollThread)
    {
        m_mqPollThread->join();
    }

    if (m_socket)
    {
        zmq_close(m_socket);
    }

    if (m_context)
    {
        zmq_ctx_destroy(m_context);
    }

    // m_registry's refcount drops here. If any registered handler still holds
    // a reference, the registry survives until that handler is destroyed; its
    // destructor will call removeHandler() safely against the surviving
    // registry without touching this (now-gone) ZmqServer.
}

void ZmqServer::bind()
{
    SWSS_LOG_ENTER();
    if (m_socket)
    {
        SWSS_LOG_THROW("ZmqServer has already been bound to the endpoint: %s", m_endpoint.c_str());
    }

    m_context = zmq_ctx_new();

    if(m_oneToOneSync)
    {
        m_socket = zmq_socket(m_context, ZMQ_REP);
    }
    else
    {
        m_socket = zmq_socket(m_context, ZMQ_PULL);
    }

    if (!m_oneToOneSync)
    {
        // Increase recv buffer for use all bandwidth:  http://api.zeromq.org/4-2:zmq-setsockopt
        int high_watermark = MQ_WATERMARK;
        zmq_setsockopt(m_socket, ZMQ_RCVHWM, &high_watermark, sizeof(high_watermark));

        /*
         * Enable TCP keepalive on the server socket as defense-in-depth.
         * This allows the server to detect and clean up stale client connections.
         */
        int keepalive = 1;
        zmq_setsockopt(m_socket, ZMQ_TCP_KEEPALIVE, &keepalive, sizeof(keepalive));

        int keepalive_idle = 5;
        zmq_setsockopt(m_socket, ZMQ_TCP_KEEPALIVE_IDLE, &keepalive_idle, sizeof(keepalive_idle));

        int keepalive_intvl = 1;
        zmq_setsockopt(m_socket, ZMQ_TCP_KEEPALIVE_INTVL, &keepalive_intvl, sizeof(keepalive_intvl));

        int keepalive_cnt = 5;
        zmq_setsockopt(m_socket, ZMQ_TCP_KEEPALIVE_CNT, &keepalive_cnt, sizeof(keepalive_cnt));

    }

    if (!m_vrf.empty())
    {
        zmq_setsockopt(m_socket, ZMQ_BINDTODEVICE, m_vrf.c_str(), m_vrf.length());
    }

    SWSS_LOG_NOTICE("ZmqServer socket created, actual bind to %s will occur in mqPollThread to prevent blocking", m_endpoint.c_str());

    startMqPollThread();
}

void ZmqServer::registerMessageHandler(
                                    const std::string dbName,
                                    const std::string tableName,
                                    ZmqMessageHandler* handler)
{
    m_registry->registerHandler(dbName, tableName, handler);
}

void ZmqServer::removeMessageHandler(
                                    const std::string& dbName,
                                    const std::string& tableName,
                                    const ZmqMessageHandler* handler)
{
    m_registry->removeHandler(dbName, tableName, handler);
}

ZmqMessageHandler* ZmqServer::findMessageHandler(
    const std::string /*dbName*/,
    const std::string /*tableName*/)
{
    // Retained as a no-op for source compatibility with external test mocks
    // that override this symbol at link time. Real dispatch lives in
    // ZmqHandlerRegistry::dispatch().
    return nullptr;
}

void ZmqServer::handleReceivedData(const char* buffer, const size_t size)
{
    std::string dbName;
    std::string tableName;
    std::vector<std::shared_ptr<KeyOpFieldsValuesTuple>> kcos;
    BinarySerializer::deserializeBuffer(buffer, size, dbName, tableName, kcos);

    // Dispatch through the registry (which holds the handler's lifetime lock
    // for the duration of the callback). The dispatched handler is returned
    // by dispatch() for burst-coalescing callers, but the generic path here
    // does not need it, so the return value is intentionally ignored.
    m_registry->dispatch(dbName, tableName, kcos);
}

void ZmqServer::startMqPollThread()
{
    m_buffer.resize(MQ_RESPONSE_MAX_COUNT);
    m_runThread = true;
    m_mqPollThread = std::make_shared<std::thread>(&ZmqServer::mqPollThread, this);
}

void ZmqServer::mqPollThread()
{
    SWSS_LOG_ENTER();
    SWSS_LOG_NOTICE("mqPollThread begin");

    SWSS_LOG_NOTICE("Attempting to bind to zmq endpoint: %s", m_endpoint.c_str());
    int bindStatus = zmq_bind(m_socket, m_endpoint.c_str());
    if (bindStatus != 0)
    {
        SWSS_LOG_THROW("zmq_bind failed on endpoint: %s, zmqerrno: %d",
            m_endpoint.c_str(),
            zmq_errno());
    }
    else
    {
        SWSS_LOG_NOTICE("Successfully bound to zmq endpoint: %s", m_endpoint.c_str());
    }

    // zmq_poll will use less CPU
    zmq_pollitem_t poll_item;
    poll_item.fd = 0;
    poll_item.socket = m_socket;
    poll_item.events = ZMQ_POLLIN;
    poll_item.revents = 0;
    while (m_runThread)
    {
        m_allowZmqPoll = false;

        // receive message
        auto rc = zmq_poll(&poll_item, 1, 1000);
        if (rc == 0 || !(poll_item.revents & ZMQ_POLLIN))
        {
            // timeout or other event
            SWSS_LOG_DEBUG("zmq_poll timeout or invalied event rc: %d, revents: %d", rc, poll_item.revents);
            continue;
        }

        // receive message
        if (m_oneToOneSync)
        {
            rc = zmq_recv(m_socket, m_buffer.data(), MQ_RESPONSE_MAX_COUNT, 0);
        }
        else
        {
            rc = zmq_recv(m_socket, m_buffer.data(), MQ_RESPONSE_MAX_COUNT, ZMQ_DONTWAIT);
        }

        if (rc < 0)
        {
            int zmq_err = zmq_errno();
            SWSS_LOG_DEBUG("zmq_recv failed, endpoint: %s,zmqerrno: %d", m_endpoint.c_str(), zmq_err);
            if (zmq_err == EINTR || zmq_err == EAGAIN)
            {
                continue;
            }
            else
            {
                SWSS_LOG_THROW("zmq_recv failed, endpoint: %s,zmqerrno: %d", m_endpoint.c_str(), zmq_err);
            }
        }

        if (rc >= MQ_RESPONSE_MAX_COUNT)
        {
            SWSS_LOG_THROW("zmq_recv message was truncated (over %d bytes, received %d), increase buffer size, message DROPPED",
                    MQ_RESPONSE_MAX_COUNT,
                    rc);
        }

        m_buffer.at(rc) = 0; // make sure that we end string with zero before parse
        SWSS_LOG_DEBUG("zmq received %d bytes", rc);

        // deserialize and write to redis:
        (void)handleReceivedData(m_buffer.data(), rc);
        while (m_oneToOneSync && !m_allowZmqPoll) {
          usleep(10);
        }
    }
    SWSS_LOG_NOTICE("mqPollThread end");
}

void ZmqServer::sendMsg(
    const std::string &dbName, const std::string &tableName,
    const std::vector<swss::KeyOpFieldsValuesTuple> &values)
{
  if (!m_oneToOneSync) {
    return;
  }

  int serializedlen = (int)BinarySerializer::serializeBuffer(
      m_buffer.data(), m_buffer.size(), dbName, tableName, values);

  SWSS_LOG_DEBUG("sending: %d", serializedlen);
  int zmq_err = 0;
  int retry_delay = 10;
  int rc = 0;
  for (int i = 0; i <= MQ_MAX_RETRY; ++i) {
    rc = zmq_send(m_socket, m_buffer.data(), serializedlen, 0);

    if (rc >= 0) {
      m_allowZmqPoll = true;
      SWSS_LOG_DEBUG("zmq sent %d bytes", serializedlen);
      return;
    }

    zmq_err = zmq_errno();
    // sleep (2 ^ retry time) * 10 ms
    retry_delay *= 2;
    if (zmq_err == EINTR || zmq_err == EFSM) {
      // EINTR: interrupted by signal
      // EFSM: socket state not ready
      //       For example when ZMQ socket still not receive reply message from
      //       last sended package. There was state machine inside ZMQ socket,
      //       when the socket is not in ready to send state, this error will
      //       happen.
      // for more detail, please check: http://api.zeromq.org/2-1:zmq-send
      SWSS_LOG_DEBUG("zmq send retry, endpoint: %s, error: %d",
                     m_endpoint.c_str(), zmq_err);

      retry_delay = 0;
    } else if (zmq_err == EAGAIN) {
      // EAGAIN: ZMQ is full to need try again
      SWSS_LOG_WARN("zmq is full, will retry in %d ms, endpoint: %s, error: %d",
                    retry_delay, m_endpoint.c_str(), zmq_err);
    } else if (zmq_err == ETERM) {
      auto message = "zmq connection break, endpoint: " + m_endpoint +
                     ", error: " + to_string(rc);
      SWSS_LOG_ERROR("%s", message.c_str());
      throw system_error(make_error_code(errc::connection_reset), message);
    } else {
      // for other error, send failed immediately.
      auto message = "zmq send failed, endpoint: " + m_endpoint +
                     ", error: " + to_string(rc);
      SWSS_LOG_ERROR("%s", message.c_str());
      throw system_error(make_error_code(errc::io_error), message);
    }

    usleep(retry_delay * 1000);
  }

  // failed after retry
  auto message = "zmq send failed, endpoint: " + m_endpoint +
                 ", zmqerrno: " + to_string(zmq_err) + ":" +
                 zmq_strerror(zmq_err) +
                 ", msg length:" + to_string(serializedlen);
  SWSS_LOG_ERROR("%s", message.c_str());
  throw system_error(make_error_code(errc::io_error), message);
}

}
