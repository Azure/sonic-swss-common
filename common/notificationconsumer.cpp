#include "notificationconsumer.h"

#include <iostream>
#include <deque>
#include <memory>
#include "redisapi.h"

#define NOTIFICATION_SUBSCRIBE_TIMEOUT (1000)
#define REDIS_PUBLISH_MESSAGE_INDEX (2)
#define REDIS_PUBLISH_MESSAGE_ELEMNTS (3)

namespace {

// Periodic stats SWSS_LOG_NOTICE throttle.  At most one log per
// kStatsLogInterval per consumer/queue, and only when counters have
// advanced since the previous line.  This is intentionally
// coarse-grained so a sustained storm produces a steady but small log
// trail (well under rsyslog's default imuxsock rate limit of
// 1000 msgs / 5 s).
constexpr std::chrono::seconds kStatsLogInterval{5};

// Counter-side gate that avoids the cost of steady_clock::now() for every
// message; the 5 sec wall-clock throttle still determines actual log
// frequency.
constexpr uint64_t kStatsCheckEveryN = 1024;

} // namespace

std::string swss::peekOp(const std::string &msg)
{
    if (msg.size() < 3 || msg[0] != '[' || msg[1] != '"') return {};
    auto end = msg.find('"', 2);
    if (end == std::string::npos) return {};
    return msg.substr(2, end - 2);
}

// Original 4-arg constructor. Defaults to Fifo policy; behavior identical to
// legacy.
swss::NotificationConsumer::NotificationConsumer(swss::DBConnector *db,
                                                 const std::string &channel,
                                                 int pri,
                                                 size_t popBatchSize):
    Selectable(pri),
    POP_BATCH_SIZE(popBatchSize),
    m_db(db),
    m_channel(channel),
    m_queue(std::make_unique<FifoNotificationQueue>())
{
    SWSS_LOG_ENTER();
    subscribeWithRetry();
}

// New 5-arg constructor with explicit policy.
swss::NotificationConsumer::NotificationConsumer(swss::DBConnector *db,
                                                 const std::string &channel,
                                                 int pri,
                                                 size_t popBatchSize,
                                                 swss::NotificationQueuePolicy policy):
    Selectable(pri),
    POP_BATCH_SIZE(popBatchSize),
    m_db(db),
    m_channel(channel),
    m_queue(policy == NotificationQueuePolicy::LruDedup
            ? std::unique_ptr<NotificationQueueBase>(std::make_unique<LruDedupNotificationQueue>(channel))
            : std::unique_ptr<NotificationQueueBase>(std::make_unique<FifoNotificationQueue>()))
{
    SWSS_LOG_ENTER();
    subscribeWithRetry();
}

// Both constructors share the same subscribe-and-retry loop.  Any
// future change (backoff, retry limit, error reporting) lives here
// in one place.
void swss::NotificationConsumer::subscribeWithRetry()
{
    while (true)
    {
        try
        {
            subscribe();
            break;
        }
        catch(...)
        {
            SWSS_LOG_ERROR("failed to subscribe on %s", m_channel.c_str());
        }
    }
}

void swss::NotificationConsumer::subscribe()
{
    SWSS_LOG_ENTER();

    /* Create new new context to DB */
    if (m_db->getContext()->connection_type == REDIS_CONN_TCP)
        m_subscribe = std::make_unique<DBConnector>(m_db->getDbId(),
                                      m_db->getContext()->tcp.host,
                                      m_db->getContext()->tcp.port,
                                      NOTIFICATION_SUBSCRIBE_TIMEOUT);
    else
        m_subscribe = std::make_unique<DBConnector>(m_db->getDbId(),
                                      m_db->getContext()->unix_sock.path,
                                      NOTIFICATION_SUBSCRIBE_TIMEOUT);

    std::string s = "SUBSCRIBE " + m_channel;

    RedisReply r(m_subscribe.get(), s, REDIS_REPLY_ARRAY);

    SWSS_LOG_INFO("subscribed to %s", m_channel.c_str());
}

int swss::NotificationConsumer::getFd()
{
    return m_subscribe->getContext()->fd;
}

uint64_t swss::NotificationConsumer::readData()
{
    SWSS_LOG_ENTER();

    redisReply *reply = nullptr;

    if (redisGetReply(m_subscribe->getContext(), reinterpret_cast<void**>(&reply)) != REDIS_OK)
    {
        SWSS_LOG_ERROR("failed to read redis reply on channel %s", m_channel.c_str());

        throw std::runtime_error("Unable to read redis reply");
    }
    else
    {
        RedisReply r(reply);
        processReply(reply);
    }

    reply = nullptr;
    int status;
    do
    {
        status = redisGetReplyFromReader(m_subscribe->getContext(), reinterpret_cast<void**>(&reply));
        if(reply != nullptr && status == REDIS_OK)
        {
            RedisReply r(reply);
            processReply(reply);
        }
    }
    while(reply != nullptr && status == REDIS_OK);

    if (status != REDIS_OK)
    {
        throw std::runtime_error("Unable to read redis reply");
    }
    return 0;
}

bool swss::NotificationConsumer::hasData()
{
    return m_queue->size() > 0;
}

bool swss::NotificationConsumer::hasCachedData()
{
    return m_queue->size() > 1;
}

void swss::NotificationConsumer::processReply(redisReply *reply)
{
    SWSS_LOG_ENTER();

    if (reply->type != REDIS_REPLY_ARRAY)
    {
        SWSS_LOG_ERROR("expected ARRAY redis reply on channel %s, got: %d", m_channel.c_str(), reply->type);

        throw std::runtime_error("getRedisReply operation failed");
    }

    if (reply->elements != REDIS_PUBLISH_MESSAGE_ELEMNTS)
    {
        SWSS_LOG_ERROR("expected %d elements in redis reply on channel %s, got: %zu",
                       REDIS_PUBLISH_MESSAGE_ELEMNTS,
                       m_channel.c_str(),
                       reply->elements);

        throw std::runtime_error("getRedisReply operation failed");
    }

    std::string msg = std::string(reply->element[REDIS_PUBLISH_MESSAGE_INDEX]->str);

    SWSS_LOG_DEBUG("got message: %s", msg.c_str());

    m_received.fetch_add(1, std::memory_order_relaxed);

    if (!m_op_allowlist.empty())
    {
        auto op = peekOp(msg);
        // Drop messages whose op is not in the allowlist before they
        // consume queue memory.  This is the cross-fanout defense for
        // shared channels like "NOTIFICATIONS": each subscriber
        // declares exactly which ops its doTask cares about and
        // silently drops the rest at admission instead of after pop.
        if (op.empty() || m_op_allowlist.find(op) == m_op_allowlist.end())
        {
            m_dropped_allowlist.fetch_add(1, std::memory_order_relaxed);
            maybeLogStats();
            return;
        }
    }

    m_queue->push(msg);
    maybeLogStats();
}

void swss::NotificationConsumer::setStatsLabel(const std::string &label)
{
    m_stats_label = label;
    // If the underlying queue is LruDedup, propagate the label so the
    // queue-side stats / HWM lines use the same identifier.
    if (auto *lru = getLruDedupQueue())
    {
        lru->setLabel(label);
    }
    SWSS_LOG_NOTICE("NotificationConsumer[%s]: stats label set (channel=%s)",
                    label.c_str(), m_channel.c_str());
}

void swss::NotificationConsumer::setOpAllowList(std::unordered_set<std::string> ops)
{
    m_op_allowlist = std::move(ops);
    SWSS_LOG_NOTICE("NotificationConsumer[%s]: op allowlist set to %zu entries",
                    (m_stats_label.empty() ? m_channel : m_stats_label).c_str(),
                    m_op_allowlist.size());
}

swss::LruDedupNotificationQueue* swss::NotificationConsumer::getLruDedupQueue() const
{
    return dynamic_cast<LruDedupNotificationQueue*>(m_queue.get());
}

void swss::NotificationConsumer::maybeLogStats()
{
    uint64_t r = m_received.load(std::memory_order_relaxed);
    if ((r & (kStatsCheckEveryN - 1)) != 0) return;

    auto now = std::chrono::steady_clock::now();
    if (now - m_last_stats_log < kStatsLogInterval) return;

    if (r == m_last_logged_received) return;     // idle, nothing new since last log

    uint64_t d = m_dropped_allowlist.load(std::memory_order_relaxed);
    SWSS_LOG_NOTICE("NotificationConsumer[%s]: stats received=%llu "
                    "dropped_allowlist=%llu admit_ratio=%.2f%% allowlist_entries=%zu",
                    (m_stats_label.empty() ? m_channel : m_stats_label).c_str(),
                    static_cast<unsigned long long>(r),
                    static_cast<unsigned long long>(d),
                    r ? 100.0 * static_cast<double>(r - d) / static_cast<double>(r) : 0.0,
                    m_op_allowlist.size());

    m_last_logged_received = r;
    m_last_stats_log = now;
}

void swss::NotificationConsumer::pop(std::string &op, std::string &data, std::vector<FieldValueTuple> &values)
{
    SWSS_LOG_ENTER();

    if (m_queue->empty())
    {
        SWSS_LOG_ERROR("notification queue is empty, can't pop");
        throw std::runtime_error("notification queue is empty, can't pop");
    }

    std::string msg = m_queue->front();
    m_queue->pop();

    values.clear();
    JSon::readJson(msg, values);

    FieldValueTuple fvt = values.at(0);

    op = fvField(fvt);
    data = fvValue(fvt);

    values.erase(values.begin());
}

void swss::NotificationConsumer::pops(std::deque<KeyOpFieldsValuesTuple> &vkco)
{
    SWSS_LOG_ENTER();

    vkco.clear();
    while(!m_queue->empty())
    {
        while(!m_queue->empty())
        {
            std::string op;
            std::string data;
            std::vector<FieldValueTuple> values;

            pop(op, data, values);
            vkco.emplace_back(data, op, values);
        }

        // Too many popped, let's return to prevent DoS attach
        if (vkco.size() >= POP_BATCH_SIZE)
            return;

        // Peek for more data in redis socket
        int rc = swss::peekRedisContext(m_subscribe->getContext());
        if (rc <= 0)
            break;

        readData();
    }
}

int swss::NotificationConsumer::peek()
{
    SWSS_LOG_ENTER();
    if (m_queue->empty())
    {
        // Peek for more data in redis socket
        int rc = swss::peekRedisContext(m_subscribe->getContext());
        if (rc <= 0)
            return rc;

        // Feed into internal queue
        readData();
    }

    return m_queue->empty() ? 0 : 1;
}

void swss::LruDedupNotificationQueue::push(const std::string& msg)
{
    bool was_dedup_hit = false;
    auto it = m_idx.find(msg);
    if (it != m_idx.end()) {
        m_dq.erase(it->second);
        m_idx.erase(it);
        was_dedup_hit = true;
    }
    m_dq.push_back(msg);
    m_idx[msg] = std::prev(m_dq.end());

    // Counters.  Relaxed atomics throughout: every member that
    // getStats() reads may be observed from a different thread
    // (orchagent's telemetry publisher).  push()/pop() are still
    // single-threaded; the atomics are only for cross-thread reads.
    m_pushed.fetch_add(1, std::memory_order_relaxed);
    if (was_dedup_hit) {
        m_dedup_hits.fetch_add(1, std::memory_order_relaxed);
    } else {
        // Net queue growth happens only on novel payloads.  Dedup
        // hits keep depth constant.
        m_current_depth.fetch_add(1, std::memory_order_relaxed);
    }

    // Track the high watermark.  Don't emit a per-update syslog --
    // under a workload with many distinct payloads, every novel push
    // raises HWM and would flood syslog.  HWM is visible via the
    // throttled stats line and getStats() / COUNTERS_DB.
    size_t cur = m_current_depth.load(std::memory_order_relaxed);
    size_t prev_hwm = m_high_watermark.load(std::memory_order_relaxed);
    if (cur > prev_hwm) {
        m_high_watermark.store(cur, std::memory_order_relaxed);
    }

    maybeLogStats();
}

// ---------------------------------------------------------------------------
// LruDedupNotificationQueue::maybeLogStats -- defined out-of-line so the
// steady_clock dependency stays in the .cpp.
// ---------------------------------------------------------------------------
void swss::LruDedupNotificationQueue::maybeLogStats()
{
    uint64_t p = m_pushed.load(std::memory_order_relaxed);
    if ((p & (kStatsCheckEveryN - 1)) != 0) return;

    auto now = std::chrono::steady_clock::now();
    if (now - m_last_stats_log < kStatsLogInterval) return;

    if (p == m_last_logged_pushed) return;       // idle, no progress

    uint64_t h = m_dedup_hits.load(std::memory_order_relaxed);
    size_t   d = m_current_depth.load(std::memory_order_relaxed);
    size_t   hwm = m_high_watermark.load(std::memory_order_relaxed);
    SWSS_LOG_NOTICE(
        "LruDedupNotificationQueue[%s]: stats pushed=%llu dedup_hits=%llu "
        "dedup_ratio=%.2f%% depth=%zu hwm=%zu",
        m_label.c_str(),
        static_cast<unsigned long long>(p),
        static_cast<unsigned long long>(h),
        p ? 100.0 * static_cast<double>(h) / static_cast<double>(p) : 0.0,
        d, hwm);

    m_last_logged_pushed = p;
    m_last_stats_log = now;
}
