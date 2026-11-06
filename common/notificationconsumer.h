#ifndef __NOTIFICATIONCONSUMER__
#define __NOTIFICATIONCONSUMER__

#include <atomic>
#include <chrono>
#include <string>
#include <vector>
#include <queue>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <memory>

#include <hiredis/hiredis.h>

#include "dbconnector.h"
#include "json.h"
#include "logger.h"
#include "redisreply.h"
#include "selectable.h"
#include "selectableevent.h"
#include "table.h"

#include <mutex>

namespace swss {

static constexpr size_t DEFAULT_NC_POP_BATCH_SIZE = 2048;

// Extract the leading op string from a NotificationProducer JSON-array
// message of the form: ["op","data","f1","v1",...].  Returns an empty
// string if the message does not start with the expected
// array-of-strings prefix.  Bounded scan, no JSON parse.  See
// NotificationProducer::send + JSon::buildJson for the wire format
// this matches.
std::string peekOp(const std::string &msg);

// ---------------------------------------------------------------------------
// Abstract queue strategy.  Single-threaded; no internal locking required.
// getStats() is safe to call from any thread, other APIs are single-threaded.
// ---------------------------------------------------------------------------
class NotificationQueueBase {
public:
    virtual ~NotificationQueueBase() = default;
    virtual void               push(const std::string& msg) = 0;
    virtual const std::string& front() const                = 0;
    virtual void               pop()                        = 0;
    virtual bool               empty() const                = 0;
    virtual size_t             size()  const                = 0;
};

// ---------------------------------------------------------------------------
// Existing FIFO behavior -- preserves strict arrival order, no dedup.
// This is the default and is bit-for-bit identical to the pre-strategy code.
// ---------------------------------------------------------------------------
class FifoNotificationQueue : public NotificationQueueBase {
public:
    void push(const std::string& msg) override
    {
        m_q.push(msg);
    }

    const std::string& front() const override
    {
        return m_q.front();
    }

    void pop() override
    {
        m_q.pop();
    }

    bool empty() const override
    {
        return m_q.empty();
    }

    size_t size() const override
    {
        return m_q.size();
    }

private:
    std::queue<std::string> m_q;
};

// ---------------------------------------------------------------------------
// LRU-dedup queue.
//
// On push(msg): if msg is already present (byte-identical), the old list
// position is erased and the entry is re-inserted at the tail.  On push
// of a novel msg, the msg is appended at the tail unconditionally.
// pop() removes from the head.
//
// Drain order: "last-seen time per unique payload."  Memory is bounded by
// count(distinct payloads ever present), not by event rate.
//
// Max queue-depth: queue depth is bound to count(distinct_payloads). If the
// consumer has very high cardinality of distinct payloads, the max queue
// depth could still be high under storm.
//
// Instrumentation:
//   - m_high_watermark : monotonic max of m_dq.size() seen.
//                        A SWSS_LOG_NOTICE fires whenever a new max is
//                        reached, including a per-instance label
//                        (typically the channel name).
//
// Only opt in for consumers that have been audited as end-state-idempotent.
// ---------------------------------------------------------------------------
class LruDedupNotificationQueue : public NotificationQueueBase {
public:
    // Snapshot returned by getStats().  Cheap to copy; can be published to
    // COUNTERS_DB or logged.
    //
    // Plain aggregate; no default member initializers because some
    // gcc/-std=c++14 combinations reject brace-init of an aggregate that
    // mixes DMIs with a returned aggregate.  All callers supply every
    // member.
    struct Stats {
        uint64_t pushed;          // total push() calls
        uint64_t dedup_hits;      // pushes that collapsed onto existing entry
        size_t   high_watermark;  // max queue depth ever
        size_t   current_depth;   // depth right now
    };

    explicit LruDedupNotificationQueue(const std::string& label = "")
        : m_label(label) {}

    void push(const std::string& msg) override;

    const std::string& front() const override
    {
        return m_dq.front();
    }

    void pop() override
    {
        if (m_dq.empty()) return;   // defensive; caller should check empty()
        auto it = m_idx.find(m_dq.front());
        if (it != m_idx.end()) m_idx.erase(it);
        m_dq.pop_front();
        m_current_depth.fetch_sub(1, std::memory_order_relaxed);
    }

    bool empty() const override
    {
        return m_dq.empty();
    }

    size_t size() const override
    {
        return m_dq.size();
    }

    // Snapshot of all counters.  Lock-free; safe to call from a thread
    // other than the one driving push()/pop().
    Stats getStats() const {
        Stats s;
        s.pushed         = m_pushed.load(std::memory_order_relaxed);
        s.dedup_hits     = m_dedup_hits.load(std::memory_order_relaxed);
        s.high_watermark = m_high_watermark.load(std::memory_order_relaxed);
        s.current_depth  = m_current_depth.load(std::memory_order_relaxed);
        return s;
    }

    // Mutable from gdb for inspection; the setter form keeps the
    // NotificationConsumer-driven label-propagation path safe under
    // a future API audit (see setLabel below).
    void setLabel(const std::string& label) { m_label = label; }

private:
    void maybeLogStats();

    std::list<std::string> m_dq;
    std::unordered_map<std::string, std::list<std::string>::iterator> m_idx;
    std::string m_label;

    // Telemetry.  All atomic so getStats() is safe to call cross-thread.
    // m_pushed / m_dedup_hits are monotonic counters incremented in
    // push().  m_high_watermark is monotonic-increasing.  m_current_depth
    // tracks net queue growth (incremented on novel push, decremented
    // on pop, unchanged on dedup-hit).
    std::atomic<uint64_t> m_pushed{0};
    std::atomic<uint64_t> m_dedup_hits{0};
    std::atomic<size_t>   m_high_watermark{0};
    std::atomic<size_t>   m_current_depth{0};

    // Throttling for the periodic stats SWSS_LOG_NOTICE.  At most one log
    // per kStatsLogInterval, and only when the counters have advanced
    // since the previous log line.  No timer thread is needed -- the
    // throttle gate runs inline from push().
    std::chrono::steady_clock::time_point m_last_stats_log{};
    uint64_t m_last_logged_pushed{0};
};

// ---------------------------------------------------------------------------
// Policy tag passed to NotificationConsumer at construction time.
// Existing call sites compile unchanged (Fifo is the default).
// ---------------------------------------------------------------------------
enum class NotificationQueuePolicy {
    // Default. Strict FIFO; every push is preserved in arrival order.
    Fifo,

    // Opt-in dedup with re-ordering. Collapses duplicated by moving the
    // duplicate to the tail of the queue, so drain order becomes "last-seen
    // time per unique payload" instead of arrival order. Bounds the size of
    // the queue under storms.
    // Pick this policy if the consumer's outcome is idempotent and determined
    // by the final state per unique payload. If the consumers needs every
    // event observed in arrival order, pick Fifo.
    LruDedup
};

class NotificationConsumer : public Selectable
{
public:
    // Snapshot returned by getStats().  Cheap to copy.
    struct Stats {
        uint64_t received;           // total processReply admissions attempted
        uint64_t dropped_allowlist;  // dropped at admission by op-allowlist
    };

    // Original 4-arg constructor.  Preserved verbatim so the mangled symbol
    // matches every binary that linked against pre-strategy libswsscommon
    // (e.g. python3-swsscommon's _swsscommon.so).  Defaults to Fifo policy.
    NotificationConsumer(swss::DBConnector *db,
                         const std::string &channel,
                         int pri = 100,
                         size_t popBatchSize = DEFAULT_NC_POP_BATCH_SIZE);

    // New 5-arg constructor that takes an explicit queue policy.  Callers
    // that want LruDedup pass NotificationQueuePolicy::LruDedup here.
    // Distinct mangled symbol from the 4-arg form, so old binaries keep
    // resolving the 4-arg version unchanged.
    NotificationConsumer(swss::DBConnector *db,
                         const std::string &channel,
                         int pri,
                         size_t popBatchSize,
                         NotificationQueuePolicy policy);

    // In-process delivery (ZMQ orchagent callback -> send()).  No Redis
    // subscribe; the Select loop waits on an internal eventfd.  channel is
    // the logical notification channel name (logging/stats), typically
    // "NOTIFICATIONS".
    NotificationConsumer(const std::string &channel,
                         NotificationQueuePolicy policy,
                         int pri = 100,
                         size_t popBatchSize = DEFAULT_NC_POP_BATCH_SIZE);

    // Thread-safe: enqueue from libsairedis notification callback thread.
    // Only valid for in-process consumers (isInProcessDelivery()).
    // Never throws: on a Redis-subscribed consumer, or on any internal
    // failure, the notification is logged and dropped.
    void send(const std::string &op,
              const std::string &data,
              const std::vector<FieldValueTuple> &values);

    // Wake the Select loop when readiness transitions (queue may already hold data).
    void notifyPending();

    bool isInProcessDelivery() const { return m_in_process; }

    // Pop one or multiple data from the internal queue which fed from redis socket
    // Note:
    //    Ensure data ready before popping, either by select or peek
    void pop(std::string &op, std::string &data, std::vector<FieldValueTuple> &values);
    void pops(std::deque<KeyOpFieldsValuesTuple> &vkco);

    // Check the internal queue which fed from redis socket for data ready
    // Returns:
    //     1 - data immediately available inside internal queue, may be just fed from redis socket
    //     0 - no data both in internal queue or redis socket
    //    -1 - error during peeking redis socket
    int peek();

    ~NotificationConsumer() override;

    int getFd() override;
    uint64_t readData() override;
    bool hasData() override;
    bool hasCachedData() override;
    const size_t POP_BATCH_SIZE;

    // Set an orch-qualified label (e.g. "FdbOrch:fdb_event") for syslog
    // identification.  Without this, every consumer on the same Redis
    // channel logs as "NotificationConsumer[<channel>]" -- indistinguish-
    // able when several orchs subscribe to the same channel.  When set,
    // both the consumer's periodic stats line and the internal LruDedup
    // queue's HWM / stats lines use this label instead of the bare
    // channel name.  Empty (default) preserves legacy channel-only
    // logging.  Thread-safe with respect to send() and the admission path.
    void setStatsLabel(const std::string &label);

    // Restrict which messages are admitted into the internal queue.
    //
    // When the set is non-empty, processReply extracts the leading "op"
    // token from the JSON-array message and drops the message unless op
    // is in the set.  An empty set (default) preserves legacy behavior:
    // every received message is enqueued.
    //
    // Intended use: every orch's doTask(NotificationConsumer&) handles a
    // small fixed set of op strings and discards the rest.  Setting the
    // same set here prevents the discarded messages from ever consuming
    // queue memory.  This is the consumer-side defense against pub/sub
    // fanout on the "NOTIFICATIONS" channel, where every subscriber
    // receives a copy of every message even if it only cares about one op.
    //
    // Thread-safe with respect to send() and the admission path: it takes
    // the same lock the allowlist is read under.
    void setOpAllowList(std::unordered_set<std::string> ops);

    // Snapshot of admission counters.  Lock-free.
    Stats getStats() const {
        Stats s;
        s.received          = m_received.load(std::memory_order_relaxed);
        s.dropped_allowlist = m_dropped_allowlist.load(std::memory_order_relaxed);
        return s;
    }

    // Returns a pointer to the internal LruDedup queue if this consumer
    // was constructed with NotificationQueuePolicy::LruDedup, otherwise
    // nullptr.  Telemetry uses this to fetch the queue-side counters
    // (dedup hits, HWM) via LruDedupNotificationQueue::getStats().
    //
    // Lifetime: pointer is valid as long as the NotificationConsumer is
    // alive.  Do not delete.
    LruDedupNotificationQueue* getLruDedupQueue() const;

    const std::string& getChannel() const { return m_channel; }

private:

    NotificationConsumer(const NotificationConsumer &other);
    NotificationConsumer& operator = (const NotificationConsumer &other);

    void processReply(redisReply *reply);
    void subscribe();
    void subscribeWithRetry();
    void maybeLogStats();

    std::string buildWireMessage(const std::string &op,
                                 const std::string &data,
                                 const std::vector<FieldValueTuple> &values) const;

    // Caller must hold m_queueMutex.  Applies allowlist and pushes to m_queue.
    void admitWireMessageLocked(const std::string &msg);

    bool m_in_process{false};
    std::unique_ptr<SelectableEvent> m_selectableEvent;
    mutable std::mutex m_queueMutex;

    swss::DBConnector *m_db;
    std::unique_ptr<swss::DBConnector> m_subscribe;
    std::string m_channel;
    std::string m_stats_label;     // orch-qualified label for syslog; empty -> use m_channel
    std::unique_ptr<NotificationQueueBase> m_queue;
    std::unordered_set<std::string> m_op_allowlist;

    // Telemetry.
    std::atomic<uint64_t> m_received{0};
    std::atomic<uint64_t> m_dropped_allowlist{0};

    // Throttling for the periodic stats SWSS_LOG_NOTICE.  Same pattern as
    // LruDedupNotificationQueue::maybeLogStats.
    std::chrono::steady_clock::time_point m_last_stats_log{};
    uint64_t m_last_logged_received{0};
};

}

#endif // __NOTIFICATIONCONSUMER__
