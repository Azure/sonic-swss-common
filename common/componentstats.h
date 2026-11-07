#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <cstdint>

namespace swss {

class DBConnector;
class Table;

/**
 * ComponentStats — a lightweight, container-agnostic counter library.
 *
 * Each instance writes counters to a Redis hash of the form:
 *     <COMPONENT>_STATS:<entity>
 * where each field is a user-defined metric name (e.g. SET, DEL, GET, ERROR...).
 *
 * Design goals:
 *   - Effectively lock-free hot path on cache hit. After the first use of a
 *     given (entity, metric) pair, increment() / setValue() only takes a
 *     shared (reader) lock on the structural mutex for lookup, then mutates
 *     atomics. Concurrent readers do not serialize against each other.
 *     The first reference to a new entity or metric still takes the
 *     structural mutex in exclusive mode to insert into the maps.
 *   - Deferred DB connection: the DBConnector is created inside the writer
 *     thread rather than the constructor, so early-startup callers (singleton
 *     initialization before Redis is ready) do not crash.
 *   - Version-based dirty tracking: the writer thread only pushes entities
 *     whose counters changed since the last flush.
 *   - Stable references: std::map is used instead of unordered_map so that
 *     references returned by the structural lookup remain valid after later
 *     inserts (unordered_map can rehash and invalidate them). DO NOT change
 *     m_entities to a container that can invalidate references on insert,
 *     and do not add an erase API without revisiting the locking model.
 *   - Clean shutdown: the destructor uses condition_variable::notify_all() to
 *     wake the writer thread immediately, then the writer performs one final
 *     dirty-flush pass before exiting so no in-memory updates are lost.
 *
 * Thread safety: all public methods are safe to call from any thread.
 *
 * Mixing increment() and setValue() on the same metric from different threads
 * is supported but the gauge semantics of setValue() take precedence — see
 * the setValue() doc for the exact ordering guarantee.
 *
 * Component naming is ASCII-only and case-insensitive; "swss" and "SWSS"
 * resolve to the same singleton with a Redis prefix of "SWSS_STATS".
 */
class ComponentStats
{
public:
    /// Snapshot of counters for a single entity (used for diagnostics / tests).
    using CounterSnapshot = std::map<std::string, uint64_t>;

    /**
     * Create a ComponentStats instance.
     *
     * @param componentName  Used as the Redis key prefix, upper-cased.
     *                       For example, "swss" produces keys like
     *                       "SWSS_STATS:<entity>". ASCII only.
     * @param dbName         Redis logical DB name (default: "COUNTERS_DB").
     * @param intervalSec    Writer thread flush period in seconds. A value
     *                       of 0 is clamped to 1.
     * @return A shared instance. The FIRST call for a given componentName
     *         (case-insensitive) wins: dbName and intervalSec from later
     *         calls are ignored, and a warning is logged if they differ
     *         from the live instance's values.
     */
    static std::shared_ptr<ComponentStats> create(
        const std::string& componentName,
        const std::string& dbName = "COUNTERS_DB",
        uint32_t intervalSec = 1);

    ~ComponentStats();

    /// Add n to the named metric on the given entity. Creates the entity on
    /// first use. Safe to call concurrently from many threads.
    void increment(const std::string& entity,
                   const std::string& metric,
                   uint64_t n = 1);

    /// Overwrite the metric with an absolute value (gauge semantics).
    ///
    /// Takes the structural mutex in exclusive mode so that the value-store
    /// and the per-entity version bump are observed atomically by the
    /// writer thread, and so concurrent setValue() calls on the same metric
    /// are serialized.
    ///
    /// IMPORTANT: mixing setValue() (gauge) and increment() (counter) on
    /// the *same* metric from concurrent threads is best-effort only.
    /// An increment() landing after setValue() releases its lock will be
    /// observed by the writer as `value + n` rather than `value`, because
    /// the increment's atomic fetch_add executes outside the lock. Use
    /// either gauge-style or counter-style on a given metric, not both.
    void setValue(const std::string& entity,
                  const std::string& metric,
                  uint64_t value);

    /// Read a single metric. Returns 0 if either entity or metric are unknown.
    uint64_t get(const std::string& entity, const std::string& metric);

    /// Snapshot all metrics for an entity. Returns empty map if unknown.
    CounterSnapshot getAll(const std::string& entity);

    /// Globally enable / disable recording. While disabled, increment() and
    /// setValue() become no-ops; existing in-memory counters are preserved.
    /// The writer thread keeps running and may still flush entities that
    /// were already marked dirty before setEnabled(false) -- disabling does
    /// not abort in-flight flushes. Re-enabling resumes normal recording.
    /// Intended to be wired to a CONFIG_DB knob if runtime control is desired.
    void setEnabled(bool on);
    bool isEnabled() const;

    /// Stop the writer thread. Called automatically by the destructor.
    /// Before exiting, the writer thread runs one final flush pass so that
    /// any counters updated after the last periodic flush are persisted to
    /// Redis (provided the DB connection is still alive).
    void stop();

    // Non-copyable / non-movable (singleton-ish usage)
    ComponentStats(const ComponentStats&) = delete;
    ComponentStats& operator=(const ComponentStats&) = delete;

private:
    struct Counter
    {
        std::atomic<uint64_t> value{0};
        Counter() = default;
        Counter(const Counter&) = delete;
        Counter& operator=(const Counter&) = delete;
    };

    struct EntityStats
    {
        // Map of metric name -> counter. std::map keeps references and
        // iterators stable across inserts, which lets the hot path keep a
        // Counter& after releasing m_structMutex. Counter is non-movable
        // (std::atomic), but std::map::emplace constructs in place so we do
        // not need an extra unique_ptr indirection.
        std::map<std::string, Counter> metrics;
        // Incremented whenever any counter on this entity changes.
        std::atomic<uint64_t> version{0};

        EntityStats() = default;
        EntityStats(const EntityStats&) = delete;
        EntityStats& operator=(const EntityStats&) = delete;
    };

    // Bundle returned by lookupOrCreate(). Both references are stable for
    // the lifetime of this ComponentStats (std::map never invalidates
    // references on insert and this class exposes no erase API).
    struct CounterRef
    {
        EntityStats& entity;
        Counter&     counter;
    };

    ComponentStats(const std::string& componentName,
                   const std::string& dbName,
                   uint32_t intervalSec);

    // Combined lookup-or-create for (entity, metric).
    //
    // Fast path: takes a shared lock and returns existing references — N
    // concurrent callers do not serialize against each other.
    // Slow path: on cache miss, upgrades to an exclusive lock to insert.
    //
    // INVARIANT: no code path may erase from m_entities or from
    // EntityStats::metrics while a CounterRef might still be in use on
    // another thread.
    CounterRef lookupOrCreate(const std::string& entity,
                              const std::string& metric);

    void writerThread();

    // One snapshot + flush pass over m_entities. Used by both the periodic
    // writer loop and the final-flush pass on shutdown. Returns the number
    // of entities written.
    size_t flushDirty(std::unordered_map<std::string, uint64_t>& lastVersions);

    const std::string m_component;   // already upper-cased
    const std::string m_tableName;   // "<COMPONENT>_STATS"
    const std::string m_dbName;
    const uint32_t    m_intervalSec;

    std::atomic<bool> m_enabled{true};
    std::atomic<bool> m_running{true};

    // Reader/writer mutex guarding the *structure* of m_entities and each
    // EntityStats::metrics. Counter *values* are atomics and do not need
    // this lock.
    //
    // shared_timed_mutex (rather than shared_mutex) so the code compiles
    // under -std=c++14, which is what swss-common's build uses today;
    // shared_mutex is C++17-only. The timed-wait API is not used.
    mutable std::shared_timed_mutex m_structMutex;
    std::mutex              m_cvMutex;    // pairs with m_cv for waits
    std::condition_variable m_cv;         // wakes writer on stop()
    std::map<std::string, EntityStats> m_entities;

    std::shared_ptr<DBConnector>  m_db;    // created in writerThread
    std::unique_ptr<Table>        m_table;

    std::unique_ptr<std::thread>  m_thread;
};

} // namespace swss
