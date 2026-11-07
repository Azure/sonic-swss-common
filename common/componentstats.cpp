#include "componentstats.h"

#include "dbconnector.h"
#include "table.h"
#include "logger.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <exception>
#include <unordered_map>
#include <utility>

namespace swss {

namespace {

std::string toUpper(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::toupper(c); });
    return s;
}

// Registry of created instances keyed by component name, so callers that
// forget to hold onto the shared_ptr still get the same singleton back.
std::mutex& registryMutex()
{
    static std::mutex m;
    return m;
}

std::map<std::string, std::weak_ptr<ComponentStats>>& registry()
{
    static std::map<std::string, std::weak_ptr<ComponentStats>> r;
    return r;
}

// Drop entries whose shared_ptr has been released. Called from create()
// under registryMutex() so long-running processes that cycle through
// distinct component names don't leak the registry map.
void pruneExpiredRegistryLocked()
{
    auto& r = registry();
    for (auto it = r.begin(); it != r.end(); )
    {
        if (it->second.expired())
        {
            it = r.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

} // namespace

std::shared_ptr<ComponentStats> ComponentStats::create(
    const std::string& componentName,
    const std::string& dbName,
    uint32_t intervalSec)
{
    const std::string key = toUpper(componentName);
    const uint32_t effectiveInterval = intervalSec == 0 ? 1 : intervalSec;

    std::lock_guard<std::mutex> lock(registryMutex());

    // Reclaim any slots whose ComponentStats has already been destroyed
    // before we (possibly) insert a new one.
    pruneExpiredRegistryLocked();

    auto& slot = registry()[key];
    if (auto existing = slot.lock())
    {
        // First call wins. Warn if a later caller passes different params
        // so the inconsistency is at least visible in syslog.
        if (existing->m_dbName != dbName ||
            existing->m_intervalSec != effectiveInterval)
        {
            SWSS_LOG_WARN("ComponentStats[%s]: ignoring later create() with "
                          "db=%s interval=%us; live instance uses db=%s "
                          "interval=%us",
                          key.c_str(),
                          dbName.c_str(), effectiveInterval,
                          existing->m_dbName.c_str(), existing->m_intervalSec);
        }
        return existing;
    }

    // std::make_shared cannot access the private constructor; use new + wrap.
    // effectiveInterval is already clamped above, so the constructor receives
    // a sane value and does not need to clamp again.
    std::shared_ptr<ComponentStats> inst(
        new ComponentStats(key, dbName, effectiveInterval));
    slot = inst;
    return inst;
}

ComponentStats::ComponentStats(const std::string& componentName,
                               const std::string& dbName,
                               uint32_t intervalSec)
    : m_component(componentName)
    , m_tableName(componentName + "_STATS")
    , m_dbName(dbName)
    , m_intervalSec(intervalSec)
{
    SWSS_LOG_ENTER();
    m_thread = std::make_unique<std::thread>(&ComponentStats::writerThread, this);
    SWSS_LOG_NOTICE("ComponentStats[%s] initialized (db=%s, interval=%us)",
                    m_component.c_str(), m_dbName.c_str(), m_intervalSec);
}

ComponentStats::~ComponentStats()
{
    stop();
}

void ComponentStats::stop()
{
    if (!m_running.exchange(false))
    {
        return; // already stopped
    }
    {
        std::lock_guard<std::mutex> lock(m_cvMutex);
        // ensure the writer observes m_running==false even if it just
        // entered the wait_for above.
    }
    m_cv.notify_all();
    if (m_thread && m_thread->joinable())
    {
        m_thread->join();
    }
    SWSS_LOG_NOTICE("ComponentStats[%s] stopped", m_component.c_str());
}

void ComponentStats::setEnabled(bool on)
{
    m_enabled.store(on, std::memory_order_relaxed);
}

bool ComponentStats::isEnabled() const
{
    return m_enabled.load(std::memory_order_relaxed);
}

ComponentStats::CounterRef
ComponentStats::lookupOrCreate(const std::string& entity,
                               const std::string& metric)
{
    // Fast path: shared lock, lookup only. N concurrent readers proceed
    // without serializing against each other, which is what makes the hot
    // path effectively lock-free after the first call for (entity, metric).
    {
        std::shared_lock<std::shared_timed_mutex> rlock(m_structMutex);
        auto eit = m_entities.find(entity);
        if (eit != m_entities.end())
        {
            auto mit = eit->second.metrics.find(metric);
            if (mit != eit->second.metrics.end())
            {
                // std::map references stay valid across later inserts,
                // so returning them after rlock release is safe.
                return CounterRef{eit->second, mit->second};
            }
        }
    }

    // Slow path: upgrade to an exclusive lock and insert. A racing thread
    // may have inserted the same key in the meantime; the second find()
    // covers that case so we never construct twice.
    std::unique_lock<std::shared_timed_mutex> wlock(m_structMutex);
    auto eit = m_entities.find(entity);
    if (eit == m_entities.end())
    {
        eit = m_entities.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(entity),
                                 std::forward_as_tuple()).first;
    }
    auto mit = eit->second.metrics.find(metric);
    if (mit == eit->second.metrics.end())
    {
        // std::map::emplace constructs Counter in place; no move required.
        mit = eit->second.metrics.emplace(std::piecewise_construct,
                                          std::forward_as_tuple(metric),
                                          std::forward_as_tuple()).first;
    }
    return CounterRef{eit->second, mit->second};
}

void ComponentStats::increment(const std::string& entity,
                               const std::string& metric,
                               uint64_t n)
{
    if (!isEnabled() || n == 0)
    {
        return;
    }
    auto ref = lookupOrCreate(entity, metric);
    ref.counter.value.fetch_add(n, std::memory_order_relaxed);
    // Release ordering pairs with the acquire in writerThread() so that the
    // counter write becomes visible before the version bump.
    ref.entity.version.fetch_add(1, std::memory_order_release);
}

void ComponentStats::setValue(const std::string& entity,
                              const std::string& metric,
                              uint64_t value)
{
    if (!isEnabled())
    {
        return;
    }
    auto ref = lookupOrCreate(entity, metric);
    // Take the structural lock in exclusive mode so that this setValue's
    // store and version-bump are observed atomically with respect to other
    // setValue() callers and to writer-thread snapshots. Note this does
    // NOT fully serialise against concurrent increment(): an increment
    // whose lookup races ahead of this lock can still land its atomic
    // fetch_add after setValue's store. Callers that need strict gauge
    // semantics must not mix increment() and setValue() on the same metric
    // (see header doc).
    std::unique_lock<std::shared_timed_mutex> wlock(m_structMutex);
    ref.counter.value.store(value, std::memory_order_relaxed);
    ref.entity.version.fetch_add(1, std::memory_order_release);
}

uint64_t ComponentStats::get(const std::string& entity, const std::string& metric)
{
    std::shared_lock<std::shared_timed_mutex> rlock(m_structMutex);
    auto eit = m_entities.find(entity);
    if (eit == m_entities.end()) return 0;
    auto mit = eit->second.metrics.find(metric);
    if (mit == eit->second.metrics.end()) return 0;
    return mit->second.value.load(std::memory_order_relaxed);
}

ComponentStats::CounterSnapshot
ComponentStats::getAll(const std::string& entity)
{
    CounterSnapshot snap;
    std::shared_lock<std::shared_timed_mutex> rlock(m_structMutex);
    auto eit = m_entities.find(entity);
    if (eit == m_entities.end()) return snap;
    for (const auto& kv : eit->second.metrics)
    {
        snap[kv.first] = kv.second.value.load(std::memory_order_relaxed);
    }
    return snap;
}

size_t ComponentStats::flushDirty(
    std::unordered_map<std::string, uint64_t>& lastVersions)
{
    if (!m_table) return 0;

    std::vector<std::string> keys;
    std::vector<std::vector<FieldValueTuple>> values;

    {
        std::shared_lock<std::shared_timed_mutex> rlock(m_structMutex);
        for (const auto& entry : m_entities)
        {
            const std::string& name = entry.first;
            const EntityStats& stats = entry.second;

            // Acquire pairs with release in increment()/setValue(),
            // guaranteeing we see all counter updates performed before
            // the version bump we're about to read.
            uint64_t curVer = stats.version.load(std::memory_order_acquire);

            auto vit = lastVersions.find(name);
            if (vit != lastVersions.end() && vit->second == curVer)
            {
                continue; // no change since last flush
            }
            // Provisionally record the version we're about to write. If
            // the write below fails we roll this entry back so the next
            // periodic pass retries it.
            lastVersions[name] = curVer;

            std::vector<FieldValueTuple> row;
            row.reserve(stats.metrics.size());
            for (const auto& mkv : stats.metrics)
            {
                row.emplace_back(
                    mkv.first,
                    std::to_string(mkv.second.value.load(std::memory_order_relaxed)));
            }
            keys.push_back(name);
            values.push_back(std::move(row));
        }
    }

    // Write outside the lock so that increment() / setValue() stay
    // responsive while Redis round-trips are in flight. Per-entity
    // try/catch: a single failed write must not abort the rest of the
    // batch — any entity skipped here would have its lastVersions entry
    // set above and would be silently "unchanged" on the next cycle.
    // Each failed entity's lastVersions slot is rolled back so the next
    // periodic pass retries that one entity.
    size_t failures = 0;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        try
        {
            m_table->set(keys[i], values[i]);
        }
        catch (const std::exception& e)
        {
            SWSS_LOG_WARN("ComponentStats[%s]: write failed for %s: %s",
                          m_component.c_str(), keys[i].c_str(), e.what());
            lastVersions.erase(keys[i]);
            ++failures;
        }
    }
    if (failures > 0)
    {
        // Signal partial failure to writerThread without losing the
        // count of how many entities were actually written.
        throw std::runtime_error("flushDirty: " + std::to_string(failures) +
                                 " of " + std::to_string(keys.size()) +
                                 " writes failed");
    }
    return keys.size();
}

void ComponentStats::writerThread()
{
    SWSS_LOG_ENTER();
    SWSS_LOG_NOTICE("ComponentStats[%s] writer thread started",
                    m_component.c_str());

    // Defer the DB connection: early-startup containers may call create()
    // before Redis is reachable. Retry on failure until Redis is up or
    // stop() is requested.
    while (m_running.load(std::memory_order_relaxed))
    {
        try
        {
            m_db = std::make_shared<DBConnector>(m_dbName, 0);
            m_table = std::make_unique<Table>(m_db.get(), m_tableName);
            break;
        }
        catch (const std::exception& e)
        {
            SWSS_LOG_WARN("ComponentStats[%s]: DB connect failed: %s. Retrying...",
                          m_component.c_str(), e.what());
            std::unique_lock<std::mutex> lock(m_cvMutex);
            m_cv.wait_for(lock, std::chrono::seconds(m_intervalSec),
                          [this]{ return !m_running.load(std::memory_order_relaxed); });
        }
    }

    std::unordered_map<std::string, uint64_t> lastVersions;
    int consecutiveWriteFailures = 0;
    static constexpr int kMaxConsecutiveWriteFailures = 3;

    while (true)
    {
        {
            std::unique_lock<std::mutex> lock(m_cvMutex);
            m_cv.wait_for(lock, std::chrono::seconds(m_intervalSec),
                          [this]{ return !m_running.load(std::memory_order_relaxed); });
            if (!m_running.load(std::memory_order_relaxed))
            {
                break;
            }
        }

        if (!m_table) continue;

        try
        {
            flushDirty(lastVersions);
            consecutiveWriteFailures = 0;
        }
        catch (const std::exception&)
        {
            ++consecutiveWriteFailures;
            SWSS_LOG_WARN("ComponentStats[%s]: flush failed (%d in a row)",
                          m_component.c_str(), consecutiveWriteFailures);
        }

        if (consecutiveWriteFailures >= kMaxConsecutiveWriteFailures)
        {
            SWSS_LOG_WARN("ComponentStats[%s]: %d consecutive write failures; "
                          "dropping Redis connection and reconnecting",
                          m_component.c_str(), consecutiveWriteFailures);
            m_table.reset();
            m_db.reset();
            consecutiveWriteFailures = 0;
            // Force any entities we already had marked clean to be re-flushed
            // once the new connection comes up, so Redis converges with the
            // current in-memory state instead of being stale from before the
            // outage.
            lastVersions.clear();

            // Re-enter the bounded connect-retry loop.
            while (m_running.load(std::memory_order_relaxed) && !m_table)
            {
                try
                {
                    m_db = std::make_shared<DBConnector>(m_dbName, 0);
                    m_table = std::make_unique<Table>(m_db.get(), m_tableName);
                    SWSS_LOG_NOTICE("ComponentStats[%s]: reconnected to Redis",
                                    m_component.c_str());
                }
                catch (const std::exception& e)
                {
                    SWSS_LOG_WARN("ComponentStats[%s]: DB reconnect failed: %s. Retrying...",
                                  m_component.c_str(), e.what());
                    std::unique_lock<std::mutex> lock(m_cvMutex);
                    m_cv.wait_for(lock, std::chrono::seconds(m_intervalSec),
                                  [this]{ return !m_running.load(std::memory_order_relaxed); });
                }
            }
        }
    }

    // Final flush on shutdown. Without this, any counter updates that
    // happened after the last periodic flush would be silently dropped
    // when the writer thread exits. Best-effort: if Redis is already
    // gone, log and move on -- stop() must not block on a dead Redis.
    if (m_table)
    {
        try
        {
            size_t n = flushDirty(lastVersions);
            if (n > 0)
            {
                SWSS_LOG_NOTICE("ComponentStats[%s] final flush wrote %zu entities",
                                m_component.c_str(), n);
            }
        }
        catch (const std::exception& e)
        {
            SWSS_LOG_WARN("ComponentStats[%s] final flush failed: %s",
                          m_component.c_str(), e.what());
        }
    }

    SWSS_LOG_NOTICE("ComponentStats[%s] writer thread stopped",
                    m_component.c_str());
}

} // namespace swss
