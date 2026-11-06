#include <csignal>
#include <unistd.h>
#include <boost/algorithm/string.hpp>
#include <chrono>
#include <map>
#include <vector>
#include "configdb.h"
#include "pubsub.h"
#include "converter.h"
#include "logger.h"

using namespace std;
using namespace swss;

namespace {

// SIGINT support for the wait_for_init_indicator loop.
// The previous handler is forwarded to and restored when the wait ends.
volatile sig_atomic_t g_received_sigint = 0;
struct sigaction g_old_sigint_action;
bool g_sigint_handler_installed = false;

int elapsed_seconds_since(const chrono::steady_clock::time_point& start)
{
    return static_cast<int>(chrono::duration_cast<chrono::seconds>(
            chrono::steady_clock::now() - start).count());
}

void print_blocked_on_db_init(const string& db_name, int waited_sec)
{
    fprintf(stderr, "Waiting for %s to be initialized (%d seconds so far); "
                    "config initialization may have failed -- check "
                    "'journalctl -u config-setup' and syslog\n",
                    db_name.c_str(), waited_sec);
    fflush(stderr);
}

void log_blocked_on_db_init(const string& db_name, int waited_sec, int escalate_sec)
{
    const char *indicator = ConfigDBConnector_Native::INIT_INDICATOR;
    if (waited_sec >= escalate_sec)
    {
        SWSS_LOG_ERROR("Blocked for %d seconds waiting for %s in %s -- config "
                       "initialization (config-setup.service / config reload / "
                       "config load_minigraph) has most likely failed or crashed, "
                       "leaving %s empty or partially populated. While %s is unset, "
                       "this process is blocked; other %s consumers such as "
                       "swss/orchagent cannot start or reconcile; on cold boot, "
                       "nothing is programmed on the ASIC (no ports, VLANs, or "
                       "routes); after a mid-life config wipe, the ASIC keeps "
                       "forwarding on stale state that can no longer be updated. "
                       "Diagnose: 'sonic-db-cli %s GET %s', "
                       "'journalctl -u config-setup -u swss'. "
                       "Recover: fix the config source and rerun "
                       "'config reload' or 'config load_minigraph'; on success all "
                       "waiting processes will unblock immediately, no reboot needed.",
                       waited_sec, indicator, db_name.c_str(),
                       db_name.c_str(), indicator, db_name.c_str(),
                       db_name.c_str(), indicator);
    }
    else
    {
        SWSS_LOG_WARN("Still waiting for %s in %s after %d seconds; "
                      "this process is blocked until it is set",
                      indicator, db_name.c_str(), waited_sec);
    }
}

void sigint_handler(int signo)
{
    // Forward signal only when there's an actual function to forward to.
    if (g_old_sigint_action.sa_handler != SIG_IGN &&
        g_old_sigint_action.sa_handler != SIG_DFL)
    {
        g_old_sigint_action.sa_handler(signo);
    }
    g_received_sigint = 1;
}

void install_sigint_handler()
{
    g_received_sigint = 0;
    struct sigaction new_action = {};
    new_action.sa_handler = sigint_handler;
    new_action.sa_flags = 0;

    if (sigaction(SIGINT, &new_action, &g_old_sigint_action))
    {
        // Not fatal in a library: the wait keeps working, it is just not
        // interruptible by Ctrl+C.
        SWSS_LOG_ERROR("failed to setup SIGINT handler while waiting for ConfigDB init");
        return;
    }
    g_sigint_handler_installed = true;
}

void restore_sigint_handler()
{
    if (g_sigint_handler_installed)
    {
        sigaction(SIGINT, &g_old_sigint_action, nullptr);
        g_sigint_handler_installed = false;
    }
}

} // anonymous namespace

ConfigDBConnector_Native::ConfigDBConnector_Native(bool use_unix_socket_path, const char *netns)
    : SonicV2Connector_Native(use_unix_socket_path, netns)
    , m_table_name_separator("|")
    , m_key_separator("|")
{
}

void ConfigDBConnector_Native::db_connect(string db_name, bool wait_for_init, bool retry_on)
{
    m_db_name = db_name;
    m_key_separator = m_table_name_separator = get_db_separator(db_name);
    SonicV2Connector_Native::connect(m_db_name, retry_on);

    if (wait_for_init)
    {
        wait_for_init_indicator();
    }
}

void ConfigDBConnector_Native::wait_for_init_indicator()
{
    wait_for_init_indicator(WaitForInitSchedule());
}

// Block until INIT_INDICATOR is set in the db, warning periodically to
// syslog (and to the terminal when stderr is a tty) while blocked.
void ConfigDBConnector_Native::wait_for_init_indicator(const WaitForInitSchedule& schedule)
{
    auto& client = get_redis_client(m_db_name);
    auto is_initialized = [&]() {
        auto initialized = client.get(INIT_INDICATOR);
        return initialized && !initialized->empty();
    };
    if (is_initialized())
    {
        return;
    }

    // Subscribe to INIT_INDICATOR message.
    auto pubsub = make_shared<PubSub>(&client);
    string pattern = "__keyspace@" + to_string(get_dbid(m_db_name)) +  "__:" + INIT_INDICATOR;
    pubsub->psubscribe(pattern);

    // Poll for INIT_INDICATOR indefinitely, with periodic warning messages.
    const bool stderr_is_tty = isatty(fileno(stderr)) != 0;
    const auto start = chrono::steady_clock::now();
    int next_warn_sec = schedule.first_warn_sec;
    int next_tty_sec = schedule.tty_interval_sec;
    // But respect SIGINT.
    install_sigint_handler();
    while (!is_initialized())
    {
        // Throw on SIGINT.
        if (g_received_sigint)
        {
            restore_sigint_handler();
            throw InterruptedError("Interrupted while waiting for " +
                                   string(INIT_INDICATOR) + " in " + m_db_name);
        }
        int waited_sec = elapsed_seconds_since(start);
        // Print and/or log warning messages if due.
        if (stderr_is_tty && waited_sec >= next_tty_sec)
        {
            print_blocked_on_db_init(m_db_name, waited_sec);
            next_tty_sec = waited_sec + schedule.tty_interval_sec;
        }
        if (waited_sec >= next_warn_sec)
        {
            log_blocked_on_db_init(m_db_name, waited_sec, schedule.escalate_sec);
            next_warn_sec = waited_sec + schedule.warn_interval_sec;
        }
        // Poll until the next warning message is due, or interrupted by
        // signals. We throw on SIGINT in the next iteration. Unrelated signals
        // merely wake the poll early and the wait continues.
        int next_due_sec = stderr_is_tty ? min(next_warn_sec, next_tty_sec) : next_warn_sec;
        int timeout_sec = max(1, next_due_sec - waited_sec);
        pubsub->get_message(timeout_sec, /*interrupt_on_signal=*/true);
    }
    restore_sigint_handler();
    pubsub->punsubscribe(pattern);
}

void ConfigDBConnector_Native::connect(bool wait_for_init, bool retry_on)
{
    db_connect("CONFIG_DB", wait_for_init, retry_on);
}

// Write a table entry to config db.
//    Remove extra fields in the db which are not in the data.
// Args:
//     table: Table name.
//     key: Key of table entry, or a tuple of keys if it is a multi-key table.
//     data: Table row data in a form of dictionary {'column_key': 'value', ...}.
//           Pass {} as data will delete the entry.
void ConfigDBConnector_Native::set_entry(string table, string key, const map<string, string>& data)
{
    auto& client = get_redis_client(m_db_name);
    string _hash = to_upper(table) + m_table_name_separator + key;
    if (data.empty())
    {
        client.del(_hash);
    }
    else
    {
        auto original = get_entry(table, key);
        client.hmset(_hash, data.begin(), data.end());
        for (auto& it: original)
        {
            auto& k = it.first;
            bool found = data.find(k) != data.end();
            if (!found)
            {
                client.hdel(_hash, k);
            }
        }
    }
}

// Modify a table entry to config db.
// Args:
//     table: Table name.
//     key: Key of table entry, or a tuple of keys if it is a multi-key table.
//     data: Table row data in a form of dictionary {'column_key': 'value', ...}.
//           Pass {} as data will create an entry with no column if not already existed.
//           Pass None as data will delete the entry.
void ConfigDBConnector_Native::mod_entry(string table, string key, const map<string, string>& data)
{
    auto& client = get_redis_client(m_db_name);
    string _hash = to_upper(table) + m_table_name_separator + key;
    if (data.empty())
    {
        client.del(_hash);
    }
    else
    {
        client.hmset(_hash, data.begin(), data.end());
    }
}

// Read a table entry from config db.
// Args:
//     table: Table name.
//     key: Key of table entry, or a tuple of keys if it is a multi-key table.
// Returns:
//     Table row data in a form of dictionary {'column_key': 'value', ...}
//     Empty dictionary if table does not exist or entry does not exist.
map<string, string> ConfigDBConnector_Native::get_entry(string table, string key)
{
    auto& client = get_redis_client(m_db_name);
    string _hash = to_upper(table) + m_table_name_separator + key;
    return client.hgetall<map<string, string>>(_hash);
}

// Read all keys of a table from config db.
// Args:
//     table: Table name.
//     split: split the first part and return second.
//            Useful for keys with two parts <tablename>:<key>
// Returns:
//     List of keys.
vector<string> ConfigDBConnector_Native::get_keys(string table, bool split)
{
    auto& client = get_redis_client(m_db_name);
    string pattern = to_upper(table) + m_table_name_separator + "*";
    const auto& keys = client.keys(pattern);
    vector<string> data;
    for (auto& key: keys)
    {
        if (split)
        {
            size_t pos = key.find(m_table_name_separator);
            string row;
            if (pos != string::npos)
            {
                row = key.substr(pos + 1);
            }
            data.push_back(row);
        }
        else
        {
            data.push_back(key);
        }
    }
    return data;
}

// Read an entire table from config db.
// Args:
//     table: Table name.
// Returns:
//     Table data in a dictionary form of
//     { 'row_key': {'column_key': value, ...}, ...}
//     or { ('l1_key', 'l2_key', ...): {'column_key': value, ...}, ...} for a multi-key table.
//     Empty dictionary if table does not exist.
map<string, map<string, string>> ConfigDBConnector_Native::get_table(string table)
{
    auto& client = get_redis_client(m_db_name);
    string pattern = to_upper(table) + m_table_name_separator + "*";
    const auto& keys = client.keys(pattern);
    map<string, map<string, string>> data;
    for (auto& key: keys)
    {
        auto const& entry = client.hgetall<map<string, string>>(key);
        size_t pos = key.find(m_table_name_separator);
        string row;
        if (pos == string::npos)
        {
            continue;
        }
        row = key.substr(pos + 1);
        data[row] = entry;
    }
    return data;
}

// Delete an entire table from config db.
// Args:
//     table: Table name.
void ConfigDBConnector_Native::delete_table(string table)
{
    auto& client = get_redis_client(m_db_name);
    string pattern = to_upper(table) + m_table_name_separator + "*";
    const auto& keys = client.keys(pattern);
    for (auto& key: keys)
    {
        client.del(key);
    }
}

// Write multiple tables into config db.
//    Extra entries/fields in the db which are not in the data are kept.
// Args:
//     data: config data in a dictionary form
//     {
//         'TABLE_NAME': { 'row_key': {'column_key': 'value', ...}, ...},
//         'MULTI_KEY_TABLE_NAME': { ('l1_key', 'l2_key', ...) : {'column_key': 'value', ...}, ...},
//         ...
//     }
void ConfigDBConnector_Native::mod_config(const map<string, map<string, map<string, string>>>& data)
{
    for (auto const& it: data)
    {
        string table_name = it.first;
        auto const& table_data = it.second;
        if (table_data.empty())
        {
            delete_table(table_name);
            continue;
        }
        for (auto const& ie: table_data)
        {
            string key = ie.first;
            auto const& fvs = ie.second;
            mod_entry(table_name, key, fvs);
        }
    }
}

// Read all config data.
// Returns:
//     Config data in a dictionary form of
//     {
//         'TABLE_NAME': { 'row_key': {'column_key': 'value', ...}, ...},
//         'MULTI_KEY_TABLE_NAME': { ('l1_key', 'l2_key', ...) : {'column_key': 'value', ...}, ...},
//         ...
//     }
map<string, map<string, map<string, string>>> ConfigDBConnector_Native::get_config()
{
    auto& client = get_redis_client(m_db_name);
    return client.getall();
}

std::string ConfigDBConnector_Native::getKeySeparator() const
{
    return m_key_separator;
}

std::string ConfigDBConnector_Native::getTableNameSeparator() const
{
    return m_table_name_separator;
}

std::string ConfigDBConnector_Native::getDbName() const
{
    return m_db_name;
}

ConfigDBPipeConnector_Native::ConfigDBPipeConnector_Native(bool use_unix_socket_path, const char *netns)
    : ConfigDBConnector_Native(use_unix_socket_path, netns)
{
}

// Helper method to delete table entries from config db using Redis pipeline
// with batch size of REDIS_SCAN_BATCH_SIZE.
// The caller should call pipeline execute once ready
// Args:
//     client: Redis client
//     pipe: Redis DB pipe
//     pattern: key pattern
//     cursor: position to start scanning from
//
// Returns:
//     cur: poition of next item to scan
int ConfigDBPipeConnector_Native::_delete_entries(DBConnector& client, RedisTransactioner& pipe, const char *pattern, int cursor)
{
    const auto& rc = client.scan(cursor, pattern, REDIS_SCAN_BATCH_SIZE);
    auto cur = rc.first;
    auto& keys = rc.second;
    for (auto const& key: keys)
    {
        RedisCommand sdel;
        sdel.format("DEL %s", key.c_str());
        pipe.enqueue(sdel, REDIS_REPLY_INTEGER);
    }

    return cur;
}

// Helper method to delete table entries from config db using Redis pipeline.
// The caller should call pipeline execute once ready
// Args:
//     client: Redis client
//     pipe: Redis DB pipe
//     table: Table name.
void ConfigDBPipeConnector_Native::_delete_table(DBConnector& client, RedisTransactioner& pipe, string table)
{
    string pattern = to_upper(table) + m_table_name_separator + "*";
    auto cur = _delete_entries(client, pipe, pattern.c_str(), 0);
    while (cur != 0)
    {
        cur = _delete_entries(client, pipe, pattern.c_str(), cur);
    }
}

// Write a table entry to config db
//    Remove extra fields in the db which are not in the data
// Args:
//     table: Table name
//     key: Key of table entry, or a tuple of keys if it is a multi-key table
//     data: Table row data in a form of dictionary {'column_key': 'value', ...}
//           Pass {} as data will delete the entry
void ConfigDBPipeConnector_Native::set_entry(string table, string key, const map<string, string>& data)
{
    auto& client = get_redis_client(m_db_name);
    DBConnector clientPipe(client);
    RedisTransactioner pipe(&clientPipe);

    pipe.multi();
    _set_entry(pipe, table, key, data);
    pipe.exec();
}

// Write a table entry to config db
//    Remove extra fields in the db which are not in the data
// Args:
//     pipe: Redis DB pipe
//     table: Table name
//     key: Key of table entry, or a tuple of keys if it is a multi-key table
//     data: Table row data in a form of dictionary {'column_key': 'value', ...}
//           Pass {} as data will delete the entry
void ConfigDBPipeConnector_Native::_set_entry(RedisTransactioner& pipe, std::string table, std::string key, const std::map<std::string, std::string>& data)
{
    string _hash = to_upper(table) + m_table_name_separator + key;
    if (data.empty())
    {
        RedisCommand sdel;
        sdel.format("DEL %s", _hash.c_str());
        pipe.enqueue(sdel, REDIS_REPLY_INTEGER);
    }
    else
    {
        auto original = get_entry(table, key);

        RedisCommand shset;
        shset.formatHSET(_hash, data.begin(), data.end());
        pipe.enqueue(shset, REDIS_REPLY_INTEGER);

        for (auto& it: original)
        {
            auto& k = it.first;
            bool found = data.find(k) != data.end();
            if (!found)
            {
                RedisCommand shdel;
                shdel.formatHDEL(_hash, k);
                pipe.enqueue(shdel, REDIS_REPLY_INTEGER);
            }
        }
    }
}

// Modify a table entry to config db.
// Args:
//     table: Table name.
//     pipe: Redis DB pipe
//     table: Table name.
//     key: Key of table entry, or a tuple of keys if it is a multi-key table.
//     data: Table row data in a form of dictionary {'column_key': 'value', ...}.
//           Pass {} as data will create an entry with no column if not already existed.
//           Pass None as data will delete the entry.
void ConfigDBPipeConnector_Native::_mod_entry(RedisTransactioner& pipe, string table, string key, const map<string, string>& data)
{
    string _hash = to_upper(table) + m_table_name_separator + key;
    if (data.empty())
    {
        RedisCommand sdel;
        sdel.format("DEL %s", _hash.c_str());
        pipe.enqueue(sdel, REDIS_REPLY_INTEGER);
    }
    else
    {
        RedisCommand shset;
        shset.formatHSET(_hash, data.begin(), data.end());
        pipe.enqueue(shset, REDIS_REPLY_INTEGER);
    }
}
// Write multiple tables into config db.
//    Extra entries/fields in the db which are not in the data are kept.
// Args:
//     data: config data in a dictionary form
//     {
//         'TABLE_NAME': { 'row_key': {'column_key': 'value', ...}, ...},
//         'MULTI_KEY_TABLE_NAME': { ('l1_key', 'l2_key', ...) : {'column_key': 'value', ...}, ...},
//         ...
//     }
void ConfigDBPipeConnector_Native::mod_config(const map<string, map<string, map<string, string>>>& data)
{
    auto& client = get_redis_client(m_db_name);
    DBConnector clientPipe(client);
    RedisTransactioner pipe(&clientPipe);
    pipe.multi();
    for (auto const& id: data)
    {
        auto& table_name = id.first;
        auto& table_data = id.second;
        if (table_data.empty())
        {
            _delete_table(client, pipe, table_name);
            continue;
        }
        for (auto const& it: table_data)
        {
            auto& key = it.first;
            _mod_entry(pipe, table_name, key, it.second);
        }
    }
    pipe.exec();
}

// Read config data in batches of size REDIS_SCAN_BATCH_SIZE using Redis pipelines
// Args:
//     client: Redis client
//     pipe: Redis DB pipe
//     data: config dictionary
//     cursor: position to start scanning from
//
// Returns:
//     cur: position of next item to scan
int ConfigDBPipeConnector_Native::_get_config(DBConnector& client, RedisTransactioner& pipe, map<string, map<string, map<string, string>>>& data, int cursor)
{
    auto const& rc = client.scan(cursor, "*", REDIS_SCAN_BATCH_SIZE);
    auto cur = rc.first;
    auto const& keys = rc.second;
    pipe.multi();
    for (auto const& key: keys)
    {
        size_t pos = key.find(m_table_name_separator);
        if (pos == string::npos)
        {
            continue;
        }
        RedisCommand shgetall;
        shgetall.format("HGETALL %s", key.c_str());
        pipe.enqueue(shgetall, REDIS_REPLY_ARRAY);
    }
    pipe.exec();

    for (auto const& key: keys)
    {
        size_t pos = key.find(m_table_name_separator);
        string table_name = key.substr(0, pos);
        string row;
        if (pos == string::npos)
        {
            continue;
        }
        row = key.substr(pos + 1);

        auto reply = pipe.dequeueReply();
        RedisReply r(reply);

        auto& dataentry = data[table_name][row];
        for (unsigned int i = 0; i < r.getChildCount(); i += 2)
        {
            string field = r.getChild(i)->str;
            string value = r.getChild(i+1)->str;
            dataentry.emplace(field, value);
        }
    }
    return cur;
}

map<string, map<string, map<string, string>>> ConfigDBPipeConnector_Native::get_config()
{
    auto& client = get_redis_client(m_db_name);
    DBConnector clientPipe(client);
    RedisTransactioner pipe(&clientPipe);

    map<string, map<string, map<string, string>>> data;
    auto cur = _get_config(client, pipe, data, 0);
    while (cur != 0)
    {
        cur = _get_config(client, pipe, data, cur);
    }

    return data;
}
