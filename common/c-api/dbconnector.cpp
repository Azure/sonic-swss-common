#include <cstring>
#include <string>
#include <utility>

#include "../dbconnector.h"
#include "dbconnector.h"
#include "util.h"

using namespace swss;
using namespace std;

SWSSResult SWSSSonicDBConfig_initialize(const char *path) {
    SWSSTry(SonicDBConfig::initialize(path));
}

SWSSResult SWSSSonicDBConfig_initializeGlobalConfig(const char *path, uint8_t ignore_nonexistent) {
    SWSSTry(SonicDBConfig::initializeGlobalConfig(path, ignore_nonexistent));
}

SWSSResult SWSSDBConnector_new_tcp(int32_t dbId, const char *hostname, uint16_t port,
                                   uint32_t timeout, SWSSDBConnector *outDb) {
    SWSSTry(*outDb = (SWSSDBConnector) new DBConnector(dbId, string(hostname), port, timeout));
}

SWSSResult SWSSDBConnector_new_unix(int32_t dbId, const char *sock_path, uint32_t timeout, SWSSDBConnector *outDb) {
    SWSSTry(*outDb = (SWSSDBConnector) new DBConnector(dbId, string(sock_path), timeout));
}

SWSSResult SWSSDBConnector_new_named(const char *dbName, uint32_t timeout_ms, uint8_t isTcpConn, SWSSDBConnector *outDb) {
    SWSSTry(*outDb = (SWSSDBConnector) new DBConnector(string(dbName), timeout_ms, isTcpConn));
}

SWSSResult SWSSDBConnector_new_keyed(const char *dbName, uint32_t timeout_ms, uint8_t isTcpConn,
                                     const char *containerName, const char *netns, SWSSDBConnector *outDb) {
    SWSSTry({
        SonicDBKey key;
        key.containerName = string(containerName);
        key.netns = string(netns);
        *outDb = (SWSSDBConnector) new DBConnector(string(dbName), timeout_ms, isTcpConn, key);
    });
}

SWSSResult SWSSDBConnector_new_named_with_auth(const char *dbName, uint32_t timeout_ms,
    uint8_t isTcpConn, uint8_t authenticate, SWSSDBConnector *outDb) {
    return SWSSDBConnector_new_keyed_with_auth(dbName, timeout_ms, isTcpConn, "", "", authenticate, outDb);
}

SWSSResult SWSSDBConnector_new_keyed_with_auth(const char *dbName, uint32_t timeout_ms,
    uint8_t isTcpConn, const char *containerName, const char *netns,
    uint8_t authenticate, SWSSDBConnector *outDb) {
    if (outDb) *outDb = nullptr;
    SWSSTry({
        if (!dbName || !outDb || isTcpConn > 1 || authenticate > 1)
            throw invalid_argument("Invalid authenticated DBConnector constructor arguments");
        SonicDBKey key;
        key.containerName = containerName ? containerName : "";
        key.netns = netns ? netns : "";
        *outDb = (SWSSDBConnector) new DBConnector(dbName, timeout_ms, isTcpConn != 0, key, authenticate != 0);
    });
}

SWSSResult SWSSDBConnector_free(SWSSDBConnector db) {
    SWSSTry(delete (DBConnector *)db);
}

SWSSResult SWSSDBConnector_del(SWSSDBConnector db, const char *key, int8_t *outStatus) {
    SWSSTry(*outStatus = ((DBConnector *)db)->del(string(key)) ? 1 : 0);
}

SWSSResult SWSSDBConnector_set(SWSSDBConnector db, const char *key, SWSSStrRef value) {
    SWSSTry(((DBConnector *)db)->set(string(key), takeStrRef(value)));
}

SWSSResult SWSSDBConnector_get(SWSSDBConnector db, const char *key, SWSSString *outValue) {
    SWSSTry({
        shared_ptr<string> s = ((DBConnector *)db)->get(string(key));
        *outValue = s ? makeString(move(*s)) : nullptr;
    });
}

SWSSResult SWSSDBConnector_exists(SWSSDBConnector db, const char *key, int8_t *outExists) {
    SWSSTry(*outExists = ((DBConnector *)db)->exists(string(key)) ? 1 : 0);
}

SWSSResult SWSSDBConnector_hdel(SWSSDBConnector db, const char *key, const char *field, int8_t *outResult) {
    SWSSTry(*outResult = ((DBConnector *)db)->hdel(string(key), string(field)) ? 1 : 0);
}

SWSSResult SWSSDBConnector_hset(SWSSDBConnector db, const char *key, const char *field, SWSSStrRef value) {
    SWSSTry(((DBConnector *)db)->hset(string(key), string(field), takeStrRef(value)));
}

SWSSResult SWSSDBConnector_hget(SWSSDBConnector db, const char *key, const char *field, SWSSString *outValue) {
    SWSSTry({
        shared_ptr<string> s = ((DBConnector *)db)->hget(string(key), string(field));
        *outValue = s ? makeString(move(*s)) : nullptr;
    });
}

SWSSResult SWSSDBConnector_hgetall(SWSSDBConnector db, const char *key, SWSSFieldValueArray *outArr) {
    SWSSTry({
        auto map = ((DBConnector *)db)->hgetall(string(key));

        // We can't move keys out of the map, we have to copy them, until C++17 map::extract so we
        // copy them here into a vector to avoid needing an overload on makeFieldValueArray
        vector<pair<string, string>> pairs;
        pairs.reserve(map.size());
        for (auto &pair : map)
            pairs.push_back(make_pair(pair.first, move(pair.second)));

        *outArr = makeFieldValueArray(std::move(pairs));
    });
}

SWSSResult SWSSDBConnector_hexists(SWSSDBConnector db, const char *key, const char *field, int8_t *outExists) {
    SWSSTry(*outExists = ((DBConnector *)db)->hexists(string(key), string(field)) ? 1 : 0);
}

SWSSResult SWSSDBConnector_flushdb(SWSSDBConnector db, int8_t *outStatus) {
    SWSSTry(*outStatus = ((DBConnector *)db)->flushdb() ? 1 : 0);
}
