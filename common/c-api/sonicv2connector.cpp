#include <cstring>
#include <string>
#include <utility>
#include <map>
#include <vector>

#include "../sonicv2connector.h"
#include "sonicv2connector.h"
#include "util.h"

using namespace swss;
using namespace std;

SWSSResult SWSSSonicV2Connector_new(uint8_t use_unix_socket_path, const char *netns, SWSSSonicV2Connector *outConnector) {
    SWSSTry({
        string netns_str = netns ? string(netns) : "";
        *outConnector = (SWSSSonicV2Connector) new SonicV2Connector_Native(use_unix_socket_path != 0, netns_str.c_str());
    });
}

SWSSResult SWSSSonicV2Connector_new_with_auth(uint8_t use_unix_socket_path, const char *netns,
    uint8_t authenticate, SWSSSonicV2Connector *outConnector) {
    if (outConnector) *outConnector = nullptr;
    SWSSTry({
        if (!outConnector || use_unix_socket_path > 1 || authenticate > 1)
            throw invalid_argument("Invalid authenticated connector constructor arguments");
        *outConnector = (SWSSSonicV2Connector) new SonicV2Connector_Native(use_unix_socket_path != 0,
            netns ? netns : "", authenticate != 0);
    });
}

SWSSResult SWSSSonicV2Connector_free(SWSSSonicV2Connector connector) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdelete-non-virtual-dtor"
    SWSSTry(delete (SonicV2Connector_Native *)connector);
#pragma GCC diagnostic pop
}

SWSSResult SWSSSonicV2Connector_getNamespace(SWSSSonicV2Connector connector, SWSSString *outNamespace) {
    SWSSTry({
        string ns = ((SonicV2Connector_Native *)connector)->getNamespace();
        *outNamespace = makeString(std::move(ns));
    });
}

SWSSResult SWSSSonicV2Connector_connect(SWSSSonicV2Connector connector, const char *db_name, uint8_t retry_on) {
    SWSSTry(((SonicV2Connector_Native *)connector)->connect(string(db_name), retry_on != 0));
}

SWSSResult SWSSSonicV2Connector_close_db(SWSSSonicV2Connector connector, const char *db_name) {
    SWSSTry(((SonicV2Connector_Native *)connector)->close(string(db_name)));
}

SWSSResult SWSSSonicV2Connector_close_all(SWSSSonicV2Connector connector) {
    SWSSTry(((SonicV2Connector_Native *)connector)->close());
}

SWSSResult SWSSSonicV2Connector_get_db_list(SWSSSonicV2Connector connector, SWSSStringArray *outDbList) {
    SWSSTry({
        auto db_list = ((SonicV2Connector_Native *)connector)->get_db_list();
        *outDbList = makeStringArray(std::move(db_list));
    });
}

SWSSResult SWSSSonicV2Connector_get_dbid(SWSSSonicV2Connector connector, const char *db_name, int *outDbId) {
    SWSSTry({
        *outDbId = ((SonicV2Connector_Native *)connector)->get_dbid(string(db_name));
    });
}

SWSSResult SWSSSonicV2Connector_get_db_separator(SWSSSonicV2Connector connector, const char *db_name, SWSSString *outSeparator) {
    SWSSTry({
        string separator = ((SonicV2Connector_Native *)connector)->get_db_separator(string(db_name));
        *outSeparator = makeString(std::move(separator));
    });
}

SWSSResult SWSSSonicV2Connector_get_redis_client(SWSSSonicV2Connector connector, const char *db_name, SWSSDBConnector *outDbConnector) {
    SWSSTry({
        DBConnector& redis_client = ((SonicV2Connector_Native *)connector)->get_redis_client(string(db_name));
        *outDbConnector = (SWSSDBConnector)&redis_client;
    });
}

SWSSResult SWSSSonicV2Connector_publish(SWSSSonicV2Connector connector, const char *db_name, const char *channel, const char *message, int64_t *outResult) {
    SWSSTry({
        *outResult = ((SonicV2Connector_Native *)connector)->publish(string(db_name), string(channel), string(message));
    });
}

SWSSResult SWSSSonicV2Connector_exists(SWSSSonicV2Connector connector, const char *db_name, const char *key, uint8_t *outExists) {
    SWSSTry({
        *outExists = ((SonicV2Connector_Native *)connector)->exists(string(db_name), string(key)) ? 1 : 0;
    });
}

SWSSResult SWSSSonicV2Connector_keys(SWSSSonicV2Connector connector, const char *db_name, const char *pattern, uint8_t blocking, SWSSStringArray *outKeys) {
    SWSSTry({
        const char* pattern_str = pattern ? pattern : "*";
        auto keys = ((SonicV2Connector_Native *)connector)->keys(string(db_name), pattern_str, blocking != 0);
        *outKeys = makeStringArray(std::move(keys));
    });
}

SWSSResult SWSSSonicV2Connector_scan(SWSSSonicV2Connector connector, const char *db_name, int cursor, const char *match, uint32_t count, int *outCursor, SWSSStringArray *outKeys) {
    SWSSTry({
        const char* match_str = match ? match : "";
        auto result = ((SonicV2Connector_Native *)connector)->scan(string(db_name), cursor, match_str, count);
        *outCursor = result.first;
        *outKeys = makeStringArray(std::move(result.second));
    });
}

SWSSResult SWSSSonicV2Connector_get(SWSSSonicV2Connector connector, const char *db_name, const char *hash, const char *key, uint8_t blocking, SWSSString *outValue) {
    SWSSTry({
        auto value = ((SonicV2Connector_Native *)connector)->get(string(db_name), string(hash), string(key), blocking != 0);
        if (value) {
            *outValue = makeString(std::move(*value));
        } else {
            *outValue = nullptr;
        }
    });
}

SWSSResult SWSSSonicV2Connector_hexists(SWSSSonicV2Connector connector, const char *db_name, const char *hash, const char *key, uint8_t *outExists) {
    SWSSTry({
        *outExists = ((SonicV2Connector_Native *)connector)->hexists(string(db_name), string(hash), string(key)) ? 1 : 0;
    });
}

SWSSResult SWSSSonicV2Connector_get_all(SWSSSonicV2Connector connector, const char *db_name, const char *hash, uint8_t blocking, SWSSFieldValueArray *outFieldValues) {
    SWSSTry({
        auto field_values_map = ((SonicV2Connector_Native *)connector)->get_all(string(db_name), string(hash), blocking != 0);

        // Convert map<string, string> to vector<pair<string, string>> for makeFieldValueArray
        vector<pair<string, string>> pairs;
        pairs.reserve(field_values_map.size());
        for (auto &pair : field_values_map) {
            pairs.push_back(make_pair(pair.first, move(pair.second)));
        }

        *outFieldValues = makeFieldValueArray(std::move(pairs));
    });
}

SWSSResult SWSSSonicV2Connector_hmset(SWSSSonicV2Connector connector, const char *db_name, const char *key, const SWSSFieldValueArray *values) {
    SWSSTry({
        // Convert SWSSFieldValueArray to map<string, string>
        map<string, string> values_map;
        auto field_values = takeFieldValueArray(*values);
        for (auto &fv : field_values) {
            values_map[fv.first] = fv.second;
        }

        ((SonicV2Connector_Native *)connector)->hmset(string(db_name), string(key), values_map);
    });
}

SWSSResult SWSSSonicV2Connector_set(SWSSSonicV2Connector connector, const char *db_name, const char *hash, const char *key, const char *val, uint8_t blocking, int64_t *outResult) {
    SWSSTry({
        *outResult = ((SonicV2Connector_Native *)connector)->set(string(db_name), string(hash), string(key), string(val), blocking != 0);
    });
}

SWSSResult SWSSSonicV2Connector_del(SWSSSonicV2Connector connector, const char *db_name, const char *key, uint8_t blocking, int64_t *outResult) {
    SWSSTry({
        *outResult = ((SonicV2Connector_Native *)connector)->del(string(db_name), string(key), blocking != 0);
    });
}

SWSSResult SWSSSonicV2Connector_delete_all_by_pattern(SWSSSonicV2Connector connector, const char *db_name, const char *pattern) {
    SWSSTry(((SonicV2Connector_Native *)connector)->delete_all_by_pattern(string(db_name), string(pattern)));
}