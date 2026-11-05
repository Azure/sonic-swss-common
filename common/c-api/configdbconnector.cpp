#include <cstring>
#include <string>
#include <utility>
#include <map>
#include <vector>

#include "../configdb.h"
#include "configdbconnector.h"
#include "util.h"

using namespace swss;
using namespace std;

SWSSResult SWSSConfigDBConnector_new(uint8_t use_unix_socket_path, const char *netns, SWSSConfigDBConnector *outConfigDb) {
    SWSSTry({
        string netns_str = netns ? string(netns) : "";
        *outConfigDb = (SWSSConfigDBConnector) new ConfigDBConnector_Native(use_unix_socket_path != 0, netns_str.c_str());
    });
}

SWSSResult SWSSConfigDBConnector_new_with_auth(uint8_t use_unix_socket_path, const char *netns,
    uint8_t authenticate, SWSSConfigDBConnector *outConnector) {
    if (outConnector) *outConnector = nullptr;
    SWSSTry({
        if (!outConnector || use_unix_socket_path > 1 || authenticate > 1)
            throw invalid_argument("Invalid authenticated connector constructor arguments");
        *outConnector = (SWSSConfigDBConnector) new ConfigDBConnector_Native(use_unix_socket_path != 0,
            netns ? netns : "", authenticate != 0);
    });
}

SWSSResult SWSSConfigDBConnector_free(SWSSConfigDBConnector configDb) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdelete-non-virtual-dtor"
    SWSSTry(delete (ConfigDBConnector_Native *)configDb);
#pragma GCC diagnostic pop
}

SWSSResult SWSSConfigDBConnector_connect(SWSSConfigDBConnector configDb, uint8_t wait_for_init, uint8_t retry_on) {
    SWSSTry(((ConfigDBConnector_Native *)configDb)->connect(wait_for_init != 0, retry_on != 0));
}

SWSSResult SWSSConfigDBConnector_get_entry(SWSSConfigDBConnector configDb, const char *table, const char *key, SWSSFieldValueArray *outEntry) {
    SWSSTry({
        auto entry_map = ((ConfigDBConnector_Native *)configDb)->get_entry(string(table), string(key));
        // Convert map<string, string> to vector<pair<string, string>> for makeFieldValueArray
        vector<pair<string, string>> pairs;
        pairs.reserve(entry_map.size());
        for (auto &pair : entry_map) {
            pairs.push_back(make_pair(pair.first, move(pair.second)));
        }

        *outEntry = makeFieldValueArray(std::move(pairs));
    });
}

SWSSResult SWSSConfigDBConnector_get_keys(SWSSConfigDBConnector configDb, const char *table, uint8_t split, SWSSStringArray *outKeys) {
    SWSSTry({
        auto keys = ((ConfigDBConnector_Native *)configDb)->get_keys(string(table), split != 0);
        *outKeys = makeStringArray(std::move(keys));
    });
}

SWSSResult SWSSConfigDBConnector_get_table(SWSSConfigDBConnector configDb, const char *table, SWSSKeyOpFieldValuesArray *outTable) {
    SWSSTry({
        auto table_map = ((ConfigDBConnector_Native *)configDb)->get_table(string(table));

        // Convert map<string, map<string, string>> to vector<SWSSKeyOpFieldValues>
        vector<SWSSKeyOpFieldValues> table_entries;
        table_entries.reserve(table_map.size());

        for (auto &entry : table_map) {
            SWSSKeyOpFieldValues kfv_entry;
            kfv_entry.key = strdup(entry.first.c_str());
            kfv_entry.operation = SWSSKeyOperation_SET; // ConfigDB entries are always SET operations

            // Convert inner map to field-value array
            vector<pair<string, string>> pairs;
            pairs.reserve(entry.second.size());
            for (auto &field_pair : entry.second) {
                pairs.push_back(make_pair(field_pair.first, move(field_pair.second)));
            }
            kfv_entry.fieldValues = makeFieldValueArray(std::move(pairs));

            table_entries.push_back(kfv_entry);
        }

        // Convert to SWSSKeyOpFieldValuesArray
        SWSSKeyOpFieldValues *data = new SWSSKeyOpFieldValues[table_entries.size()];
        for (size_t i = 0; i < table_entries.size(); i++) {
            data[i] = table_entries[i];
        }

        SWSSKeyOpFieldValuesArray out;
        out.len = (uint64_t)table_entries.size();
        out.data = data;
        *outTable = out;
    });
}

SWSSResult SWSSConfigDBConnector_set_entry(SWSSConfigDBConnector configDb, const char *table, const char *key, const SWSSFieldValueArray *data) {
    SWSSTry({
        // Convert SWSSFieldValueArray to map<string, string>
        map<string, string> data_map;
        auto field_values = takeFieldValueArray(*data);
        for (auto &fv : field_values) {
            data_map[fv.first] = fv.second;
        }

        ((ConfigDBConnector_Native *)configDb)->set_entry(string(table), string(key), data_map);
    });
}

SWSSResult SWSSConfigDBConnector_mod_entry(SWSSConfigDBConnector configDb, const char *table, const char *key, const SWSSFieldValueArray *data) {
    SWSSTry({
        // Convert SWSSFieldValueArray to map<string, string>
        map<string, string> data_map;
        auto field_values = takeFieldValueArray(*data);
        for (auto &fv : field_values) {
            data_map[fv.first] = fv.second;
        }

        ((ConfigDBConnector_Native *)configDb)->mod_entry(string(table), string(key), data_map);
    });
}

SWSSResult SWSSConfigDBConnector_delete_table(SWSSConfigDBConnector configDb, const char *table) {
    SWSSTry(((ConfigDBConnector_Native *)configDb)->delete_table(string(table)));
}

SWSSResult SWSSConfigDBConnector_get_redis_client(SWSSConfigDBConnector configDb, const char *db_name, SWSSDBConnector *outDbConnector) {
    SWSSTry({
        // Get the Redis client from the ConfigDBConnector's parent SonicV2Connector_Native
        DBConnector& redis_client = ((ConfigDBConnector_Native *)configDb)->get_redis_client(string(db_name));

        // Return a pointer to the existing DBConnector
        // Note: This returns a reference to the existing connector, not a new one
        *outDbConnector = (SWSSDBConnector)&redis_client;
    });
}

SWSSResult SWSSConfigDBConnector_get_config(SWSSConfigDBConnector configDb, SWSSConfigMap *outConfig) {
    SWSSTry({
        // Get the entire configuration as a nested map structure
        auto config_map = ((ConfigDBConnector_Native *)configDb)->get_config();

        // Convert map<string, map<string, map<string, string>>> to SWSSConfigMap
        vector<SWSSConfigTable> config_tables;
        config_tables.reserve(config_map.size());

        for (auto &table_entry : config_map) {
            SWSSConfigTable config_table;
            config_table.table_name = strdup(table_entry.first.c_str());

            // Convert map<string, map<string, string>> to SWSSKeyOpFieldValuesArray
            vector<SWSSKeyOpFieldValues> table_entries;
            table_entries.reserve(table_entry.second.size());

            for (auto &key_entry : table_entry.second) {
                SWSSKeyOpFieldValues kfv_entry;
                kfv_entry.key = strdup(key_entry.first.c_str());
                kfv_entry.operation = SWSSKeyOperation_SET; // Config entries are always SET operations

                // Convert map<string, string> to SWSSFieldValueArray
                vector<pair<string, string>> pairs;
                pairs.reserve(key_entry.second.size());
                for (auto &field_pair : key_entry.second) {
                    pairs.push_back(make_pair(field_pair.first, move(field_pair.second)));
                }
                kfv_entry.fieldValues = makeFieldValueArray(std::move(pairs));

                table_entries.push_back(kfv_entry);
            }

            // Convert vector to array
            SWSSKeyOpFieldValues *data = new SWSSKeyOpFieldValues[table_entries.size()];
            for (size_t i = 0; i < table_entries.size(); i++) {
                data[i] = table_entries[i];
            }

            config_table.entries.len = (uint64_t)table_entries.size();
            config_table.entries.data = data;

            config_tables.push_back(config_table);
        }

        // Convert vector to final array
        SWSSConfigTable *config_data = new SWSSConfigTable[config_tables.size()];
        for (size_t i = 0; i < config_tables.size(); i++) {
            config_data[i] = config_tables[i];
        }

        outConfig->len = (uint64_t)config_tables.size();
        outConfig->data = config_data;
    });
}
