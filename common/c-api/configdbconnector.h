#ifndef SWSS_COMMON_C_API_CONFIGDBCONNECTOR_H
#define SWSS_COMMON_C_API_CONFIGDBCONNECTOR_H

#include "result.h"
#include "util.h"
#include "dbconnector.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef struct SWSSConfigDBConnectorOpaque *SWSSConfigDBConnector;

// Create a new ConfigDBConnector
// Pass 0 to use TCP connection, 1 to use Unix socket
SWSSResult SWSSConfigDBConnector_new(uint8_t use_unix_socket_path, const char *netns, SWSSConfigDBConnector *outConfigDb);

// Free the ConfigDBConnector
// Explicit authentication; flags must be 0 or 1. Output is null on failure.
SWSSResult SWSSConfigDBConnector_new_with_auth(uint8_t use_unix_socket_path, const char *netns,
    uint8_t authenticate, SWSSConfigDBConnector *outConnector);

SWSSResult SWSSConfigDBConnector_free(SWSSConfigDBConnector configDb);

// Connect to ConfigDB
// wait_for_init: wait for CONFIG_DB_INITIALIZED flag if true
// retry_on: retry connection on failure if true
SWSSResult SWSSConfigDBConnector_connect(SWSSConfigDBConnector configDb, uint8_t wait_for_init, uint8_t retry_on);

// Get a single entry from a table
// Result array must be freed using SWSSFieldValueArray_free()
SWSSResult SWSSConfigDBConnector_get_entry(SWSSConfigDBConnector configDb, const char *table, const char *key, SWSSFieldValueArray *outEntry);

// Get all keys from a table
// Result array and all of its elements must be freed using appropriate free functions
SWSSResult SWSSConfigDBConnector_get_keys(SWSSConfigDBConnector configDb, const char *table, uint8_t split, SWSSStringArray *outKeys);

// Get entire table as key-value pairs
// Result is a map-like structure where each entry contains a key and its field-value pairs
// Result array and all of its elements must be freed using appropriate free functions
SWSSResult SWSSConfigDBConnector_get_table(SWSSConfigDBConnector configDb, const char *table, SWSSKeyOpFieldValuesArray *outTable);

// Set an entry in a table
SWSSResult SWSSConfigDBConnector_set_entry(SWSSConfigDBConnector configDb, const char *table, const char *key, const SWSSFieldValueArray *data);

// Modify an entry in a table (update existing fields, add new ones)
SWSSResult SWSSConfigDBConnector_mod_entry(SWSSConfigDBConnector configDb, const char *table, const char *key, const SWSSFieldValueArray *data);

// Delete an entire table
SWSSResult SWSSConfigDBConnector_delete_table(SWSSConfigDBConnector configDb, const char *table);

// Get the Redis client for the specified database
// Returns a SWSSDBConnector that can be used to perform direct Redis operations
SWSSResult SWSSConfigDBConnector_get_redis_client(SWSSConfigDBConnector configDb, const char *db_name, SWSSDBConnector *outDbConnector);

// Get the entire configuration as a nested structure
// Returns a nested table structure containing all configuration data
// The result represents a map of table_name -> (key -> (field -> value))
SWSSResult SWSSConfigDBConnector_get_config(SWSSConfigDBConnector configDb, SWSSConfigMap *outConfig);

#ifdef __cplusplus
}
#endif

#endif