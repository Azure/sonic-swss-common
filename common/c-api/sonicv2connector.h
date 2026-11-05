#ifndef SWSS_COMMON_C_API_SONICV2CONNECTOR_H
#define SWSS_COMMON_C_API_SONICV2CONNECTOR_H

#include "result.h"
#include "util.h"
#include "dbconnector.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef struct SWSSSonicV2ConnectorOpaque *SWSSSonicV2Connector;

// Create a new SonicV2Connector
// Pass 0 to use TCP connection, 1 to use Unix socket
SWSSResult SWSSSonicV2Connector_new(uint8_t use_unix_socket_path, const char *netns, SWSSSonicV2Connector *outConnector);

// Free the SonicV2Connector
// Explicit authentication; flags must be 0 or 1. Output is null on failure.
SWSSResult SWSSSonicV2Connector_new_with_auth(uint8_t use_unix_socket_path, const char *netns,
    uint8_t authenticate, SWSSSonicV2Connector *outConnector);

SWSSResult SWSSSonicV2Connector_free(SWSSSonicV2Connector connector);

// Get namespace
SWSSResult SWSSSonicV2Connector_getNamespace(SWSSSonicV2Connector connector, SWSSString *outNamespace);

// Connect to a specific database
SWSSResult SWSSSonicV2Connector_connect(SWSSSonicV2Connector connector, const char *db_name, uint8_t retry_on);

// Close connection to a specific database
SWSSResult SWSSSonicV2Connector_close_db(SWSSSonicV2Connector connector, const char *db_name);

// Close all connections
SWSSResult SWSSSonicV2Connector_close_all(SWSSSonicV2Connector connector);

// Get list of available databases
// Result array must be freed using SWSSStringArray_free()
SWSSResult SWSSSonicV2Connector_get_db_list(SWSSSonicV2Connector connector, SWSSStringArray *outDbList);

// Get database ID for a given database name
SWSSResult SWSSSonicV2Connector_get_dbid(SWSSSonicV2Connector connector, const char *db_name, int *outDbId);

// Get database separator for a given database name
SWSSResult SWSSSonicV2Connector_get_db_separator(SWSSSonicV2Connector connector, const char *db_name, SWSSString *outSeparator);

// Get Redis client for a specific database
SWSSResult SWSSSonicV2Connector_get_redis_client(SWSSSonicV2Connector connector, const char *db_name, SWSSDBConnector *outDbConnector);

// Publish a message to a channel
SWSSResult SWSSSonicV2Connector_publish(SWSSSonicV2Connector connector, const char *db_name, const char *channel, const char *message, int64_t *outResult);

// Check if a key exists
SWSSResult SWSSSonicV2Connector_exists(SWSSSonicV2Connector connector, const char *db_name, const char *key, uint8_t *outExists);

// Get all keys matching a pattern
// Result array must be freed using SWSSStringArray_free()
SWSSResult SWSSSonicV2Connector_keys(SWSSSonicV2Connector connector, const char *db_name, const char *pattern, uint8_t blocking, SWSSStringArray *outKeys);

// Scan keys with cursor-based iteration
// Returns cursor and matching keys
// Result array must be freed using SWSSStringArray_free()
SWSSResult SWSSSonicV2Connector_scan(SWSSSonicV2Connector connector, const char *db_name, int cursor, const char *match, uint32_t count, int *outCursor, SWSSStringArray *outKeys);

// Get a single field value from a hash
// Result string must be freed using SWSSString_free()
// Returns null if key/field doesn't exist
SWSSResult SWSSSonicV2Connector_get(SWSSSonicV2Connector connector, const char *db_name, const char *hash, const char *key, uint8_t blocking, SWSSString *outValue);

// Check if a field exists in a hash
SWSSResult SWSSSonicV2Connector_hexists(SWSSSonicV2Connector connector, const char *db_name, const char *hash, const char *key, uint8_t *outExists);

// Get all field-value pairs from a hash
// Result array must be freed using SWSSFieldValueArray_free()
SWSSResult SWSSSonicV2Connector_get_all(SWSSSonicV2Connector connector, const char *db_name, const char *hash, uint8_t blocking, SWSSFieldValueArray *outFieldValues);

// Set multiple field-value pairs in a hash
SWSSResult SWSSSonicV2Connector_hmset(SWSSSonicV2Connector connector, const char *db_name, const char *key, const SWSSFieldValueArray *values);

// Set a single field value in a hash
SWSSResult SWSSSonicV2Connector_set(SWSSSonicV2Connector connector, const char *db_name, const char *hash, const char *key, const char *val, uint8_t blocking, int64_t *outResult);

// Delete a key
SWSSResult SWSSSonicV2Connector_del(SWSSSonicV2Connector connector, const char *db_name, const char *key, uint8_t blocking, int64_t *outResult);

// Delete all keys matching a pattern
SWSSResult SWSSSonicV2Connector_delete_all_by_pattern(SWSSSonicV2Connector connector, const char *db_name, const char *pattern);

#ifdef __cplusplus
}
#endif

#endif