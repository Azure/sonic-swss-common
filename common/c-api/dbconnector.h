#ifndef SWSS_COMMON_C_API_DBCONNECTOR_H
#define SWSS_COMMON_C_API_DBCONNECTOR_H

#include "result.h"
#include "util.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

// On success the caller owns *outFd. On failure *outFd is -1.
SWSSResult SWSSRedisSocket_open_unix(const char *path, uint32_t connectTimeoutMs,
                                    uint32_t authTimeoutMs, int32_t *outFd);
// One budget for connect/AUTH. The caller retains cancelFd (-1 disables it);
// readability or closure interrupts the operation. outFd has the same ownership.
SWSSResult SWSSRedisSocket_open_unix_interruptible(const char *path, uint32_t timeoutMs,
                                                 int32_t cancelFd, int32_t *outFd);
SWSSResult SWSSRedisSocket_check_tcp(const char *host, int32_t port);

SWSSResult SWSSSonicDBConfig_initialize(const char *path);

SWSSResult SWSSSonicDBConfig_initializeGlobalConfig(const char *path, uint8_t ignore_nonexistent);

typedef struct SWSSDBConnectorOpaque *SWSSDBConnector;

// Pass 0 to timeout for infinity
SWSSResult SWSSDBConnector_new_tcp(int32_t dbId, const char *hostname, uint16_t port,
                                   uint32_t timeout, SWSSDBConnector *outDb);

// Pass 0 to timeout for infinity
SWSSResult SWSSDBConnector_new_unix(int32_t dbId, const char *sock_path, uint32_t timeout, SWSSDBConnector *outDb);

// Pass 0 to timeout for infinity
SWSSResult SWSSDBConnector_new_named(const char *dbName, uint32_t timeout_ms, uint8_t isTcpConn, SWSSDBConnector *outDb);

// Pass 0 to timeout for infinity
SWSSResult SWSSDBConnector_new_keyed(const char *dbName, uint32_t timeout_ms, uint8_t isTcpConn,
                                     const char *containerName, const char *netns, SWSSDBConnector *outDb);

SWSSResult SWSSDBConnector_free(SWSSDBConnector db);

// Outputs 0 when key doesn't exist, 1 when key was deleted
SWSSResult SWSSDBConnector_del(SWSSDBConnector db, const char *key, int8_t *outStatus);

SWSSResult SWSSDBConnector_set(SWSSDBConnector db, const char *key, SWSSStrRef value);

// Outputs NULL if key doesn't exist
// Value must be freed using SWSSString_free()
SWSSResult SWSSDBConnector_get(SWSSDBConnector db, const char *key, SWSSString *outValue);

// Outputs 0 for false, 1 for true
SWSSResult SWSSDBConnector_exists(SWSSDBConnector db, const char *key, int8_t *outExists);

// Outputs 0 when key or field doesn't exist, 1 when field was deleted
SWSSResult SWSSDBConnector_hdel(SWSSDBConnector db, const char *key, const char *field, int8_t *outResult);

SWSSResult SWSSDBConnector_hset(SWSSDBConnector db, const char *key, const char *field, SWSSStrRef value);

// Outputs NULL if key or field doesn't exist
// Value must be freed using SWSSString_free()
SWSSResult SWSSDBConnector_hget(SWSSDBConnector db, const char *key, const char *field, SWSSString *outValue);

// Outputs an empty map when the key doesn't exist
// Result array and all of its elements must be freed using appropriate free functions
SWSSResult SWSSDBConnector_hgetall(SWSSDBConnector db, const char *key, SWSSFieldValueArray *outArr);

// Outputs 0 when key or field doesn't exist, 1 when field exists
SWSSResult SWSSDBConnector_hexists(SWSSDBConnector db, const char *key, const char *field, int8_t *outExists);

// Outputs 1 on success, 0 on failure
SWSSResult SWSSDBConnector_flushdb(SWSSDBConnector db, int8_t *outStatus);

#ifdef __cplusplus
}
#endif

#endif
