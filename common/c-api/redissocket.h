#pragma once
#include "result.h"
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
// Output fd is owned by the caller and -1 on failure. cancelFd stays caller-owned.
SWSSResult SWSSRedisSocket_open_unix(const char *path, uint32_t timeout_ms, uint8_t authenticate,
                                    int32_t cancelFd, int32_t *outFd, int32_t *outError);
#ifdef __cplusplus
}
#endif
