-- Incremental FDB flush used by syncd (RedisClient::processFlushEvent).
--
-- Instead of loading all matching keys at once with KEYS (which blocks the
-- redis server for a long time when the FDB is large), a single SCAN batch is
-- read and the matching entries are deleted. The next cursor is returned to the
-- caller, which keeps invoking this script until the cursor is back to 0, so
-- the lock hold time of one invocation stays short and bounded.
--
-- This script takes no KEYS, everything is passed as ARGV:
--   ARGV[1] - cursor to start from ("0" for a new scan)
--   ARGV[2] - COUNT hint passed to SCAN
--   ARGV[3] - key pattern, e.g. "ASIC_STATE:SAI_OBJECT_TYPE_FDB_ENTRY:*"
--   ARGV[4] - bridge port oid to filter on, empty string means any port
--   ARGV[5] - "0" flush dynamic entries only, "1" flush (all) static entries
--
-- Returns { nextCursor, deletedCount }, both as strings. The caller is expected
-- to adjust the COUNT hint between invocations so that a single call stays
-- within a small, bounded time budget.

local cursor = ARGV[1] or "0"
local count = ARGV[2] or "100"
local pattern = ARGV[3]
local portStr = ARGV[4]
local flushType = ARGV[5]

local result = redis.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', count)
local nextCursor = result[1]
local keys = result[2]
local deletedCount = 0

for i = 1, #keys do
   local shouldDelete = false

   if portStr == "" then
      if flushType == "1" then
         shouldDelete = true
      elseif flushType == "0" then
         local type = redis.call('HGET', keys[i], "SAI_FDB_ENTRY_ATTR_TYPE");
         if type == "SAI_FDB_ENTRY_TYPE_DYNAMIC" then
            shouldDelete = true
         end
      end
   else
      local bridge_port = redis.call('HGET', keys[i], "SAI_FDB_ENTRY_ATTR_BRIDGE_PORT_ID");
      if bridge_port == portStr then
         if flushType == "1" then
            shouldDelete = true
         elseif flushType == "0" then
            local type = redis.call('HGET', keys[i], "SAI_FDB_ENTRY_ATTR_TYPE");
            if type == "SAI_FDB_ENTRY_TYPE_DYNAMIC" then
               shouldDelete = true
            end
         end
      end
   end

   if shouldDelete then
      redis.call('DEL', keys[i])
      deletedCount = deletedCount + 1
   end
end

return {tostring(nextCursor), tostring(deletedCount)}
