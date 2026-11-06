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
--   ARGV[2] - COUNT hint passed to SCAN, clamped to COUNT_MAX
--   ARGV[3] - key pattern, e.g. "ASIC_STATE:SAI_OBJECT_TYPE_FDB_ENTRY:*"
--   ARGV[4] - bridge port oid to filter on, empty string means any port
--   ARGV[5] - "0" flush dynamic entries only, "1" flush static and dynamic
--
-- Returns { nextCursor, deletedCount }, both as strings. The caller is expected
-- to adjust the COUNT hint between invocations so that a single call stays
-- within a small, bounded time budget.
--
-- SCAN is only guaranteed to return every key that was present for the whole
-- iteration, and it may return the same key more than once. That is fine here:
-- the entries are deleted as they are found, and a repeated key is simply found
-- already gone by the second DEL, which does not increase deletedCount.

local CURSOR = ARGV[1] or "0"
local COUNT = tonumber(ARGV[2] or "100")
local PATTERN = ARGV[3]
local PORT = ARGV[4] or ""
local FLUSH_TYPE = ARGV[5] or "0"

local COUNT_MAX = 10000

if not string.match(CURSOR, "^%d+$") then
   return redis.error_reply("fdb_flush_scan: invalid cursor '" .. CURSOR .. "', expected a decimal SCAN cursor")
end

if COUNT == nil or COUNT < 1 then
   return redis.error_reply("fdb_flush_scan: invalid count '" .. (ARGV[2] or "") .. "', expected an integer >= 1")
end

if COUNT > COUNT_MAX then
   COUNT = COUNT_MAX
end

if PATTERN == nil or PATTERN == "" then
   return redis.error_reply("fdb_flush_scan: missing key pattern (ARGV[3])")
end

if FLUSH_TYPE ~= "0" and FLUSH_TYPE ~= "1" then
   return redis.error_reply("fdb_flush_scan: invalid flush type '" .. FLUSH_TYPE .. "', expected \"0\" (dynamic only) or \"1\" (static and dynamic)")
end

-- "oid:0x0" is SAI_NULL_OBJECT_ID, i.e. no port filter
if PORT == "oid:0x0" then
   PORT = ""
end

local BATCH = redis.call('SCAN', CURSOR, 'MATCH', PATTERN, 'COUNT', COUNT)
local nextCursor = BATCH[1]
local keys = BATCH[2]
local deletedCount = 0

for i = 1, #keys do
   local matched = true

   if PORT ~= "" then
      matched = redis.call('HGET', keys[i], "SAI_FDB_ENTRY_ATTR_BRIDGE_PORT_ID") == PORT
   end

   if matched and FLUSH_TYPE == "0" then
      matched = redis.call('HGET', keys[i], "SAI_FDB_ENTRY_ATTR_TYPE") == "SAI_FDB_ENTRY_TYPE_DYNAMIC"
   end

   if matched then
      -- count what was actually removed, DEL returns 0 for a key that was
      -- already deleted by a previous (possibly duplicated) SCAN result
      deletedCount = deletedCount + redis.call('DEL', keys[i])
   end
end

return {tostring(nextCursor), tostring(deletedCount)}
