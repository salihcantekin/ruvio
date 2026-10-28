function lines(value) {
  return String(value ?? "")
    .split("\n")
    .map((item) => item.trim())
    .filter(Boolean);
}

function pairLines(value) {
  const items = lines(value);
  if (!items.length || items.length % 2) throw new Error("Enter a field and its value on alternating lines. Every field needs a value.");
  return items;
}

const key = (label = "Key") => ({ name: "key", label });
const fieldBlock = (f) => {
  const names = lines(f.fields);
  if (!names.length) throw new Error("Enter at least one field.");
  return ["FIELDS", String(names.length), ...names];
};
const value = (label = "Value") => ({ name: "value", label });
const millis = (label = "Lifetime (milliseconds)") => ({ name: "ms", label, inputmode: "numeric", placeholder: "60000", required: true });

const path = (label = "Path") => ({ name: "path", label, placeholder: "$", hint: "$ is the whole document. Use .field and [0]." });
const at = (f) => (f.path || "$").trim() || "$";
const jsonField = (name, label, hint) => ({ name, label, area: true, placeholder: '{"name": "Ada", "tags": ["a", "b"]}', hint: hint || "Any valid JSON. Text needs quotes." });
function compactJson(text, what = "value") {
  const source = String(text ?? "").trim();
  if (!source) throw new Error(`Enter the ${what} as JSON.`);
  try {
    return JSON.stringify(JSON.parse(source));
  } catch (error) {
    throw new Error(`The ${what} is not valid JSON: ${error.message}`);
  }
}

export const families = [
  {
    id: "locking",
    label: "Locking",
    description: "Coordinate work with a lease and fencing token.",
    commands: [
      { name: "LEASE", blurb: "Acquire a lock", help: "Returns a fencing token, or nil while held. Keep the token to release the lock. Expiry preserves its sequence; deleting the key resets it.", fields: [key("Lock key"), millis()], build: (f) => ["LEASE", f.key, f.ms] },
      { name: "SEMAPHORE", blurb: "Hold one of N slots", help: "Returns a token while fewer than the maximum holders are live, or nil when full. Each holder has its own lifetime, so a crashed worker frees its slot. Release the token with RELEASE. The key goes away with its last holder.", fields: [key("Semaphore key"), { name: "max", label: "Maximum holders", inputmode: "numeric" }, millis()], build: (f) => ["SEMAPHORE", f.key, f.max, f.ms] },
      { name: "RENEW", blurb: "Extend a lease or slot", help: "Moves the deadline of a lease or semaphore slot you hold to now + ttl. 1 means extended; 0 means the token expired, was released or is not the holder. The token stays the same.", fields: [key("Lock key"), { name: "token", label: "Fencing token", inputmode: "numeric" }, millis()], build: (f) => ["RENEW", f.key, f.token, f.ms] },
      { name: "RELEASE", blurb: "Release a lock", help: "Only the current holder's token can release the lease. 1 means released; 0 means the token did not match.", fields: [key("Lock key"), { name: "token", label: "Fencing token", inputmode: "numeric" }], build: (f) => ["RELEASE", f.key, f.token] },
    ],
  },
  {
    id: "conditional",
    label: "Conditional writes",
    description: "Write only when a version or a number comparison holds.",
    commands: [
      { name: "SET IFGT / IFLT", blurb: "Keep the highest or lowest number", help: "Writes the whole number only when the key is missing or the number is greater (IFGT) or smaller (IFLT) than the stored one. Replies OK when written and nil when the stored number wins. A value or stored string that is not a whole number is an error and nothing is written. Use it for watermarks, versions and best prices.", fields: [key("Number key"), { name: "number", label: "Number", inputmode: "numeric" }, { name: "when", label: "Write only when", choices: [{ value: "IFGT", label: "The number is greater (IFGT)" }, { value: "IFLT", label: "The number is smaller (IFLT)" }] }], build: (f) => ["SET", f.key, f.number, f.when || "IFGT"] },
      { name: "SETV", blurb: "Write with a version", help: "Leave the version empty to create version 1. Later writes must supply the current version and return the next one. A mismatch leaves the value unchanged. Plain SET remains unconditional.", fields: [key(), value(), { name: "version", label: "Expected version", optional: true, inputmode: "numeric", placeholder: "Empty for first write" }], build: (f) => ["SETV", f.key, f.value, ...(f.version ? [f.version] : [])] },
    ],
  },
  {
    id: "limiting",
    label: "Rate limiting",
    description: "Control requests in a fixed time window.",
    commands: [
      {
        name: "LIMIT",
        blurb: "Consume an allowance",
        help: "Returns the allowance remaining after this request, or nil when full. Rejected calls do not count. Fixed counts per window, Sliding weighs the previous window so a burst at the edge is not doubled, Token bucket refills continuously and takes Cost tokens. With the retry hint the reply is [remaining, retry_after_ms], remaining -1 when refused. A key keeps the mode it was created with.",
        fields: [
          key("Limit key"),
          { name: "max", label: "Maximum requests (or bucket size)", inputmode: "numeric" },
          millis("Window (or refill time, milliseconds)"),
          {
            name: "mode",
            label: "Mode",
            choices: [
              { value: "", label: "Fixed window" },
              { value: "SLIDING", label: "Sliding window" },
              { value: "TOKENS", label: "Token bucket" },
            ],
          },
          { name: "cost", label: "Cost (token bucket)", inputmode: "numeric", placeholder: "1" },
          {
            name: "retry",
            label: "Retry hint",
            choices: [
              { value: "", label: "Plain reply" },
              { value: "WITHRETRY", label: "Also return retry_after_ms (sliding or token bucket)" },
            ],
          },
        ],
        build: (f) => [
          "LIMIT",
          f.key,
          f.max,
          f.ms,
          ...(f.mode ? [f.mode] : []),
          ...(f.mode === "TOKENS" && f.cost ? ["COST", f.cost] : []),
          ...(f.retry ? [f.retry] : []),
        ],
      },
    ],
  },
  {
    id: "idempotency",
    label: "Idempotency",
    description: "Reserve requests, detect conflicts, and replay completed results within retention.",
    commands: [
      { name: "IDEM BEGIN", blurb: "Reserve or replay a request", help: "Returns acquired, pending, conflict, or completed with a binary result. Only acquired allows execution. Tokens are 1-128 bytes; retention is 1 ms-7 days. No TTL renewal or external exactly-once guarantee.", fields: [key("Request key"), { name: "fingerprint", label: "Request fingerprint" }, { name: "owner", label: "Unique attempt token" }, millis()], build: (f) => ["IDEM", f.key, "BEGIN", f.fingerprint, f.owner, f.ms] },
      { name: "IDEM COMPLETE", blurb: "Publish an owned result", help: "1 means saved or an identical completion; 0 means missing/expired ownership, mismatch, or a changed completed result. The original deadline is preserved. Do not delete uncertain pending operations.", fields: [key("Request key"), { name: "fingerprint", label: "Request fingerprint" }, { name: "owner", label: "Original attempt token" }, value("Result (text)")], build: (f) => ["IDEM", f.key, "COMPLETE", f.fingerprint, f.owner, f.value] },
      { name: "ONCE", blurb: "Mark a request as handled", help: "1 means the first call; 0 means already handled. Repeated calls do not extend the lifetime. This marker is not a readable string.", fields: [key("Request key"), millis()], build: (f) => ["ONCE", f.key, f.ms] },
    ],
  },
  {
    id: "inventory",
    label: "Inventory",
    description: "Take stock without dropping below zero.",
    commands: [
      { name: "INCRBY MAX", blurb: "Add up to a capacity", help: "Adds the amount only when the result stays at or below the capacity, and returns the new value. Otherwise returns nil and changes nothing.", fields: [key("Counter key"), { name: "by", label: "Amount", inputmode: "numeric" }, { name: "max", label: "Capacity", inputmode: "numeric" }], build: (f) => ["INCRBY", f.key, f.by, "MAX", f.max] },
      { name: "TAKE", blurb: "Reserve available stock", help: "Initialize stock with a string containing an integer. Returns the remaining stock, or nil if missing or insufficient. A shortfall changes nothing.", fields: [key("Inventory key"), { name: "count", label: "Quantity to take", inputmode: "numeric" }], build: (f) => ["TAKE", f.key, f.count] },
      { name: "TRANSFER", blurb: "Move an amount between two keys", help: "Moves the amount from the source integer to the destination integer only when the source holds enough, and returns both balances after the move. Otherwise returns nil and changes nothing. A missing destination starts at 0. Both keys must hash to the same slot.", fields: [{ name: "source", label: "Source key" }, { name: "destination", label: "Destination key" }, { name: "amount", label: "Amount", inputmode: "numeric" }], build: (f) => ["TRANSFER", f.source, f.destination, f.amount] },
      { name: "HTRANSFER", blurb: "Move an amount between two hash fields", help: "Like TRANSFER, between two integer fields of one hash. Returns both field values after the move, or nil when the source field is missing or short.", fields: [key("Hash key"), { name: "from", label: "From field" }, { name: "to", label: "To field" }, { name: "amount", label: "Amount", inputmode: "numeric" }], build: (f) => ["HTRANSFER", f.key, f.from, f.to, f.amount] },
    ],
  },
  {
    id: "sessions",
    label: "Sliding sessions",
    description: "Read a session and renew its expiry.",
    commands: [
      { name: "GETEX", blurb: "Read and renew a session", help: "Returns the string and moves its deadline by this many milliseconds. A missing session returns nil and is not created. Plain GET does not extend expiry.", fields: [key("Session key"), millis()], build: (f) => ["GETEX", f.key, f.ms] },
    ],
  },
  {
    id: "jobs",
    label: "Delayed jobs",
    description: "Schedule stream entries for later delivery.",
    commands: [
      { name: "XADD DELAY", blurb: "Schedule a job", help: "The entry is hidden from XREADGROUP until the delay passes, but remains visible in XRANGE. The delay is persisted with the entry.", fields: [key("Stream"), millis("Delay (milliseconds)"), { name: "fields", label: "Job fields", lines: true, hint: "Field then value, on alternating lines. Example: task, send-email." }], build: (f) => ["XADD", f.key, "DELAY", f.ms, "*", ...pairLines(f.fields)] },
      { name: "XGROUP CREATE", blurb: "Create a worker group", fields: [key("Stream"), { name: "group", label: "Group" }, { name: "id", label: "Start after entry", placeholder: "0" }], build: (f) => ["XGROUP", "CREATE", f.key, f.group, f.id || "0", "MKSTREAM"] },
      { name: "XREADGROUP", blurb: "Receive ready jobs", help: "Reads up to 20 new entries without blocking. Acknowledge completed entries to remove them from the pending list.", fields: [key("Stream"), { name: "group", label: "Group" }, { name: "consumer", label: "Worker name" }], build: (f) => ["XREADGROUP", "GROUP", f.group, f.consumer, "COUNT", "20", "STREAMS", f.key, ">"] },
      { name: "XACK", blurb: "Acknowledge a completed job", fields: [key("Stream"), { name: "group", label: "Group" }, { name: "id", label: "Entry ID" }], build: (f) => ["XACK", f.key, f.group, f.id] },
      { name: "XPENDING", blurb: "Inspect pending jobs", fields: [key("Stream"), { name: "group", label: "Group" }], build: (f) => ["XPENDING", f.key, f.group] },
    ],
  },
  {
    id: "changes",
    label: "Change feed",
    description: "Watch mutations after an explicit start.",
    commands: [
      { name: "CHANGES START", blurb: "Start a change feed", help: "Arms this shard's in-memory feed and returns a cursor. Nothing before START is recorded. Pin a shard to keep subsequent reads on the same owner. Restart clears the feed.", fields: [], build: () => ["CHANGES", "START"] },
      { name: "CHANGES", blurb: "Read changes after a cursor", help: "Returns up to 1024 sequence-and-note pairs. Use the last sequence as the next cursor on the same shard. This feed is not a durable WAL.", fields: [{ name: "cursor", label: "After cursor", inputmode: "numeric" }], build: (f) => ["CHANGES", f.cursor] },
    ],
  },
  {
    id: "json",
    label: "JSON documents",
    description: "Store, read, and edit JSON documents by path.",
    commands: [
      { name: "JSON.GET", blurb: "Read a document or path", help: "Returns compact JSON, or nil when the key or path is missing.", fields: [key("Document"), path()], build: (f) => ["JSON.GET", f.key, at(f)] },
      { name: "JSON.SET", blurb: "Write a document or path", help: "Stores a validated JSON document as its own type. Setting a path edits only that value and keeps the lifetime.", fields: [key("Document"), path(), jsonField("value", "Value")], build: (f) => ["JSON.SET", f.key, at(f), compactJson(f.value)] },
      { name: "JSON.MERGE", blurb: "Merge a patch", help: "Objects merge field by field; null removes a field; other values replace.", fields: [key("Document"), path(), jsonField("patch", "Patch")], build: (f) => ["JSON.MERGE", f.key, at(f), compactJson(f.patch, "patch")] },
      { name: "JSON.DEL", blurb: "Delete a path or the key", help: "Without a path the whole document is removed. Returns 1 when something was deleted.", fields: [key("Document"), { ...path(), hint: "Empty deletes the whole key." }], build: (f) => (f.path?.trim() ? ["JSON.DEL", f.key, f.path.trim()] : ["JSON.DEL", f.key]) },
      { name: "JSON.MGET", blurb: "Read one path from several keys", help: "All keys must hash to one slot. A missing or non-JSON key gives nil.", fields: [{ name: "keys", label: "Documents", lines: true }, path()], build: (f) => ["JSON.MGET", ...lines(f.keys), at(f)] },
      { name: "JSON.TYPE", blurb: "Type at a path", fields: [key("Document"), path()], build: (f) => ["JSON.TYPE", f.key, at(f)] },
      { name: "JSON.STRLEN", blurb: "Length of a string", fields: [key("Document"), path()], build: (f) => ["JSON.STRLEN", f.key, at(f)] },
      { name: "JSON.ARRLEN", blurb: "Length of an array", fields: [key("Document"), path()], build: (f) => ["JSON.ARRLEN", f.key, at(f)] },
      { name: "JSON.OBJKEYS", blurb: "Keys of an object", fields: [key("Document"), path()], build: (f) => ["JSON.OBJKEYS", f.key, at(f)] },
      { name: "JSON.NUMINCRBY", blurb: "Add to a number", help: "Returns the new value. The path must hold a number.", fields: [key("Document"), path(), { name: "by", label: "Amount", inputmode: "decimal", placeholder: "1", required: true }], build: (f) => ["JSON.NUMINCRBY", f.key, at(f), f.by] },
      { name: "JSON.TOGGLE", blurb: "Flip a boolean", fields: [key("Document"), path()], build: (f) => ["JSON.TOGGLE", f.key, at(f)] },
      { name: "JSON.ARRAPPEND", blurb: "Append to an array", help: "The cost grows with the array length. For append-heavy data use a list.", fields: [key("Document"), path(), jsonField("value", "Value", "One JSON value. Text needs quotes.")], build: (f) => ["JSON.ARRAPPEND", f.key, at(f), compactJson(f.value)] },
      { name: "JSON.CLEAR", blurb: "Empty an array or object", fields: [key("Document"), path()], build: (f) => ["JSON.CLEAR", f.key, at(f)] },
      { name: "JSON.INDEX CREATE", blurb: "Index one field", help: "Exact-match index on one scalar field of the JSON keys with a prefix. It builds in the background and follows every write. Database 0 only.", fields: [{ name: "name", label: "Index name" }, { name: "prefix", label: "Key prefix", placeholder: "user:" }, { name: "field", label: "Field path", placeholder: "$.city" }, { name: "type", label: "Value type", choices: [{ value: "string", label: "Text" }, { value: "number", label: "Number" }, { value: "bool", label: "Boolean" }] }], build: (f) => ["JSON.INDEX", "CREATE", f.name, "PREFIX", f.prefix, "PATH", f.field, "TYPE", f.type || "string"] },
      { name: "JSON.INDEX LIST", blurb: "List indexes", fields: [], build: () => ["JSON.INDEX", "LIST"] },
      { name: "JSON.INDEX DROP", blurb: "Drop an index", fields: [{ name: "name", label: "Index name" }], build: (f) => ["JSON.INDEX", "DROP", f.name] },
      { name: "JSON.FIND", blurb: "Find documents by field value", help: "Returns keys whose indexed field equals the value. Continue with the last key as After.", fields: [{ name: "name", label: "Index name" }, { name: "value", label: "Value" }, { name: "count", label: "Count", optional: true, inputmode: "numeric", placeholder: "50" }, { name: "after", label: "After key", optional: true }], build: (f) => ["JSON.FIND", f.name, "VALUE", f.value, ...(f.count ? ["COUNT", f.count] : []), ...(f.after ? ["AFTER", f.after] : [])] },
    ],
  },
  {
    id: "strings",
    label: "Strings",
    commands: [
      { name: "GET", blurb: "Read a string", fields: [key()], build: (f) => ["GET", f.key] },
      { name: "SET", blurb: "Write a string", help: "Replaces the value unconditionally. No token or version is checked. Add an optional lifetime to set it atomically with the value.", fields: [key(), value(), { name: "seconds", label: "Lifetime (seconds)", optional: true, type: "number", min: "1", step: "1", inputmode: "numeric", placeholder: "No expiry" }], build: (f) => ["SET", f.key, f.value ?? "", ...(f.seconds ? ["EX", f.seconds] : [])] },
      { name: "INCR", blurb: "Add 1", help: "A lifetime is set only when this call creates the key; an existing key keeps its deadline.", fields: [key(), { name: "seconds", label: "Lifetime if created (seconds)", optional: true, type: "number", min: "1", step: "1", inputmode: "numeric", placeholder: "No expiry" }], build: (f) => ["INCR", f.key, ...(f.seconds ? ["EX", f.seconds] : [])] },
      { name: "INCRBY", blurb: "Add a number", help: "A lifetime is set only when this call creates the key; an existing key keeps its deadline.", fields: [key(), { name: "by", label: "Amount" }, { name: "seconds", label: "Lifetime if created (seconds)", optional: true, type: "number", min: "1", step: "1", inputmode: "numeric", placeholder: "No expiry" }], build: (f) => ["INCRBY", f.key, f.by, ...(f.seconds ? ["EX", f.seconds] : [])] },
      { name: "DECR", blurb: "Subtract 1", fields: [key()], build: (f) => ["DECR", f.key] },
      { name: "DECRBY", blurb: "Subtract a number", fields: [key(), { name: "by", label: "Amount" }], build: (f) => ["DECRBY", f.key, f.by] },
      { name: "APPEND", blurb: "Append text", fields: [key(), value("Text")], build: (f) => ["APPEND", f.key, f.value] },
      { name: "STRLEN", blurb: "Length in bytes", fields: [key()], build: (f) => ["STRLEN", f.key] },
      { name: "GETSET", blurb: "Replace and return the old value", fields: [key(), value()], build: (f) => ["GETSET", f.key, f.value] },
      { name: "MGET", blurb: "Read several keys", fields: [{ name: "keys", label: "Keys", lines: true }], build: (f) => ["MGET", ...lines(f.keys)] },
    ],
  },
  {
    id: "keys",
    label: "Keys",
    commands: [
      { name: "SCAN", blurb: "Walk key names", fields: [{ name: "cursor", label: "Cursor", placeholder: "0" }, { name: "match", label: "Match", placeholder: "*" }], build: (f) => ["SCAN", f.cursor || "0", "MATCH", f.match || "*", "COUNT", "50"] },
      { name: "EXISTS", blurb: "Does the key exist", fields: [key()], build: (f) => ["EXISTS", f.key] },
      { name: "TYPE", blurb: "Value type", fields: [key()], build: (f) => ["TYPE", f.key] },
      { name: "DEL", blurb: "Delete keys", fields: [{ name: "keys", label: "Keys", lines: true }], build: (f) => ["DEL", ...lines(f.keys)] },
      { name: "EXPIRE", blurb: "Set a lifetime in seconds", help: "Replies 1 when the lifetime was set and 0 when the key is missing or the condition does not hold.", fields: [key(), { name: "seconds", label: "Seconds" }, { name: "when", label: "Only when", choices: [{ value: "", label: "Always" }, { value: "NX", label: "The key has no lifetime (NX)" }, { value: "XX", label: "The key has a lifetime (XX)" }, { value: "GT", label: "The new lifetime is later (GT)" }, { value: "LT", label: "The new lifetime is earlier (LT)" }] }], build: (f) => ["EXPIRE", f.key, f.seconds, ...(f.when ? [f.when] : [])] },
      { name: "TTL", blurb: "Remaining lifetime", fields: [key()], build: (f) => ["TTL", f.key] },
      { name: "PERSIST", blurb: "Remove the lifetime", fields: [key()], build: (f) => ["PERSIST", f.key] },
      { name: "RENAME", blurb: "Rename a key", fields: [key("From"), { name: "to", label: "To" }], build: (f) => ["RENAME", f.key, f.to] },
    ],
  },
  {
    id: "lists",
    label: "Lists",
    commands: [
      { name: "LPUSH", blurb: "Push onto the head", fields: [key("List"), { name: "values", label: "Values", lines: true }], build: (f) => ["LPUSH", f.key, ...lines(f.values)] },
      { name: "RPUSH", blurb: "Push onto the tail", fields: [key("List"), { name: "values", label: "Values", lines: true }], build: (f) => ["RPUSH", f.key, ...lines(f.values)] },
      { name: "LPOP", blurb: "Take from the head", fields: [key("List")], build: (f) => ["LPOP", f.key] },
      { name: "RPOP", blurb: "Take from the tail", fields: [key("List")], build: (f) => ["RPOP", f.key] },
      { name: "LRANGE", blurb: "Read a slice", fields: [key("List"), { name: "start", label: "Start", placeholder: "0" }, { name: "stop", label: "Stop", placeholder: "49" }], build: (f) => ["LRANGE", f.key, f.start || "0", f.stop || "49"] },
      { name: "LINDEX", blurb: "Read one index", fields: [key("List"), { name: "index", label: "Index", placeholder: "0" }], build: (f) => ["LINDEX", f.key, f.index || "0"] },
      { name: "LLEN", blurb: "Length", fields: [key("List")], build: (f) => ["LLEN", f.key] },
      { name: "LTRIM", blurb: "Keep a slice", fields: [key("List"), { name: "start", label: "Start" }, { name: "stop", label: "Stop" }], build: (f) => ["LTRIM", f.key, f.start, f.stop] },
    ],
  },
  {
    id: "hashes",
    label: "Hashes",
    commands: [
      { name: "HSET", blurb: "Set a field", fields: [key("Hash"), { name: "field", label: "Field" }, value()], build: (f) => ["HSET", f.key, f.field, f.value] },
      { name: "HGET", blurb: "Read a field", fields: [key("Hash"), { name: "field", label: "Field" }], build: (f) => ["HGET", f.key, f.field] },
      { name: "HDEL", blurb: "Delete fields", fields: [key("Hash"), { name: "fields", label: "Fields", lines: true }], build: (f) => ["HDEL", f.key, ...lines(f.fields)] },
      { name: "HGETALL", blurb: "Read every field", fields: [key("Hash")], build: (f) => ["HGETALL", f.key] },
      { name: "HKEYS", blurb: "Field names", fields: [key("Hash")], build: (f) => ["HKEYS", f.key] },
      { name: "HLEN", blurb: "Field count", fields: [key("Hash")], build: (f) => ["HLEN", f.key] },
      {
        name: "HEXPIRE",
        blurb: "Expire single fields",
        help: "Sets a field TTL in seconds. One reply per field: 1 set, 0 condition not met, 2 deleted by a zero TTL, -2 no such field. HSET and HDEL drop a field TTL; HINCRBY keeps it. The last expired field removes the key.",
        fields: [
          key("Hash"),
          { name: "seconds", label: "Seconds", inputmode: "numeric", placeholder: "60" },
          {
            name: "when",
            label: "Only when",
            choices: [
              { value: "", label: "Always" },
              { value: "NX", label: "The field has no TTL (NX)" },
              { value: "XX", label: "The field has a TTL (XX)" },
              { value: "GT", label: "The new TTL is later (GT)" },
              { value: "LT", label: "The new TTL is earlier (LT)" },
            ],
          },
          { name: "fields", label: "Fields", lines: true },
        ],
        build: (f) => ["HEXPIRE", f.key, f.seconds, ...(f.when ? [f.when] : []), ...fieldBlock(f)],
      },
      { name: "HTTL", blurb: "Field TTLs", help: "Seconds left per field, -1 without a TTL, -2 when the field is missing.", fields: [key("Hash"), { name: "fields", label: "Fields", lines: true }], build: (f) => ["HTTL", f.key, ...fieldBlock(f)] },
      { name: "HPERSIST", blurb: "Keep fields forever", help: "Drops the field TTL. 1 removed a TTL, -1 the field had none, -2 it is missing.", fields: [key("Hash"), { name: "fields", label: "Fields", lines: true }], build: (f) => ["HPERSIST", f.key, ...fieldBlock(f)] },
    ],
  },
  {
    id: "sets",
    label: "Sets",
    commands: [
      { name: "SADD", blurb: "Add members", fields: [key("Set"), { name: "members", label: "Members", lines: true }], build: (f) => ["SADD", f.key, ...lines(f.members)] },
      { name: "SREM", blurb: "Remove members", fields: [key("Set"), { name: "members", label: "Members", lines: true }], build: (f) => ["SREM", f.key, ...lines(f.members)] },
      { name: "SISMEMBER", blurb: "Is this a member", fields: [key("Set"), { name: "member", label: "Member" }], build: (f) => ["SISMEMBER", f.key, f.member] },
      { name: "SMEMBERS", blurb: "List members", fields: [key("Set")], build: (f) => ["SMEMBERS", f.key] },
      { name: "SCARD", blurb: "Member count", fields: [key("Set")], build: (f) => ["SCARD", f.key] },
    ],
  },
  {
    id: "zsets",
    label: "Sorted sets",
    commands: [
      { name: "ZADD", blurb: "Add a member with a score", fields: [key("Sorted set"), { name: "score", label: "Score" }, { name: "member", label: "Member" }], build: (f) => ["ZADD", f.key, f.score, f.member] },
      { name: "ZINCRBY", blurb: "Change a score", fields: [key("Sorted set"), { name: "by", label: "Amount" }, { name: "member", label: "Member" }], build: (f) => ["ZINCRBY", f.key, f.by, f.member] },
      { name: "ZSCORE", blurb: "Read a score", fields: [key("Sorted set"), { name: "member", label: "Member" }], build: (f) => ["ZSCORE", f.key, f.member] },
      {
        name: "ZRANGE",
        blurb: "Read by rank",
        fields: [
          key("Sorted set"),
          { name: "start", label: "Start", placeholder: "0" },
          { name: "stop", label: "Stop", placeholder: "49" },
          {
            name: "what",
            label: "Return",
            choices: [
              { value: "both", label: "Members and scores" },
              { value: "members", label: "Members" },
              { value: "scores", label: "Scores" },
            ],
          },
        ],
        build: (f) => {
          const args = ["ZRANGE", f.key, f.start || "0", f.stop || "49"];
          if ((f.what || "both") !== "members") args.push("WITHSCORES");
          return args;
        },
      },
      { name: "ZREM", blurb: "Remove members", fields: [key("Sorted set"), { name: "members", label: "Members", lines: true }], build: (f) => ["ZREM", f.key, ...lines(f.members)] },
      { name: "ZCARD", blurb: "Member count", fields: [key("Sorted set")], build: (f) => ["ZCARD", f.key] },
      { name: "ZPOPMIN", blurb: "Take the lowest scores", help: "Removes and returns up to this many members, lowest score first. A priority queue pops this way.", fields: [key("Sorted set"), { name: "count", label: "Count", inputmode: "numeric", placeholder: "1" }], build: (f) => ["ZPOPMIN", f.key, f.count || "1"] },
      { name: "ZPOPMAX", blurb: "Take the highest scores", fields: [key("Sorted set"), { name: "count", label: "Count", inputmode: "numeric", placeholder: "1" }], build: (f) => ["ZPOPMAX", f.key, f.count || "1"] },
      { name: "ZPOPBYSCORE", blurb: "Take the due members", help: "Removes and returns up to this many members whose score is at most max, lowest first. With scores set to a due time, this takes the jobs that are ready. Bounds accept -inf, +inf, and ( for exclusive.", fields: [key("Sorted set"), { name: "max", label: "Max score", placeholder: "+inf" }, { name: "count", label: "Count", inputmode: "numeric", placeholder: "1" }], build: (f) => ["ZPOPBYSCORE", f.key, f.max || "+inf", "COUNT", f.count || "1"] },
      { name: "ZMOVEDUE", blurb: "Move due members to a list", help: "Moves up to this many members whose score is at most max from a sorted set to the end of a list, and returns how many moved. Both keys must hash to the same slot.", fields: [key("Sorted set"), { name: "destination", label: "Destination list", placeholder: "ready" }, { name: "max", label: "Max score", placeholder: "+inf" }, { name: "count", label: "Count", inputmode: "numeric", placeholder: "1" }], build: (f) => ["ZMOVEDUE", f.key, f.destination, f.max || "+inf", "COUNT", f.count || "1"] },
      { name: "ZREMRANGEBYRANK", blurb: "Trim by rank", help: "Removes ranks start through stop. 0 -11 keeps the top 10 of a leaderboard sorted by ascending score.", fields: [key("Sorted set"), { name: "start", label: "Start", placeholder: "0" }, { name: "stop", label: "Stop", placeholder: "-11" }], build: (f) => ["ZREMRANGEBYRANK", f.key, f.start || "0", f.stop || "-11"] },
      { name: "ZREMRANGEBYSCORE", blurb: "Remove a score range", help: "Bounds accept -inf, +inf, and ( for exclusive. Removes due entries from a time-scored set.", fields: [key("Sorted set"), { name: "min", label: "Min", placeholder: "-inf" }, { name: "max", label: "Max", placeholder: "+inf" }], build: (f) => ["ZREMRANGEBYSCORE", f.key, f.min || "-inf", f.max || "+inf"] },
    ],
  },
  {
    id: "streams",
    label: "Streams",
    commands: [
      { name: "XADD", blurb: "Append an entry", fields: [key("Stream"), { name: "id", label: "Id", placeholder: "*" }, { name: "fields", label: "Fields", lines: true, hint: "field then value, one per line" }], build: (f) => ["XADD", f.key, f.id || "*", ...pairLines(f.fields)] },
      { name: "XRANGE", blurb: "Read a range", fields: [key("Stream"), { name: "start", label: "Start", placeholder: "-" }, { name: "end", label: "End", placeholder: "+" }], build: (f) => ["XRANGE", f.key, f.start || "-", f.end || "+", "COUNT", "30"] },
      { name: "XLEN", blurb: "Entry count", fields: [key("Stream")], build: (f) => ["XLEN", f.key] },
      { name: "XREAD", blurb: "Read new entries", fields: [key("Stream"), { name: "id", label: "After id", placeholder: "0-0" }], build: (f) => ["XREAD", "COUNT", "20", "STREAMS", f.key, f.id || "0-0"] },
      { name: "XDEL", blurb: "Delete ids", fields: [key("Stream"), { name: "ids", label: "Ids", lines: true }], build: (f) => ["XDEL", f.key, ...lines(f.ids)] },
    ],
  },
  {
    id: "server",
    label: "Server",
    commands: [
      { name: "PING", blurb: "Check the connection", fields: [], build: () => ["PING"] },
      { name: "INFO", blurb: "Server statistics", fields: [{ name: "section", label: "Section", placeholder: "all" }], build: (f) => (f.section ? ["INFO", f.section] : ["INFO"]) },
      { name: "DBSIZE", blurb: "Keys in this database", fields: [], build: () => ["DBSIZE"] },
      { name: "SELECT", blurb: "Switch database", fields: [{ name: "index", label: "Database", placeholder: "0" }], build: (f) => ["SELECT", f.index || "0"] },
      { name: "SAVE", blurb: "Write a checkpoint", fields: [], build: () => ["SAVE"] },
    ],
  },
  {
    id: "users",
    label: "Users",
    commands: [
      { name: "ACL LIST", blurb: "Every user rule", fields: [], build: () => ["ACL", "LIST"] },
      { name: "ACL USERS", blurb: "User names", fields: [], build: () => ["ACL", "USERS"] },
      { name: "ACL WHOAMI", blurb: "Current user", fields: [], build: () => ["ACL", "WHOAMI"] },
      { name: "ACL GETUSER", blurb: "One user's rules", fields: [{ name: "user", label: "User" }], build: (f) => ["ACL", "GETUSER", f.user] },
      { name: "ACL SETUSER", blurb: "Create or change a user", fields: [{ name: "user", label: "User" }, { name: "rules", label: "Rules", lines: true, hint: "One rule per line, for example on, >secret, ~*, +@all" }], build: (f) => ["ACL", "SETUSER", f.user, ...lines(f.rules)] },
    ],
  },
];

export const taskFamilies = families.filter((family) => family.id !== "users");

export function findTasks(query = "") {
  const words = query.toLowerCase().trim().split(/\s+/);
  return taskFamilies.flatMap((family) =>
    family.commands
      .filter((command) => words.every((word) =>
        `${family.label} ${family.description || ""} ${command.name} ${command.blurb} ${command.help || ""}`.toLowerCase().includes(word),
      ))
      .map((command) => ({ family, command })),
  );
}
