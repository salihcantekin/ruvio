export function generateUuid() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, (c) =>
      (c ^ (crypto.getRandomValues(new Uint8Array(1))[0] & (15 >> (c / 4)))).toString(16)
    );
  }
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

const uiSessionId = typeof sessionStorage === "undefined"
  ? generateUuid()
  : sessionStorage.getItem("ruvio.session") || generateUuid();
if (typeof sessionStorage !== "undefined") sessionStorage.setItem("ruvio.session", uiSessionId);

export async function api(path, body) {
  const readOnly = ["/api/session", "/api/defaults", "/api/shards"].includes(path)
    || (path === "/api/command" && isReadOnly(body?.args || []));
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await request(path, body, readOnly && attempt < 2);
    } catch (error) {
      if (!readOnly || attempt >= 2 || !(error instanceof TypeError || error.transient)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 100 * (attempt + 1)));
    }
  }
}

function isReadOnly(args) {
  const command = String(args[0]).toUpperCase();
  return ["PING", "INFO", "TYPE", "TTL", "PTTL", "GET", "MGET", "EXISTS", "SCAN",
    "LLEN", "LRANGE", "HLEN", "HSCAN", "HGET", "HGETALL", "SCARD", "SMEMBERS",
    "SISMEMBER", "ZCARD", "ZRANGE", "ZSCORE", "XLEN", "XRANGE", "XINFO", "DBSIZE"].includes(command)
    || (command === "ACL" && ["USERS", "GETUSER", "LIST"].includes(String(args[1]).toUpperCase()));
}

async function request(path, body, canRetry) {
  const response = await fetch(path, {
    method: body ? "POST" : "GET",
    headers: {
      "x-ruvio-session": uiSessionId,
      ...(body ? { "content-type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (canRetry && [502, 503, 504].includes(response.status)) {
    const error = new Error(`HTTP ${response.status}: ${response.statusText}`);
    error.transient = true;
    throw error;
  }
  const result = await response.json();
  if (!response.ok || result.ok === false) {
    throw new Error(result.error || `HTTP ${response.status}: ${response.statusText}`);
  }
  return result;
}

export function textOf(value) {
  if (!value) return "";
  if (value.kind === "bulk" || value.kind === "simple" || value.kind === "error") {
    return value.text ?? value.base64 ?? "";
  }
  if (value.kind === "integer") return String(value.value);
  if (value.kind === "null") return "(nil)";
  if (value.kind === "array") {
    return value.items.map((item, index) => `${index + 1}) ${textOf(item)}`).join("\n");
  }
  return "";
}
