// Key counts per database from the text of `INFO keyspace` ("db3:keys=120,expires=4,avg_ttl=0").
export function parseKeyspace(text) {
  const counts = {};
  for (const line of String(text ?? "").split(/\r?\n/)) {
    const match = /^db(\d+):(.*)$/.exec(line.trim());
    if (!match) continue;
    const keys = /(?:^|,)keys=(\d+)/.exec(match[2]);
    if (keys) counts[Number(match[1])] = Number(keys[1]);
  }
  return counts;
}

export function dbLabel(db, counts) {
  const keys = counts?.[db];
  if (keys === undefined) return counts ? `DB ${db} · empty` : `DB ${db}`;
  return `DB ${db} · ${keys.toLocaleString("en-US")} ${keys === 1 ? "key" : "keys"}`;
}
