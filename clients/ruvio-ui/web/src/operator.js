import { api } from "./api.js";
import { scalar } from "./value.js";

export function nodeId(node) {
  return node ? `${node.host}:${node.port}` : "all";
}

// The server takes every argument as text, while number inputs hand back numbers.
export const textArgs = (args) => args.map((arg) => String(arg ?? ""));

export async function execute(args, shard = null) {
  const result = await api("/api/command", { args: textArgs(args), shard });
  if (result.value?.kind === "error") throw new Error(result.value.text);
  return result.value;
}

export function scanPage(value) {
  const items = value?.items;
  if (value?.kind !== "array" || items.length !== 2 || items[1]?.kind !== "array") {
    throw new Error("Unexpected SCAN reply. Retry the scan.");
  }
  return { cursor: scalar(items[0]), keys: items[1].items.map(scalar) };
}

export async function scanOwners(args, command) {
  const countAt = args.findIndex((arg) => String(arg).toUpperCase() === "COUNT");
  const count = countAt < 0 ? 50 : Number(args[countAt + 1]);
  if (!Number.isInteger(count) || count < 1) throw new Error("COUNT must be a positive integer.");
  let cursor = String(args[1] || "0");
  const keys = [];
  // The client cursor includes the owner. Empty pages can still advance to another shard.
  for (let page = 0; page < 64; page += 1) {
    const nextArgs = [...args];
    nextArgs[1] = cursor;
    const next = scanPage(await command(nextArgs));
    keys.push(...next.keys);
    if (next.cursor === cursor && cursor !== "0") throw new Error("SCAN did not advance. Retry from the start.");
    cursor = next.cursor;
    if (cursor === "0" || keys.length >= count) break;
  }
  return {
    kind: "array",
    items: [
      { kind: "bulk", text: cursor },
      { kind: "array", items: [...new Set(keys)].map((text) => ({ kind: "bulk", text })) },
    ],
  };
}

export async function scanSelected(args, nodes, command = execute) {
  const raw = String(args[1] || "0");
  let owner = 0;
  let cursor = "0";
  if (raw !== "0") {
    const match = /^selected:(\d+):(.+)$/.exec(raw);
    if (!match || Number(match[1]) >= nodes.length) throw new Error("Invalid selected-shard cursor. Restart the scan.");
    owner = Number(match[1]);
    cursor = decodeURIComponent(match[2]);
  }
  const keys = [];
  const countAt = args.findIndex((arg) => String(arg).toUpperCase() === "COUNT");
  const count = countAt < 0 ? 50 : Number(args[countAt + 1]);
  if (!Number.isInteger(count) || count < 1) throw new Error("COUNT must be a positive integer.");
  for (let page = 0; page < 64 && owner < nodes.length; page += 1) {
    const nextArgs = [...args];
    nextArgs[1] = cursor;
    const next = scanPage(await command(nextArgs, nodes[owner]));
    if (next.cursor === cursor && cursor !== "0") throw new Error("SCAN did not advance. Restart the scan.");
    keys.push(...next.keys);
    cursor = next.cursor;
    if (cursor === "0") owner += 1;
    if (keys.length >= count) break;
  }
  return {
    kind: "array",
    items: [
      { kind: "bulk", text: owner >= nodes.length ? "0" : `selected:${owner}:${encodeURIComponent(cursor)}` },
      { kind: "array", items: [...new Set(keys)].map((text) => ({ kind: "bulk", text })) },
    ],
  };
}

export function metric(info, name) {
  const raw = info?.[name];
  if (raw == null || raw.trim() === "") return null;
  const value = Number(raw);
  return Number.isFinite(value) && value >= 0 ? value : null;
}

export function number(value) {
  return value == null ? "Not reported" : value.toLocaleString();
}

export function bytes(value) {
  if (value == null) return "Not reported";
  if (value < 1024) return `${value} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let size = value / 1024;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size.toFixed(1)} ${units[unit]}`;
}
