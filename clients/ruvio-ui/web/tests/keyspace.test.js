import test from "node:test";
import assert from "node:assert/strict";
import { parseKeyspace, dbLabel } from "../src/keyspace.js";

test("parseKeyspace reads key counts per database", () => {
  const counts = parseKeyspace("# Keyspace\r\ndb0:keys=12,expires=1,avg_ttl=0\r\ndb3:keys=1200,expires=0,avg_ttl=0\r\n");
  assert.deepEqual(counts, { 0: 12, 3: 1200 });
});

test("parseKeyspace tolerates empty or odd input", () => {
  assert.deepEqual(parseKeyspace(""), {});
  assert.deepEqual(parseKeyspace(undefined), {});
  assert.deepEqual(parseKeyspace("db1:expires=3\ngarbage"), {});
});

test("dbLabel shows counts, empty databases and the unknown state", () => {
  const counts = { 0: 1, 3: 1200 };
  assert.equal(dbLabel(0, counts), "DB 0 · 1 key");
  assert.equal(dbLabel(3, counts), "DB 3 · 1,200 keys");
  assert.equal(dbLabel(5, counts), "DB 5 · empty");
  assert.equal(dbLabel(5, null), "DB 5");
});
