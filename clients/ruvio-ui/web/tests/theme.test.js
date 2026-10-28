import assert from "node:assert/strict";
import test from "node:test";
import { modes, nextMode, resolve } from "../src/theme.js";

test("the switch cycles system, light, dark and wraps", () => {
  assert.deepEqual(modes, ["auto", "light", "dark"]);
  assert.equal(nextMode("auto"), "light");
  assert.equal(nextMode("light"), "dark");
  assert.equal(nextMode("dark"), "auto");
});

test("auto follows the system and explicit modes win", () => {
  assert.equal(resolve("auto", true), "light");
  assert.equal(resolve("auto", false), "dark");
  assert.equal(resolve("dark", true), "dark");
  assert.equal(resolve("light", false), "light");
});
