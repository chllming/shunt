import test from "node:test";
import assert from "node:assert/strict";
import { normalizeTag, parseArgs, validateOptions } from "../bin/vibe-shunt.mjs";

test("accepts and normalizes explicit open-core release tags", () => {
  for (const value of ["0.2.0", "v0.2.0", "core-v0.2.0"]) {
    const options = validateOptions(parseArgs(["--version", value]));
    assert.equal(normalizeTag(options.version), "core-v0.2.0");
  }
});
