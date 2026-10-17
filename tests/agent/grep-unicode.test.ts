import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createGrepTool } from "../../src/agent/tools/grep.js";
import { contentText } from "../../src/agent/types.js";

// A single-file content search prefixes the line with "1:". The 500-unit
// cap includes that prefix, so place emoji on either side of that boundary.
const marker = "… [truncated]";
const cases = [
  { name: "short Unicode line", input: "中文 🟡", expected: "1:中文 🟡" },
  { name: "exactly 500 units", input: "a".repeat(496) + "🟡", expected: "1:" + "a".repeat(496) + "🟡" },
  { name: "long ASCII line", input: "a".repeat(600), expected: "1:" + "a".repeat(498) + marker },
  { name: "emoji straddles cap", input: "a".repeat(497) + "🟡tail", expected: "1:" + "a".repeat(497) + marker },
  { name: "emoji ends at cap", input: "a".repeat(496) + "🟡tail", expected: "1:" + "a".repeat(496) + "🟡" + marker },
  { name: "emoji starts after cap", input: "a".repeat(498) + "🟡tail", expected: "1:" + "a".repeat(498) + marker },
];

for (const { name, input, expected } of cases) {
  test(`grep content truncation: ${name}`, async (t) => {
    const dir = await mkdtemp(join(tmpdir(), "agent-sh-grep-unicode-"));
    t.after(() => rm(dir, { recursive: true, force: true }));
    await writeFile(join(dir, "sample.txt"), input + "\n", "utf8");
    const result = await createGrepTool(() => dir).execute({
      pattern: ".", path: "sample.txt", output_mode: "content", head_limit: 0,
    });
    assert.equal(result.isError, false);
    const text = contentText(result.content).trimEnd();
    // With /u, valid surrogate pairs are matched as code points outside this
    // range; only lone surrogates would remain and upset strict JSON parsers.
    assert.doesNotMatch(text, /[\uD800-\uDFFF]/u);
    assert.equal(text, expected);
  });
}
