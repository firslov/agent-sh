import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const DRIVER = fileURLToPath(new URL("../fixtures/steer-mid-turn-driver.ts", import.meta.url));

interface DriverResult {
  requestCount: number;
  roles: string[];
  steerAInSecondRequest: boolean;
  steerAAfterToolResults: boolean;
  toolResultsInSecondRequest: number;
  steerBInThirdRequest: boolean;
  consumed: string[];
  queries: string[];
}

function runDriver(): Promise<DriverResult> {
  const home = mkdtempSync(join(tmpdir(), "agent-sh-steer-"));
  return new Promise<DriverResult>((resolve, reject) => {
    const child = spawn("node", ["--import", "tsx", DRIVER], {
      env: {
        PATH: process.env.PATH,
        HOME: home,
        AGENT_SH_HOME: home,
        AGENT_SH_SKIP_SHELL_ENV: "1",
        OPENROUTER_API_KEY: "",
        OPENAI_API_KEY: "",
        DEEPSEEK_API_KEY: "",
        OPENAI_BASE_URL: "",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout!.on("data", (c) => { stdout += c.toString(); });
    child.stderr!.on("data", (c) => { stderr += c.toString(); });
    const timer = setTimeout(() => child.kill("SIGKILL"), 30000);
    child.on("close", (code) => {
      clearTimeout(timer);
      rmSync(home, { recursive: true, force: true });
      try {
        resolve(JSON.parse(stdout.trim().split(/\r?\n/).pop() ?? "") as DriverResult);
      } catch (err) {
        reject(new Error(`driver output not JSON.\nexit=${code}\nstdout:\n${stdout}\nstderr:\n${stderr}\n${(err as Error).message}`));
      }
    });
  });
}

test("a message steered mid-turn lands before the turn's next set of tool calls", async () => {
  const r = await runDriver();
  assert.equal(r.toolResultsInSecondRequest, 2, `parallel batch should have completed: ${JSON.stringify(r.roles)}`);
  assert.equal(r.steerAInSecondRequest, true, `steer should reach the next request: ${JSON.stringify(r.roles)}`);
  assert.equal(r.steerAAfterToolResults, true, "steer must sit after the tool results, not inside the tool_call gap");
  assert.deepEqual(r.consumed, ["STEER-A", "STEER-B"]);
  assert.deepEqual(r.queries, ["start", "STEER-A", "STEER-B"]);
});

test("a message steered in while the final answer streams gets its own round", async () => {
  const r = await runDriver();
  assert.equal(r.requestCount, 3, `loop should not end with an unanswered message: ${JSON.stringify(r.roles)}`);
  assert.equal(r.steerBInThirdRequest, true);
});
